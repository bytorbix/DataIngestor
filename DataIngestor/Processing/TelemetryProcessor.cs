using DataIngestor.Channels;
using DataIngestor.Ingestion;
using System.Text.Json.Nodes;

namespace DataIngestor.Processing
{
    public class TelemetryProcessor(ChannelRegistry channelRegistry, TelemetryFilter filter, ILogger<TelemetryProcessor> logger)
    {
        public void Process(string tailNumber, string telemetryJson)
        {
            var channel = channelRegistry.Get(tailNumber);
            if (channel == null)
            {
                logger.LogWarning("Dropping telemetry for unregistered channel: {TailNumber}", tailNumber);
                return;
            }

            string strippedJson = filter.Strip(telemetryJson);
            JsonNode? node = JsonNode.Parse(strippedJson);

            // we check if the field exists and if so if it's nulled
            if (!node!.AsObject().TryGetPropertyValue("time", out JsonNode? timeNode) || timeNode == null)
            {
                logger.LogWarning("Telemetry for {TailNumber} missing 'time' field, dropping.", tailNumber);
                return;
            }
            long timeMs = timeNode.GetValue<long>();

            // pipeline record into Channel buffer
            TelemetryRecord record = new TelemetryRecord(timeMs, strippedJson);
            channel.TelemetryChannel.Writer.TryWrite(record);
        }
    }
}