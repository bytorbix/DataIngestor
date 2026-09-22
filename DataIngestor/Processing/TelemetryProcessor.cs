using DataIngestor.Channels;
using DataIngestor.Ingestion;
using System.Text.Json.Nodes;

namespace DataIngestor.Processing
{
    public class TelemetryProcessor(ChannelRegistry channelRegistry, TelemetryFilter filter, ILogger<TelemetryProcessor> logger)
    {
        const string PtsTimePropertyName = "pts_time";
        const string TimePropertyName = "time";
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
            JsonObject obj = node!.AsObject();

            if (!obj.TryGetPropertyValue(PtsTimePropertyName, out JsonNode? ptsNode) || ptsNode == null ||
             !obj.TryGetPropertyValue(TimePropertyName, out JsonNode? timeNode) || timeNode == null)
            {
                logger.LogWarning("Telemetry for {TailNumber} missing {PtsField}/{TimeField}, dropping.", tailNumber, PtsTimePropertyName, TimePropertyName);
                return;
            }

            double ptsTime = ptsNode.GetValue<double>();
            long timeMs = timeNode.GetValue<long>() * 1000;

            // pipeline record into Channel buffer
            TelemetryRecord record = new TelemetryRecord(timeMs, ptsTime, strippedJson);
            channel.TelemetryChannel.Writer.TryWrite(record);
        }
    }
}