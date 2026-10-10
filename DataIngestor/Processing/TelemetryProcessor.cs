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
            Channel? channel = channelRegistry.Get(tailNumber);
            if (channel == null)
            {
                logger.LogWarning("Dropping telemetry for unregistered channel: {TailNumber}", tailNumber);
                return;
            }

            string strippedJson = filter.Strip(telemetryJson);
            TelemetryRecord? record = ParseRecord(tailNumber, strippedJson);
            if (record == null)
                return;

            // pipeline record into Channel buffer
            channel.TelemetryChannel.Writer.TryWrite(record);
        }

        // null means the telemetry is missing pts_time or time and should be dropped
        private TelemetryRecord? ParseRecord(string tailNumber, string strippedJson)
        {
            JsonNode? node = JsonNode.Parse(strippedJson);
            JsonObject obj = node!.AsObject();

            if (!obj.TryGetPropertyValue(PtsTimePropertyName, out JsonNode? ptsNode) || ptsNode == null ||
             !obj.TryGetPropertyValue(TimePropertyName, out JsonNode? timeNode) || timeNode == null)
            {
                logger.LogWarning("Telemetry for {TailNumber} missing {PtsField}/{TimeField}, dropping.", tailNumber, PtsTimePropertyName, TimePropertyName);
                return null;
            }

            double ptsTime = ptsNode.GetValue<double>();
            long timeMs = timeNode.GetValue<long>();

            return new TelemetryRecord(timeMs, ptsTime, strippedJson);
        }
    }
}