using System.Text.Json.Nodes;

namespace DataIngestor.Ingestion
{
    public class TelemetryFilter
    {
        // packet framing, not telemetry; "Timestamp" is the TelemetryDevice's packet-capture time.
        // Matched ignoring case, since JSON property names are case-sensitive and TelemetryDevice writes "Timestamp".
        private static readonly HashSet<string> FieldsToStrip = new(StringComparer.OrdinalIgnoreCase)
        {
            "sync_1", "sync_2", "sync_3", "correlator", "zero space", "Timestamp"
        };

        public string Strip(string telemetryJson)
        {
            JsonNode? node = JsonNode.Parse(telemetryJson);
            JsonObject obj = node?.AsObject() ?? throw new InvalidOperationException("Failed to parse telemetry JSON.");

            foreach (string field in obj.Select(p => p.Key).Where(FieldsToStrip.Contains).ToList())
            {
                obj.Remove(field);
            }

            return obj.ToJsonString();
        }
    }
}
