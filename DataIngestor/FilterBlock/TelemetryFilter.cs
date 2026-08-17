using System.Text.Json.Nodes;

namespace DataIngestor.FilterBlock
{
    public class TelemetryFilter
    {
        private static readonly string[] FieldsToStrip =
        {
            "sync_1", "sync_2", "sync_3", "Tail number", "correlator", "zero space"
        };

        public string Strip(string telemetryJson)
        {
            JsonNode? node = JsonNode.Parse(telemetryJson);
            JsonObject obj = node?.AsObject() ?? throw new InvalidOperationException("Failed to parse telemetry JSON.");

            foreach (string field in FieldsToStrip)
            {
                obj.Remove(field);
            }

            return obj.ToJsonString();
        }
    }
}
