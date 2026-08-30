using System.Text.Json.Nodes;

namespace DataIngestor.Ingestion
{
    public class TelemetryFilter
    {
        private static readonly string[] FieldsToStrip =
        {
            "sync_1", "sync_2", "sync_3", "correlator", "zero space", "timestamp" // captured packet timestamp
        };

        public string Strip(string telemetryJson)
        {
            JsonNode? node = JsonNode.Parse(telemetryJson);
            JsonNode ss = "";
            JsonObject obj = node?.AsObject() ?? throw new InvalidOperationException("Failed to parse telemetry JSON.");

            foreach (string field in FieldsToStrip)
            {

                obj.Remove(field);
            }

            return obj.ToJsonString();
        }
    }
}
