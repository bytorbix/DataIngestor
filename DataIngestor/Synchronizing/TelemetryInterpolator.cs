using System.Text.Json;
using System.Text.Json.Nodes;

namespace DataIngestor.Synchronizing
{
    // Blends the telemetry rows on either side of a frame into one estimated row at the frame's time.
    public static class TelemetryInterpolator
    {
        private const string TimeFieldName = "time";

        private static readonly HashSet<string> DiscreteFields = new()
        {
            "Tail number", "flight_state", "gps_num", "gps_level", "battery"
        };

        // degrees that wrap around (359 -> 1 is a 2 degree turn), blended along the shorter direction
        private static readonly HashSet<string> AngleFields = new()
        {
            "pitch", "roll", "yaw", "gimbal_pitch", "gimbal_roll", "gimbal_yaw"
        };

        // fraction: 0 = exactly at before, 1 = exactly at after
        public static JsonObject Interpolate(JsonObject before, JsonObject after, double fraction)
        {
            JsonObject nearest = fraction < 0.5 ? before : after;
            JsonObject result = new();

            IEnumerable<string> keys = before.Select(p => p.Key).Union(after.Select(p => p.Key));
            foreach (string key in keys)
            {
                JsonNode? beforeValue = before[key];
                JsonNode? afterValue = after[key];

                // field only present in one row (e.g. correlator group not sent), use what we have
                if (beforeValue is null || afterValue is null)
                {
                    result[key] = (beforeValue ?? afterValue)?.DeepClone();
                    continue;
                }

                if (DiscreteFields.Contains(key) || !TryGetNumber(beforeValue, out double a) || !TryGetNumber(afterValue, out double b))
                {
                    result[key] = nearest[key]?.DeepClone();
                    continue;
                }

                if (key == TimeFieldName)
                    result[key] = (long)Math.Round(Lerp(a, b, fraction));
                else if (AngleFields.Contains(key))
                    result[key] = LerpAngle(a, b, fraction);
                else
                    result[key] = Lerp(a, b, fraction);
            }

            return result;
        }

        private static bool TryGetNumber(JsonNode node, out double value)
        {
            if (node is JsonValue jsonValue && jsonValue.GetValueKind() == JsonValueKind.Number)
            {
                value = jsonValue.GetValue<double>();
                return true;
            }

            value = 0;
            return false;
        }

        private static double Lerp(double a, double b, double fraction) => a + (b - a) * fraction;

        private static double LerpAngle(double a, double b, double fraction)
        {
            double shortestDelta = ((b - a) % 360 + 540) % 360 - 180;
            double value = a + shortestDelta * fraction;

            // keep the same convention as the inputs: [0, 360) or [-180, 180)
            return a >= 0 && b >= 0
                ? (value % 360 + 360) % 360
                : ((value + 180) % 360 + 360) % 360 - 180;
        }
    }
}
