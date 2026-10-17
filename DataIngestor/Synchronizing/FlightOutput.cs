using System.Text.Json;
using System.Text.Json.Nodes;

namespace DataIngestor.Synchronizing
{
    // one flight = one register -> unregister of a tail; its id comes from the data's own first stamp, so a replay keeps its original time
    public class FlightOutput : IDisposable
    {
        private const string FRAMES_FILE = "frames.jsonl";
        private const string MANIFEST_FILE = "flight.json";

        private readonly string _outputDirectory;
        private readonly string _tailNumber;
        private readonly TimeProvider _clock;

        private string? _flightDirectory;
        private StreamWriter? _framesWriter;
        private long _recordedStartUtcMs;
        private long _recordedEndUtcMs;
        private DateTimeOffset _ingestedAtUtc;

        public string? FlightId { get; private set; }

        public FlightOutput(string outputDirectory, string tailNumber, TimeProvider? clock = null)
        {
            _outputDirectory = outputDirectory;
            _tailNumber = tailNumber;
            _clock = clock ?? TimeProvider.System;
        }

        public static string FormatFlightId(string tailNumber, long recordedStartUtcMs)
            => $"{tailNumber}-{DateTimeOffset.FromUnixTimeMilliseconds(recordedStartUtcMs).UtcDateTime:yyyyMMdd'T'HHmmss'Z'}";

        // the flight is opened by its first record, since that's the first time its start is known
        public void Write(long videoUtcMs, JsonObject record)
        {
            if (_framesWriter is null)
                Open(videoUtcMs);

            _recordedEndUtcMs = Math.Max(_recordedEndUtcMs, videoUtcMs);
            record["flightId"] = FlightId;
            _framesWriter!.WriteLine(record.ToJsonString());
        }

        private void Open(long recordedStartUtcMs)
        {
            FlightId = FormatFlightId(_tailNumber, recordedStartUtcMs);
            _recordedStartUtcMs = recordedStartUtcMs;
            _recordedEndUtcMs = recordedStartUtcMs;
            _ingestedAtUtc = _clock.GetUtcNow();

            // same clip replayed = same flight id, so the new run replaces the old one
            _flightDirectory = Path.Combine(_outputDirectory, _tailNumber, FlightId);
            if (Directory.Exists(_flightDirectory))
                Directory.Delete(_flightDirectory, recursive: true);
            Directory.CreateDirectory(_flightDirectory);

            _framesWriter = new StreamWriter(Path.Combine(_flightDirectory, FRAMES_FILE), append: false) { AutoFlush = true };
            WriteManifest(closed: false);
        }

        private void WriteManifest(bool closed)
        {
            JsonObject manifest = new()
            {
                ["flightId"] = FlightId,
                ["tailNumber"] = _tailNumber,
                ["recordedStartUtc"] = DateTimeOffset.FromUnixTimeMilliseconds(_recordedStartUtcMs).UtcDateTime.ToString("O"),
                ["recordedEndUtc"] = closed ? DateTimeOffset.FromUnixTimeMilliseconds(_recordedEndUtcMs).UtcDateTime.ToString("O") : null,
                ["ingestedAtUtc"] = _ingestedAtUtc.UtcDateTime.ToString("O"),
                ["status"] = closed ? "closed" : "open"
            };

            File.WriteAllText(Path.Combine(_flightDirectory!, MANIFEST_FILE), manifest.ToJsonString(new JsonSerializerOptions { WriteIndented = true }));
        }

        // unregister ends the flight; a flight that never got a record leaves nothing behind
        public void Dispose()
        {
            if (_framesWriter is null)
                return;

            _framesWriter.Dispose();
            _framesWriter = null;
            WriteManifest(closed: true);
        }
    }
}
