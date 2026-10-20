using System.Text.Json;
using System.Text.Json.Nodes;

namespace DataIngestor.Synchronizing
{
    // one flight = one register -> unregister of a tail; its id comes from the data's own first stamp, so a replay keeps its original time
    public class FlightOutput : IDisposable
    {
        private const string CHUNKS_DIRECTORY = "chunks";
        private const string SEGMENTS_DIRECTORY = "segments";
        private const string TEMP_SUFFIX = ".tmp";
        private const string MANIFEST_FILE = "flight.json";

        private readonly string _outputDirectory;
        private readonly string _tailNumber;
        private readonly TimeProvider _clock;

        private string? _flightDirectory;
        // one chunk per video segment; it's written as NNNNNN.jsonl.tmp and renamed once complete, so the uploader only sees whole files
        private StreamWriter? _chunkWriter;
        private string? _chunkTempPath;
        private int _chunkNumber;
        // the open chunk's staged .ts, owned by this flight from its first frame; it moves into segments/ when the chunk closes
        private string? _segmentPath;
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
        // records arrive in order, so the first record of segment N+1 means chunk N is complete
        public void Write(long videoUtcMs, int segmentNumber, string segmentPath, JsonObject record)
        {
            if (_flightDirectory is null)
                Open(videoUtcMs);

            if (_chunkWriter is null || segmentNumber != _chunkNumber)
            {
                CloseChunk();
                OpenChunk(segmentNumber, segmentPath);
            }

            _recordedEndUtcMs = Math.Max(_recordedEndUtcMs, videoUtcMs);
            record["flightId"] = FlightId;
            _chunkWriter!.WriteLine(record.ToJsonString());
        }

        public static string FormatChunkName(int segmentNumber) => $"{segmentNumber:D6}.jsonl";

        // where a segment lives inside its flight; frame records point here, not at the Wowza URL
        public static string FormatSegmentKey(int segmentNumber) => $"{SEGMENTS_DIRECTORY}/{segmentNumber:D6}.ts";

        private void OpenChunk(int segmentNumber, string segmentPath)
        {
            _chunkNumber = segmentNumber;
            _segmentPath = segmentPath;
            _chunkTempPath = Path.Combine(_flightDirectory!, CHUNKS_DIRECTORY, FormatChunkName(segmentNumber) + TEMP_SUFFIX);
            _chunkWriter = new StreamWriter(_chunkTempPath, append: false) { AutoFlush = true };
        }

        // the rename is the "done" signal: it's atomic, so a reader sees either the .tmp or the whole chunk
        private void CloseChunk()
        {
            if (_chunkWriter is null)
                return;

            _chunkWriter.Dispose();
            _chunkWriter = null;

            // segment first, so a finished chunk never shows up without its video
            if (File.Exists(_segmentPath))
                File.Move(_segmentPath, Path.Combine(_flightDirectory!, FormatSegmentKey(_chunkNumber)), overwrite: true);
            _segmentPath = null;

            File.Move(_chunkTempPath!, _chunkTempPath![..^TEMP_SUFFIX.Length], overwrite: true);
            _chunkTempPath = null;
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
            Directory.CreateDirectory(Path.Combine(_flightDirectory, CHUNKS_DIRECTORY));
            Directory.CreateDirectory(Path.Combine(_flightDirectory, SEGMENTS_DIRECTORY));

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
            if (_chunkWriter is null)
                return;

            CloseChunk();
            WriteManifest(closed: true);
        }
    }
}
