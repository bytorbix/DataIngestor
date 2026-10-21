using DataIngestor.Channels;
using System.Globalization;
using System.Text;
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
        public const string ManifestFile = "flight.json";
        // tells the uploader this flight id started a new run, so the old run in the archive goes before any of this one is uploaded
        public const string ResetMarkerFile = ".reset";
        private const string PLAYLIST_FILE = "playlist.m3u8";
        // never written; a gap entry still needs a URI, and the player skips it without fetching
        private const string GAP_URI = SEGMENTS_DIRECTORY + "/gap.ts";
        // ID3 anchoring and ms rounding leave neighbouring segments a few ms apart; anything smaller than this isn't a lost segment
        private const long MinGapMs = 100;

        public record PlaylistEntry(string Uri, double DurationSeconds, bool IsGap);

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
        // the recorded playlist, built as segments open; a gap is wherever a segment starts after the previous one ended
        private readonly List<PlaylistEntry> _playlist = new();
        private long? _previousSegmentEndUtcMs;
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
        public void Write(long videoUtcMs, FrameRecord frame, JsonObject record)
        {
            if (_flightDirectory is null)
                Open(videoUtcMs);

            if (_chunkWriter is null || frame.SegmentNumber != _chunkNumber)
            {
                CloseChunk();
                OpenChunk(frame.SegmentNumber, frame.SegmentPath);
                AddToPlaylist(videoUtcMs, frame);
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

        // the segment's first frame marks its start; overlaps (timing noise, a Wowza restart) are listed as they come
        private void AddToPlaylist(long startUtcMs, FrameRecord frame)
        {
            if (_previousSegmentEndUtcMs is long previousEndUtcMs && startUtcMs - previousEndUtcMs >= MinGapMs)
                _playlist.Add(new PlaylistEntry(GAP_URI, (startUtcMs - previousEndUtcMs) / 1000.0, IsGap: true));

            _playlist.Add(new PlaylistEntry(FormatSegmentKey(frame.SegmentNumber), frame.SegmentDurationSeconds, IsGap: false));
            _previousSegmentEndUtcMs = startUtcMs + (long)Math.Round(frame.SegmentDurationSeconds * 1000);
        }

        // VOD playlist: every #EXTINF must fit the target duration, so long gaps are split into equal pieces
        public static string FormatPlaylist(IReadOnlyList<PlaylistEntry> entries)
        {
            double longestSegmentSeconds = entries.Where(entry => !entry.IsGap).Select(entry => entry.DurationSeconds).DefaultIfEmpty(1).Max();
            int targetDurationSeconds = Math.Max(1, (int)Math.Ceiling(longestSegmentSeconds));

            StringBuilder playlist = new();
            playlist.Append("#EXTM3U\n");
            playlist.Append("#EXT-X-VERSION:3\n");
            playlist.Append(CultureInfo.InvariantCulture, $"#EXT-X-TARGETDURATION:{targetDurationSeconds}\n");
            playlist.Append("#EXT-X-PLAYLIST-TYPE:VOD\n");

            foreach ((string uri, double durationSeconds, bool isGap) in entries)
            {
                int pieces = isGap ? (int)Math.Ceiling(durationSeconds / targetDurationSeconds) : 1;
                for (int i = 0; i < pieces; i++)
                {
                    if (isGap)
                        playlist.Append("#EXT-X-GAP\n");
                    playlist.Append(CultureInfo.InvariantCulture, $"#EXTINF:{durationSeconds / pieces:0.000},\n{uri}\n");
                }
            }

            playlist.Append("#EXT-X-ENDLIST\n");
            return playlist.ToString();
        }

        private void WritePlaylist()
        {
            string path = Path.Combine(_flightDirectory!, PLAYLIST_FILE);
            File.WriteAllText(path + TEMP_SUFFIX, FormatPlaylist(_playlist));
            File.Move(path + TEMP_SUFFIX, path, overwrite: true);
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
            // on every open, not only after a local wipe: the old run may have been uploaded and removed from disk already
            File.WriteAllText(Path.Combine(_flightDirectory, ResetMarkerFile), string.Empty);

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

            // written whole then renamed, so the uploader never reads a half-written status
            string path = Path.Combine(_flightDirectory!, ManifestFile);
            File.WriteAllText(path + TEMP_SUFFIX, manifest.ToJsonString(new JsonSerializerOptions { WriteIndented = true }));
            File.Move(path + TEMP_SUFFIX, path, overwrite: true);
        }

        // unregister ends the flight; a flight that never got a record leaves nothing behind
        public void Dispose()
        {
            if (_chunkWriter is null)
                return;

            CloseChunk();
            // the closed flight.json comes last: it says everything else is in place
            WritePlaylist();
            WriteManifest(closed: true);
        }
    }
}
