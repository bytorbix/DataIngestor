using DataIngestor.Channels;

namespace DataIngestor.Ingestion
{
    // turns one HLS segment into FrameRecords: download, probe, decode the ID3 UTC anchor, timestamp every frame
    public class SegmentProcessor(IConfiguration configuration, ILogger<SegmentProcessor> logger, IHttpClientFactory httpClientFactory, ChannelRegistry channelRegistry, FfprobeRunner ffprobe)
    {
        private const string HLS_SEGMENT_ARCHIVE_FIELD = "Hls:SegmentArchiveDirectory";
        private const string OUTPUT_DIRECTORY_FIELD = "Output:Directory";
        // dot-prefixed so the uploader skips it; it sits under the output directory so moving a segment into its flight is an atomic rename
        private const string STAGING_DIRECTORY = ".incoming";

        // optional: also copy every downloaded segment here (used for offline accuracy checks)
        private readonly string? _segmentArchiveDirectory = string.IsNullOrWhiteSpace(configuration[HLS_SEGMENT_ARCHIVE_FIELD]) ? null : configuration[HLS_SEGMENT_ARCHIVE_FIELD];
        private readonly string _outputDirectory = configuration[OUTPUT_DIRECTORY_FIELD] ?? throw new InvalidOperationException("Output:Directory is not configured");

        private string StagingDirectory(string tailNumber) => Path.Combine(_outputDirectory, tailNumber, STAGING_DIRECTORY);

        // a new channel is a new flight, so anything still staged is left over from a crash
        public void ClearStaging(string tailNumber)
        {
            string directory = StagingDirectory(tailNumber);
            if (Directory.Exists(directory))
                Directory.Delete(directory, recursive: true);
        }

        // stateless: the caller owns the flight's segment count and passes each segment's number in
        public async Task ProcessSegmentAsync(string tailNumber, int segmentNumber, Uri segmentUrl, double segmentDurationSeconds, CancellationToken cancellationToken)
        {
            Channel? channel = channelRegistry.Get(tailNumber);
            if (channel is null)
                return;

            string segmentPath = await DownloadSegmentAsync(tailNumber, segmentNumber, segmentUrl, cancellationToken);
            // the file is ours until a frame carrying its path is written; from then on the flight output owns it
            bool handedOff = false;
            try
            {
                if (_segmentArchiveDirectory is not null)
                {
                    string archiveDirectory = Path.Combine(_segmentArchiveDirectory, tailNumber);
                    Directory.CreateDirectory(archiveDirectory);
                    File.Copy(segmentPath, Path.Combine(archiveDirectory, Path.GetFileName(segmentUrl.LocalPath)), overwrite: true);
                }

                Task<List<(double PtsTimeSeconds, long UtcMs)>> id3Task = GetId3EventsAsync(segmentPath, cancellationToken);
                Task<List<double>> framesTask = ffprobe.GetVideoFramePtsTimesAsync(segmentPath, cancellationToken);
                List<(double PtsTimeSeconds, long UtcMs)> id3Events = await id3Task;
                List<double> framePtsTimes = await framesTask;

                if (id3Events.Count == 0)
                {
                    logger.LogWarning("[{TailNumber}] No ID3 timestamp found in segment {SegmentUrl}, dropping {FrameCount} frames", tailNumber, segmentUrl, framePtsTimes.Count);
                    return;
                }

                (double AnchorPtsTimeSeconds, long AnchorUtcMs) = id3Events[0];

                // every frame carries its own UTC, so the whole segment is handed over at once (no real-time pacing needed)
                foreach (double framePtsTime in framePtsTimes)
                {
                    long videoUtcMs = AnchorUtcMs + (long)Math.Round((framePtsTime - AnchorPtsTimeSeconds) * 1000);
                    if (channel.FrameChannel.Writer.TryWrite(new FrameRecord(framePtsTime, videoUtcMs, segmentNumber, segmentPath, segmentDurationSeconds)))
                        handedOff = true;
                }
            }
            finally
            {
                if (!handedOff)
                    File.Delete(segmentPath);
            }
        }

        // downloads the segment once so both ffprobe passes read a local file instead of fetching it again
        // named by flight segment number, since Wowza reuses file names after a restart
        private async Task<string> DownloadSegmentAsync(string tailNumber, int segmentNumber, Uri segmentUrl, CancellationToken cancellationToken)
        {
            string directory = StagingDirectory(tailNumber);
            Directory.CreateDirectory(directory);

            string segmentPath = Path.Combine(directory, $"{segmentNumber:D6}.ts");
            HttpClient client = httpClientFactory.CreateClient();
            byte[] bytes = await client.GetByteArrayAsync(segmentUrl, cancellationToken);
            await File.WriteAllBytesAsync(segmentPath, bytes, cancellationToken);
            return segmentPath;
        }

        private async Task<List<(double PtsTimeSeconds, long UtcMs)>> GetId3EventsAsync(string segmentPath, CancellationToken cancellationToken)
        {
            List<(double PtsTimeSeconds, string HexDump)> packets = await ffprobe.GetId3PacketsAsync(segmentPath, cancellationToken);
            List<(double, long)> events = new();

            foreach ((double ptsTimeSeconds, string hexDump) in packets)
            {
                byte[] bytes = Id3TimestampDecoder.ParseHexDump(hexDump);
                long? utcMs = Id3TimestampDecoder.ExtractEpochMillis(bytes);

                if (utcMs is not null)
                    events.Add((ptsTimeSeconds, utcMs.Value));
            }

            return events;
        }
    }
}
