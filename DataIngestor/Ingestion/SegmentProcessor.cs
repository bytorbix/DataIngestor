using DataIngestor.Channels;

namespace DataIngestor.Ingestion
{
    // turns one HLS segment into FrameRecords: download, probe, decode the ID3 UTC anchor, timestamp every frame
    public class SegmentProcessor(IConfiguration configuration, ILogger<SegmentProcessor> logger, IHttpClientFactory httpClientFactory, ChannelRegistry channelRegistry, FfprobeRunner ffprobe)
    {
        private const string HLS_SEGMENT_ARCHIVE_FIELD = "Hls:SegmentArchiveDirectory";

        // optional: keep every downloaded segment here (used for offline accuracy checks), otherwise they're deleted after processing
        private readonly string? _segmentArchiveDirectory = string.IsNullOrWhiteSpace(configuration[HLS_SEGMENT_ARCHIVE_FIELD]) ? null : configuration[HLS_SEGMENT_ARCHIVE_FIELD];

        public async Task ProcessSegmentAsync(string tailNumber, Uri segmentUrl, CancellationToken cancellationToken)
        {
            Channel? channel = channelRegistry.Get(tailNumber);
            if (channel is null)
                return;

            string segmentPath = await DownloadSegmentAsync(tailNumber, segmentUrl, cancellationToken);
            try
            {
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
                    long videoUtcMs = AnchorUtcMs + (long)((framePtsTime - AnchorPtsTimeSeconds) * 1000);
                    channel.FrameChannel.Writer.TryWrite(new FrameRecord(framePtsTime, videoUtcMs, segmentUrl.ToString()));
                }
            }
            finally
            {
                if (_segmentArchiveDirectory is null)
                    File.Delete(segmentPath);
            }
        }

        // downloads the segment once so both ffprobe passes read a local file instead of fetching it again
        private async Task<string> DownloadSegmentAsync(string tailNumber, Uri segmentUrl, CancellationToken cancellationToken)
        {
            string directory = _segmentArchiveDirectory is not null
                ? Path.Combine(_segmentArchiveDirectory, tailNumber)
                : Path.Combine(Path.GetTempPath(), "DataIngestor", tailNumber);
            Directory.CreateDirectory(directory);

            string segmentPath = Path.Combine(directory, Path.GetFileName(segmentUrl.LocalPath));
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
