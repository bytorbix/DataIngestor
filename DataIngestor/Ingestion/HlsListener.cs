using DataIngestor.Channels;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;

namespace DataIngestor.Ingestion
{
    public class HlsListener(IConfiguration configuration, ILogger<HlsListener> logger, IHttpClientFactory httpClientFactory, ChannelRegistry channelRegistry)
    {
        private const string HLS_HOST_FIELD = "Hls:Host";
        private const string HLS_PORT_FIELD = "Hls:Port";
        private const string HLS_APP_FIELD = "Hls:App";

        private readonly string _hlsHost = configuration[HLS_HOST_FIELD] ?? throw new InvalidOperationException("Hls:Host is not configured");
        private readonly string _hlsPort = configuration[HLS_PORT_FIELD] ?? throw new InvalidOperationException("Hls:Port is not configured");
        private readonly string _hlsApp = configuration[HLS_APP_FIELD] ?? throw new InvalidOperationException("Hls:App is not configured");

        private const string HLS_SEGMENT_ARCHIVE_FIELD = "Hls:SegmentArchiveDirectory";

        // optional: keep every downloaded segment here (used for offline accuracy checks), otherwise they're deleted after processing
        private readonly string? _segmentArchiveDirectory = string.IsNullOrWhiteSpace(configuration[HLS_SEGMENT_ARCHIVE_FIELD]) ? null : configuration[HLS_SEGMENT_ARCHIVE_FIELD];

        private readonly ConcurrentDictionary<string, CancellationTokenSource> _tokens = new();
        private static readonly Regex EpochMillisRegex = new(@"(\d{10,})\|", RegexOptions.Compiled);
        private const int NoNewSegmentRetryDelayMs = 300;
        private const int PlaylistUnavailableRetryDelayMs = 2000;


        public void Start(string tailNumber)
        {
            CancellationTokenSource cts = new();
            if (!_tokens.TryAdd(tailNumber, cts))
            {
                logger.LogWarning("HLS Listener already running for {TailNumber}", tailNumber);
                cts.Dispose();
                return;
            }

            _ = Task.Run(() => RunAsync(tailNumber, cts.Token));

            logger.LogInformation("Started HLS listener for {TailNumber}", tailNumber);
        }

        public void Stop(string tailNumber)
        {
            if (_tokens.TryRemove(tailNumber, out CancellationTokenSource? cts))
            {
                cts.Cancel();
                cts.Dispose();
                logger.LogInformation("Stopped HLS listener for {TailNumber}", tailNumber);
            }
            else
            {
                logger.LogWarning("Attempted to stop unknown HLS listener for {TailNumber}", tailNumber);
            }

        }

        private string BuildMasterPlaylistUrl(string tailNumber) => $"http://{_hlsHost}:{_hlsPort}/{tailNumber}/_definst_/{tailNumber}.stream/playlist.m3u8";

        private async Task<Uri> ResolveChunklistUrlAsync(string tailNumber, CancellationToken cancellationToken)
        {
            Uri masterUrl = new(BuildMasterPlaylistUrl(tailNumber));

            HttpClient client = httpClientFactory.CreateClient();
            string masterPlaylist = await client.GetStringAsync(masterUrl, cancellationToken);

            string? chunklistLine = masterPlaylist
                .Split('\n')
                .Select(line => line.Trim())
                .FirstOrDefault(line => line.Length > 0 && !line.StartsWith('#'));

            if (chunklistLine is null)
                throw new InvalidOperationException($"No chunklist reference found in master playlist for {tailNumber}");

            return new Uri(masterUrl, chunklistLine);
        }
        private static (int targetDurationSeconds, long mediaSequence, List<string> segmentUris) ParseChunklist(string chunklistText)
        {
            int targetDurationSeconds = 10;
            long mediaSequence = 0;
            List<string> segmentUris = new();

            foreach (string rawLine in chunklistText.Split('\n'))
            {
                string line = rawLine.Trim();
                if (line.Length == 0) continue;

                if (line.StartsWith("#EXT-X-TARGETDURATION:"))
                    targetDurationSeconds = int.Parse(line["#EXT-X-TARGETDURATION:".Length..]);
                else if (line.StartsWith("#EXT-X-MEDIA-SEQUENCE:"))
                    mediaSequence = long.Parse(line["#EXT-X-MEDIA-SEQUENCE:".Length..]);
                else if (!line.StartsWith('#'))
                    segmentUris.Add(line);
            }

            return (targetDurationSeconds, mediaSequence, segmentUris);
        }
        private async Task<string> RunProcessCaptureOutputAsync(string fileName, IEnumerable<string> args, CancellationToken cancellationToken)
        {
            ProcessStartInfo startInfo = new()
            {
                FileName = fileName,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            };

            foreach (string arg in args)
                startInfo.ArgumentList.Add(arg);

            using Process process = new() { StartInfo = startInfo };
            process.Start();

            Task<string> stdoutTask = process.StandardOutput.ReadToEndAsync(cancellationToken);
            Task<string> stderrTask = process.StandardError.ReadToEndAsync(cancellationToken);

            await process.WaitForExitAsync(cancellationToken);
            string stdout = await stdoutTask;

            if (process.ExitCode != 0)
            {
                string stderr = await stderrTask;
                throw new InvalidOperationException($"{fileName} exited with code {process.ExitCode}: {stderr}");
            }

            return stdout;
        }

        private async Task<List<(double PtsTimeSeconds, string HexDump)>> GetId3PacketsAsync(string segmentPath, CancellationToken cancellationToken)
        {
            string[] args = { "-v", "error", "-select_streams", "d:0", "-show_packets", "-show_data", "-of", "json", segmentPath };
            string json = await RunProcessCaptureOutputAsync("ffprobe", args, cancellationToken);

            JsonNode? root = JsonNode.Parse(json);
            JsonArray packets = root?["packets"]?.AsArray() ?? new JsonArray();

            List<(double, string)> result = new();
            foreach (JsonNode? packet in packets)
            {
                string? ptsTimeText = packet?["pts_time"]?.GetValue<string>();
                string? hexDump = packet?["data"]?.GetValue<string>();

                if (ptsTimeText is null || hexDump is null)
                    continue;

                result.Add((double.Parse(ptsTimeText, CultureInfo.InvariantCulture), hexDump));
            }

            return result;
        }

        private static byte[] ParseHexDump(string hexDump)
        {
            List<byte> bytes = new();

            foreach (string rawLine in hexDump.Split('\n'))
            {
                string line = rawLine.Trim();
                if (line.Length == 0) continue;

                int colonIndex = line.IndexOf(':');
                if (colonIndex < 0) continue;

                string afterOffset = line[(colonIndex + 1)..];
                int asciiGapIndex = afterOffset.IndexOf("  ", StringComparison.Ordinal);
                string hexPart = asciiGapIndex >= 0 ? afterOffset[..asciiGapIndex] : afterOffset;

                foreach (string group in hexPart.Split(' ', StringSplitOptions.RemoveEmptyEntries))
                    for (int i = 0; i + 1 < group.Length; i += 2)
                        bytes.Add(Convert.ToByte(group.Substring(i, 2), 16));
            }

            return bytes.ToArray();
        }

        private static long? ExtractEpochMillis(byte[] id3Bytes)
        {
            string text = Encoding.Latin1.GetString(id3Bytes);
            Match match = EpochMillisRegex.Match(text);
            return match.Success ? long.Parse(match.Groups[1].Value, CultureInfo.InvariantCulture) : null;
        }

        private async Task<List<double>> GetVideoFramePtsTimesAsync(string segmentPath, CancellationToken cancellationToken)
        {
            string[] args = { "-v", "error", "-select_streams", "v:0", "-show_entries", "frame=pts_time", "-of", "default=noprint_wrappers=1:nokey=1", segmentPath };
            string output = await RunProcessCaptureOutputAsync("ffprobe", args, cancellationToken);

            List<double> ptsTimes = new();
            foreach (string rawLine in output.Split('\n'))
            {
                string line = rawLine.Trim();
                if (line.Length == 0) continue;

                if (double.TryParse(line, NumberStyles.Float, CultureInfo.InvariantCulture, out double ptsTime))
                    ptsTimes.Add(ptsTime);
                else
                    logger.LogWarning("Unparseable ffprobe frame output: {Line}", line);
            }

            return ptsTimes;
        }

        // downloads the segment once so both ffprobe passes read a local file instead of fetching it again
        private async Task<string> DownloadSegmentAsync(HttpClient client, string tailNumber, Uri segmentUrl, CancellationToken cancellationToken)
        {
            string directory = _segmentArchiveDirectory is not null
                ? Path.Combine(_segmentArchiveDirectory, tailNumber)
                : Path.Combine(Path.GetTempPath(), "DataIngestor", tailNumber);
            Directory.CreateDirectory(directory);

            string segmentPath = Path.Combine(directory, Path.GetFileName(segmentUrl.LocalPath));
            byte[] bytes = await client.GetByteArrayAsync(segmentUrl, cancellationToken);
            await File.WriteAllBytesAsync(segmentPath, bytes, cancellationToken);
            return segmentPath;
        }

        private async Task ProcessSegmentAsync(HttpClient client, string tailNumber, Uri segmentUrl, CancellationToken cancellationToken)
        {
            var channel = channelRegistry.Get(tailNumber);
            if (channel is null)
                return;

            string segmentPath = await DownloadSegmentAsync(client, tailNumber, segmentUrl, cancellationToken);
            try
            {
                Task<List<(double PtsTimeSeconds, long UtcMs)>> id3Task = GetId3EventsAsync(segmentPath, cancellationToken);
                Task<List<double>> framesTask = GetVideoFramePtsTimesAsync(segmentPath, cancellationToken);
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

        private async Task<List<(double PtsTimeSeconds, long UtcMs)>> GetId3EventsAsync(string segmentPath, CancellationToken cancellationToken)
        {
            List<(double PtsTimeSeconds, string HexDump)> packets = await GetId3PacketsAsync(segmentPath, cancellationToken);
            List<(double, long)> events = new();

            foreach (var (ptsTimeSeconds, hexDump) in packets)
            {
                byte[] bytes = ParseHexDump(hexDump);
                long? utcMs = ExtractEpochMillis(bytes);

                if (utcMs is not null)
                    events.Add((ptsTimeSeconds, utcMs.Value));
            }

            return events;
        }


        public async Task RunAsync(string tailNumber, CancellationToken cancellationToken)
        {
            HttpClient client = httpClientFactory.CreateClient();
            Uri? chunklistUrl = null;
            long lastProcessedSequence = -1;

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        // (re)resolve until the stream is published; a new chunklist means Wowza restarted the stream and sequences start over
                        Uri resolvedUrl = await ResolveChunklistUrlAsync(tailNumber, cancellationToken);
                        if (resolvedUrl != chunklistUrl)
                        {
                            chunklistUrl = resolvedUrl;
                            lastProcessedSequence = -1;
                            logger.LogInformation("[{TailNumber}] Resolved chunklist URL: {ChunklistUrl}", tailNumber, chunklistUrl);
                        }

                        lastProcessedSequence = await PollChunklistAsync(client, tailNumber, chunklistUrl, lastProcessedSequence, cancellationToken);
                    }
                    catch (HttpRequestException ex)
                    {
                        logger.LogWarning("[{TailNumber}] HLS playlist unavailable ({Message}), retrying in {DelayMs}ms", tailNumber, ex.Message, PlaylistUnavailableRetryDelayMs);
                        await Task.Delay(PlaylistUnavailableRetryDelayMs, cancellationToken);
                    }
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex) { logger.LogError(ex, "HLS listener for {TailNumber} faulted.", tailNumber); }
        }

        // polls until the chunklist becomes unavailable, then returns the last processed sequence so the caller can re-resolve
        private async Task<long> PollChunklistAsync(HttpClient client, string tailNumber, Uri chunklistUrl, long lastProcessedSequence, CancellationToken cancellationToken)
        {
            while (true)
            {
                string chunklistText;
                try
                {
                    chunklistText = await client.GetStringAsync(chunklistUrl, cancellationToken);
                }
                catch (HttpRequestException ex)
                {
                    // playlist dropped mid-stream: go back to resolving, but keep our position in case it's the same chunklist
                    logger.LogWarning("[{TailNumber}] Chunklist unavailable ({Message}), re-resolving in {DelayMs}ms", tailNumber, ex.Message, PlaylistUnavailableRetryDelayMs);
                    await Task.Delay(PlaylistUnavailableRetryDelayMs, cancellationToken);
                    return lastProcessedSequence;
                }

                var (_, mediaSequence, segmentUris) = ParseChunklist(chunklistText);

                if (lastProcessedSequence >= 0 && mediaSequence > lastProcessedSequence + 1)
                {
                    logger.LogWarning("[{TailNumber}] Missed segments #{From}-#{To}: they left the playlist before being processed", tailNumber, lastProcessedSequence + 1, mediaSequence - 1);
                }

                bool foundNewSegment = false;

                for (int i = 0; i < segmentUris.Count; i++)
                {
                    long sequence = mediaSequence + i;
                    if (sequence <= lastProcessedSequence)
                        continue;

                    Uri segmentUrl = new(chunklistUrl, segmentUris[i]);
                    logger.LogInformation("[{TailNumber}] New segment #{Sequence}: {SegmentUrl}", tailNumber, sequence, segmentUrl);

                    try
                    {
                        await ProcessSegmentAsync(client, tailNumber, segmentUrl, cancellationToken);
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException)
                    {
                        logger.LogError(ex, "[{TailNumber}] Failed to process segment #{Sequence}, skipping it", tailNumber, sequence);
                    }

                    lastProcessedSequence = sequence;
                    foundNewSegment = true;
                }

                if (!foundNewSegment)
                    await Task.Delay(NoNewSegmentRetryDelayMs, cancellationToken);
            }
        }
    }
}
