using System.Collections.Concurrent;

namespace DataIngestor.Ingestion
{
    public class HlsListener(IConfiguration configuration, ILogger<HlsListener> logger, IHttpClientFactory httpClientFactory, SegmentProcessor segmentProcessor)
    {
        private const string HLS_HOST_FIELD = "Hls:Host";
        private const string HLS_PORT_FIELD = "Hls:Port";
        private const string HLS_APP_FIELD = "Hls:App";

        private readonly string _hlsHost = configuration[HLS_HOST_FIELD] ?? throw new InvalidOperationException("Hls:Host is not configured");
        private readonly string _hlsPort = configuration[HLS_PORT_FIELD] ?? throw new InvalidOperationException("Hls:Port is not configured");
        private readonly string _hlsApp = configuration[HLS_APP_FIELD] ?? throw new InvalidOperationException("Hls:App is not configured");

        private readonly ConcurrentDictionary<string, CancellationTokenSource> _tokens = new();
        private const int NoNewSegmentRetryDelayMs = 300;
        private const int PlaylistUnavailableRetryDelayMs = 2000;
        // HLS spec default when a chunklist omits #EXT-X-TARGETDURATION
        private const int DefaultTargetDurationSeconds = 10;
        private const string TargetDurationTag = "#EXT-X-TARGETDURATION:";
        private const string MediaSequenceTag = "#EXT-X-MEDIA-SEQUENCE:";


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
            int targetDurationSeconds = DefaultTargetDurationSeconds;
            long mediaSequence = 0;
            List<string> segmentUris = new();

            foreach (string rawLine in chunklistText.Split('\n'))
            {
                string line = rawLine.Trim();
                if (line.Length == 0) continue;

                if (line.StartsWith(TargetDurationTag))
                    targetDurationSeconds = int.Parse(line[TargetDurationTag.Length..]);
                else if (line.StartsWith(MediaSequenceTag))
                    mediaSequence = long.Parse(line[MediaSequenceTag.Length..]);
                else if (!line.StartsWith('#'))
                    segmentUris.Add(line);
            }

            return (targetDurationSeconds, mediaSequence, segmentUris);
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

                (_, long mediaSequence, List<string> segmentUris) = ParseChunklist(chunklistText);

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
                        await segmentProcessor.ProcessSegmentAsync(tailNumber, segmentUrl, cancellationToken);
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
