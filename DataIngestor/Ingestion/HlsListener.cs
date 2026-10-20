using DataIngestor.Channels;
using System.Collections.Concurrent;

namespace DataIngestor.Ingestion
{
    public class HlsListener(IConfiguration configuration, ILogger<HlsListener> logger, IHttpClientFactory httpClientFactory, SegmentProcessor segmentProcessor, ChannelRegistry channelRegistry)
    {
        // Stop ends polling for new segments (graceful); Abort also cancels the segment in progress (hard)
        private sealed class ListenerRun
        {
            public CancellationTokenSource Stop { get; } = new();
            public CancellationTokenSource Abort { get; } = new();
            public Task Task { get; set; } = Task.CompletedTask;
            public int TargetDurationSeconds { get; set; } = DefaultTargetDurationSeconds;
        }

        private const string HLS_HOST_FIELD = "Hls:Host";
        private const string HLS_PORT_FIELD = "Hls:Port";
        private const string HLS_APP_FIELD = "Hls:App";

        private readonly string _hlsHost = configuration[HLS_HOST_FIELD] ?? throw new InvalidOperationException("Hls:Host is not configured");
        private readonly string _hlsPort = configuration[HLS_PORT_FIELD] ?? throw new InvalidOperationException("Hls:Port is not configured");
        private readonly string _hlsApp = configuration[HLS_APP_FIELD] ?? throw new InvalidOperationException("Hls:App is not configured");

        // token sources aren't disposed: StopAsync may still cancel Abort after the run has finished
        private readonly ConcurrentDictionary<string, ListenerRun> _runs = new();
        private const int NoNewSegmentRetryDelayMs = 300;
        private const int PlaylistUnavailableRetryDelayMs = 2000;
        // HLS spec default when a chunklist omits #EXT-X-TARGETDURATION
        private const int DefaultTargetDurationSeconds = 10;
        private const string TargetDurationTag = "#EXT-X-TARGETDURATION:";
        private const string MediaSequenceTag = "#EXT-X-MEDIA-SEQUENCE:";


        public void Start(string tailNumber)
        {
            ListenerRun run = new();
            if (!_runs.TryAdd(tailNumber, run))
            {
                logger.LogWarning("HLS Listener already running for {TailNumber}", tailNumber);
                return;
            }

            run.Task = Task.Run(() => RunAsync(tailNumber, run));

            logger.LogInformation("Started HLS listener for {TailNumber}", tailNumber);
        }

        // flight end waits about one segment: long enough for the segment in progress to finish
        public TimeSpan GetDrainTimeout(string tailNumber)
        {
            int targetDurationSeconds = _runs.TryGetValue(tailNumber, out ListenerRun? run) ? run.TargetDurationSeconds : DefaultTargetDurationSeconds;
            return TimeSpan.FromSeconds(targetDurationSeconds + 1);
        }

        // graceful: no new segments, the one in progress finishes, then the frame channel completes; aborts once the timeout passes
        public async Task StopAsync(string tailNumber, CancellationToken timeout)
        {
            if (!_runs.TryRemove(tailNumber, out ListenerRun? run))
            {
                logger.LogWarning("Attempted to stop unknown HLS listener for {TailNumber}", tailNumber);
                return;
            }

            run.Stop.Cancel();
            try
            {
                await run.Task.WaitAsync(timeout);
                logger.LogInformation("Stopped HLS listener for {TailNumber}", tailNumber);
            }
            catch (OperationCanceledException)
            {
                logger.LogWarning("HLS listener for {TailNumber} didn't finish its last segment in time, aborting it", tailNumber);
                run.Abort.Cancel();
                await run.Task;
            }
        }

        // hard stop, used when registration fails
        public void Stop(string tailNumber)
        {
            if (_runs.TryRemove(tailNumber, out ListenerRun? run))
            {
                run.Abort.Cancel();
                run.Stop.Cancel();
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

        private async Task RunAsync(string tailNumber, ListenerRun run)
        {
            CancellationToken stopToken = run.Stop.Token;
            HttpClient client = httpClientFactory.CreateClient();
            Uri? chunklistUrl = null;
            long lastProcessedSequence = -1;
            // one RunAsync = one flight, so the count starts here; unlike lastProcessedSequence it survives a Wowza restart
            int segmentNumber = 0;

            try
            {
                while (!stopToken.IsCancellationRequested)
                {
                    try
                    {
                        // (re)resolve until the stream is published; a new chunklist means Wowza restarted the stream and sequences start over
                        Uri resolvedUrl = await ResolveChunklistUrlAsync(tailNumber, stopToken);
                        if (resolvedUrl != chunklistUrl)
                        {
                            chunklistUrl = resolvedUrl;
                            lastProcessedSequence = -1;
                            logger.LogInformation("[{TailNumber}] Resolved chunklist URL: {ChunklistUrl}", tailNumber, chunklistUrl);
                        }

                        (lastProcessedSequence, segmentNumber) = await PollChunklistAsync(client, tailNumber, chunklistUrl, lastProcessedSequence, segmentNumber, run);
                    }
                    catch (HttpRequestException ex)
                    {
                        logger.LogWarning("[{TailNumber}] HLS playlist unavailable ({Message}), retrying in {DelayMs}ms", tailNumber, ex.Message, PlaylistUnavailableRetryDelayMs);
                        await Task.Delay(PlaylistUnavailableRetryDelayMs, stopToken);
                    }
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex) { logger.LogError(ex, "HLS listener for {TailNumber} faulted.", tailNumber); }
            finally
            {
                // no more frames for this flight: lets the synchronizer drain what's left and finish
                channelRegistry.Get(tailNumber)?.FrameChannel.Writer.TryComplete();
            }
        }

        // polls until the chunklist becomes unavailable, then returns the last processed sequence and segment count so the caller can re-resolve
        private async Task<(long LastProcessedSequence, int SegmentNumber)> PollChunklistAsync(HttpClient client, string tailNumber, Uri chunklistUrl, long lastProcessedSequence, int segmentNumber, ListenerRun run)
        {
            CancellationToken stopToken = run.Stop.Token;
            while (true)
            {
                string chunklistText;
                try
                {
                    chunklistText = await client.GetStringAsync(chunklistUrl, stopToken);
                }
                catch (HttpRequestException ex)
                {
                    // playlist dropped mid-stream: go back to resolving, but keep our position in case it's the same chunklist
                    logger.LogWarning("[{TailNumber}] Chunklist unavailable ({Message}), re-resolving in {DelayMs}ms", tailNumber, ex.Message, PlaylistUnavailableRetryDelayMs);
                    await Task.Delay(PlaylistUnavailableRetryDelayMs, stopToken);
                    return (lastProcessedSequence, segmentNumber);
                }

                (int targetDurationSeconds, long mediaSequence, List<string> segmentUris) = ParseChunklist(chunklistText);
                run.TargetDurationSeconds = targetDurationSeconds;

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

                    // flight end: the segment in progress was the last one
                    stopToken.ThrowIfCancellationRequested();

                    Uri segmentUrl = new(chunklistUrl, segmentUris[i]);
                    // numbered before processing, so a segment that fails still uses its number and shows up as a gap
                    segmentNumber++;
                    logger.LogInformation("[{TailNumber}] New segment #{Sequence} (flight segment {SegmentNumber}): {SegmentUrl}", tailNumber, sequence, segmentNumber, segmentUrl);

                    try
                    {
                        await segmentProcessor.ProcessSegmentAsync(tailNumber, segmentNumber, segmentUrl, run.Abort.Token);
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException)
                    {
                        logger.LogError(ex, "[{TailNumber}] Failed to process segment #{Sequence}, skipping it", tailNumber, sequence);
                    }

                    lastProcessedSequence = sequence;
                    foundNewSegment = true;
                }

                if (!foundNewSegment)
                    await Task.Delay(NoNewSegmentRetryDelayMs, stopToken);
            }
        }
    }
}
