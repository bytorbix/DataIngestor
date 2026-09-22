using DataIngestor.Channels;
using System.Threading.Channels;
using Channel = DataIngestor.Channels.Channel;

namespace DataIngestor.Synchronizing
{
    public class Synchronizer(ChannelRegistry channelRegistry, string tailNumber, ILogger<Synchronizer> logger)
    {
        private const int FrameWaitTimeoutMs = 150;

        private record WaitingFrame(FrameRecord Frame, long VideoUtcMs, long EnqueuedAtMs);
        private readonly Channel _channel = channelRegistry.Get(tailNumber)
            ?? throw new InvalidOperationException($"Channel not found for {tailNumber}");

        private readonly PriorityQueue<TelemetryRecord, long> _buffer = new();
        private readonly Queue<WaitingFrame> _waitingFrames = new();

        private List<TelemetryRecord> DrainMatching(long videoUtcMs)
        {
            List<TelemetryRecord> matched = new();

            while (_buffer.TryPeek(out var nextTelemetry, out long priority) && priority <= videoUtcMs)
            {
                _buffer.Dequeue();
                matched.Add(nextTelemetry);
            }

            return matched;
        }

        private void EmitSyncedFrame(FrameRecord frameRecord, long videoUtcMs, List<TelemetryRecord> matched)
        {
            SyncedFrame synced = new SyncedFrame(frameRecord, matched);

            long? offsetMissMs = matched.Count > 0 ? videoUtcMs - matched[^1].TimeMs : null;
            logger.LogInformation("[{TailNumber}] video={VideoUtcMs}ms telemetryCount={TelemetryCount} offsetMissMs={OffsetMissMs}", tailNumber, videoUtcMs, synced.Telemetry.Count, offsetMissMs);
        }

        private void TryUnblockWaitingFrames()
        {
            while (_waitingFrames.TryPeek(out var waiting))
            {
                List<TelemetryRecord> matched = DrainMatching(waiting.VideoUtcMs);
                if (matched.Count == 0)
                {
                    break;
                }

                _waitingFrames.Dequeue();
                EmitSyncedFrame(waiting.Frame, waiting.VideoUtcMs, matched);
            }
        }

        private Task CreateWaitingFrameTimeoutTask(CancellationToken cancellationToken)
        {
            if (!_waitingFrames.TryPeek(out var oldest))
            {
                return Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            }

            long elapsedMs = Environment.TickCount64 - oldest.EnqueuedAtMs;
            long remainingMs = Math.Max(FrameWaitTimeoutMs - elapsedMs, 0);
            return Task.Delay(TimeSpan.FromMilliseconds(remainingMs), cancellationToken);
        }

        private void FlushExpiredWaitingFrames()
        {
            while (_waitingFrames.TryPeek(out var waiting) &&
                   Environment.TickCount64 - waiting.EnqueuedAtMs >= FrameWaitTimeoutMs)
            {
                _waitingFrames.Dequeue();
                List<TelemetryRecord> matched = DrainMatching(waiting.VideoUtcMs);
                EmitSyncedFrame(waiting.Frame, waiting.VideoUtcMs, matched);
            }
        }

        private void ProcessFrame(FrameRecord frameRecord)
        {
            long videoUtcMs = frameRecord.VideoUtcMs;
            List<TelemetryRecord> matched = DrainMatching(videoUtcMs);

            if (matched.Count > 0)
            {
                EmitSyncedFrame(frameRecord, videoUtcMs, matched);
            }
            else
            {
                _waitingFrames.Enqueue(new WaitingFrame(frameRecord, videoUtcMs, Environment.TickCount64));
            }
        }

        public async Task RunAsync(CancellationToken cancellationToken)
        {
            var telemetryReader = _channel.TelemetryChannel.Reader;
            var frameReader = _channel.FrameChannel.Reader;

            while (!cancellationToken.IsCancellationRequested)
            {
                var telemetryReady = telemetryReader.WaitToReadAsync(cancellationToken).AsTask();
                var frameReady = frameReader.WaitToReadAsync(cancellationToken).AsTask();
                var waitingFrameTimeout = CreateWaitingFrameTimeoutTask(cancellationToken);

                var completed = await Task.WhenAny(telemetryReady, frameReady, waitingFrameTimeout);

                if (completed == telemetryReady && telemetryReader.TryRead(out var telemetryRecord))
                {
                    _buffer.Enqueue(telemetryRecord, telemetryRecord.TimeMs);
                    TryUnblockWaitingFrames();
                }
                else if (completed == frameReady && frameReader.TryRead(out var frameRecord))
                {
                    ProcessFrame(frameRecord);
                }
                else if (completed == waitingFrameTimeout)
                {
                    FlushExpiredWaitingFrames();
                }
            }
        }


    }
}
    