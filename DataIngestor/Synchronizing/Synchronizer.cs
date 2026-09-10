using DataIngestor.Channels;

namespace DataIngestor.Synchronizing
{
    public class Synchronizer(ChannelRegistry channelRegistry, string tailNumber, ILogger<Synchronizer> logger)
    {
        private readonly PriorityQueue<TelemetryRecord, long> _buffer = new();
        private readonly Channel _channel = channelRegistry.Get(tailNumber)
            ?? throw new InvalidOperationException($"Channel not found for {tailNumber}");
        private long? _anchorMs;
        private readonly Queue<FrameRecord> _pendingFrames = new();

        private void ProcessFrame(FrameRecord frameRecord)
        {
            long videoUtcMs = _anchorMs!.Value + (long)(frameRecord.PtsTime * 1000);
            List<TelemetryRecord> matched = new();

            while (_buffer.TryPeek(out var nextTelemetry, out long priority) && priority <= videoUtcMs)
            {
                _buffer.Dequeue();
                matched.Add(nextTelemetry);
            }

            SyncedFrame synced = new SyncedFrame(frameRecord, matched);
            logger.LogInformation("[{TailNumber}] video={VideoUtcMs}ms telemetryCount={TelemetryCount}", tailNumber, videoUtcMs, synced.Telemetry.Count);
        }

        public async Task RunAsync(CancellationToken cancellationToken)
        {
            var telemetryReader = _channel.TelemetryChannel.Reader;
            var frameReader = _channel.FrameChannel.Reader;

            while (!cancellationToken.IsCancellationRequested)
            {
                var telemetryReady = telemetryReader.WaitToReadAsync(cancellationToken).AsTask();
                var frameReady = frameReader.WaitToReadAsync(cancellationToken).AsTask();

                var completed = await Task.WhenAny(telemetryReady, frameReady);

                if (completed == telemetryReady && telemetryReader.TryRead(out var telemetryRecord))
                {
                    bool anchorJustSet = _anchorMs is null;
                    _anchorMs ??= telemetryRecord.TimeMs - (long)(telemetryRecord.PtsTime * 1000);
                    _buffer.Enqueue(telemetryRecord, telemetryRecord.TimeMs);

                    if (anchorJustSet)
                    {
                        while (_pendingFrames.TryDequeue(out var pending))
                            ProcessFrame(pending);
                    }
                }

                else if (completed == frameReady && frameReader.TryRead(out var frameRecord))
                {
                    if (_anchorMs is null)
                        _pendingFrames.Enqueue(frameRecord);
                    else
                        ProcessFrame(frameRecord);
                }
            }
        }
    }
}
    