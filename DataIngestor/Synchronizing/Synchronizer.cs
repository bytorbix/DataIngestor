using DataIngestor.Channels;

namespace DataIngestor.Synchronizing
{
    public class Synchronizer(ChannelRegistry channelRegistry, string tailNumber)
    {
        private readonly PriorityQueue<TelemetryRecord, long> _buffer = new();
        private readonly Channel _channel = channelRegistry.Get(tailNumber)
            ?? throw new InvalidOperationException($"Channel not found for {tailNumber}");
        private long? _t0;

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
                    _t0 ??= telemetryRecord.TimeMs;
                    long elapsedMs = telemetryRecord.TimeMs - _t0.Value;
                    _buffer.Enqueue(telemetryRecord, elapsedMs);
                }
                else if (completed == frameReady && frameReader.TryRead(out var frameRecord))
                {
                    double videoElapsedMs = frameRecord.PtsTime * 1000;
                    
                    while (_buffer.TryPeek(out var nextTelemetry, out long priority) && priority <= videoElapsedMs)
                    {
                        _buffer.Dequeue();
                        Console.WriteLine($"[{tailNumber}] video={videoElapsedMs}ms telemetry={priority}ms payload={nextTelemetry.Payload}");
                    }
                }
            }
        }
    }
}
    