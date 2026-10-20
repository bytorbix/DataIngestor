using DataIngestor.Channels;
using Microsoft.Extensions.Logging;
using System.Text.Json.Nodes;
using System.Threading.Channels;
using Channel = DataIngestor.Channels.Channel;

namespace DataIngestor.Synchronizing
{
    public class Synchronizer
    {
        // how long a frame waits for the telemetry row after it before falling back to the row before
        private const int FrameWaitTimeoutMs = 500;
        // rows further apart than this are treated as a gap in telemetry, not blended across
        private const long MaxInterpolationGapMs = 1000;
        private const string OUTPUT_DIRECTORY_FIELD = "Output:Directory";

        private record WaitingFrame(FrameRecord Frame, long VideoUtcMs, long EnqueuedAtMs);
        private record BufferedTelemetry(long TimeMs, JsonObject Data);

        private readonly string _tailNumber;
        private readonly ILogger<Synchronizer> _logger;
        private readonly Channel _channel;
        private readonly FlightOutput _flightOutput;

        public Synchronizer(ChannelRegistry channelRegistry, string tailNumber, ILogger<Synchronizer> logger, IConfiguration configuration)
        {
            _tailNumber = tailNumber;
            _logger = logger;

            _channel = channelRegistry.Get(tailNumber) ?? throw new InvalidOperationException($"Channel not found for {tailNumber}");
            string outputDirectory = configuration[OUTPUT_DIRECTORY_FIELD] ?? throw new InvalidOperationException("Output:Directory is not configured");
            _flightOutput = new FlightOutput(outputDirectory, tailNumber);
        }

        // telemetry sorted by time, frames in arrival order (emitted strictly in that order)
        private readonly List<BufferedTelemetry> _telemetry = new();
        private readonly Queue<WaitingFrame> _waitingFrames = new();
        private int _writtenFrames;

        private void AddTelemetry(TelemetryRecord record)
        {
            JsonObject? data = JsonNode.Parse(record.Payload)?.AsObject();
            if (data is null)
                return;

            // rows almost always arrive in order, so this is usually an append
            int index = _telemetry.Count;
            while (index > 0 && _telemetry[index - 1].TimeMs > record.TimeMs)
                index--;

            _telemetry.Insert(index, new BufferedTelemetry(record.TimeMs, data));
        }

        // index of the first row strictly after timeMs
        private int FindFirstAfter(long timeMs)
        {
            int low = 0, high = _telemetry.Count;
            while (low < high)
            {
                int mid = (low + high) / 2;
                if (_telemetry[mid].TimeMs <= timeMs) low = mid + 1;
                else high = mid;
            }
            return low;
        }

        // null means: keep waiting for the row after this frame
        private SyncedFrame? TrySync(WaitingFrame waiting, bool timedOut)
        {
            int afterIndex = FindFirstAfter(waiting.VideoUtcMs);
            BufferedTelemetry? before = afterIndex > 0 ? _telemetry[afterIndex - 1] : null;
            BufferedTelemetry? after = afterIndex < _telemetry.Count ? _telemetry[afterIndex] : null;

            if (before is not null && before.TimeMs == waiting.VideoUtcMs)
                return Synced(waiting, before.Data.DeepClone().AsObject(), SyncMethod.Exact, before, null);

            if (before is not null && after is not null)
            {
                if (after.TimeMs - before.TimeMs > MaxInterpolationGapMs)
                {
                    BufferedTelemetry nearest = waiting.VideoUtcMs - before.TimeMs <= after.TimeMs - waiting.VideoUtcMs ? before : after;
                    return Synced(waiting, nearest.Data.DeepClone().AsObject(), SyncMethod.Nearest, before, after);
                }

                double fraction = (double)(waiting.VideoUtcMs - before.TimeMs) / (after.TimeMs - before.TimeMs);
                return Synced(waiting, TelemetryInterpolator.Interpolate(before.Data, after.Data, fraction), SyncMethod.Interpolated, before, after);
            }

            // frame is older than all buffered telemetry: nothing before it will ever arrive in order
            if (before is null && after is not null)
                return Synced(waiting, null, SyncMethod.None, null, after);

            if (!timedOut)
                return null;

            return before is not null
                ? Synced(waiting, before.Data.DeepClone().AsObject(), SyncMethod.Hold, before, null)
                : Synced(waiting, null, SyncMethod.None, null, null);
        }

        private static SyncedFrame Synced(WaitingFrame waiting, JsonObject? telemetry, SyncMethod method, BufferedTelemetry? before, BufferedTelemetry? after)
            => new(waiting.Frame, waiting.VideoUtcMs, telemetry, method, before?.TimeMs, after?.TimeMs);

        private void EmitReadyFrames()
        {
            while (_waitingFrames.TryPeek(out WaitingFrame? waiting))
            {
                bool timedOut = Environment.TickCount64 - waiting.EnqueuedAtMs >= FrameWaitTimeoutMs;
                SyncedFrame? synced = TrySync(waiting, timedOut);
                if (synced is null)
                    break;

                _waitingFrames.Dequeue();
                EmitSyncedFrame(synced);
                PruneTelemetry(waiting.VideoUtcMs);
            }
        }

        // later frames can still need the last row at or before this one, so keep that one
        private void PruneTelemetry(long emittedVideoUtcMs)
        {
            int removeCount = FindFirstAfter(emittedVideoUtcMs) - 1;
            if (removeCount > 0)
                _telemetry.RemoveRange(0, removeCount);
        }

        private void EmitSyncedFrame(SyncedFrame synced)
        {
            _logger.LogInformation("[{TailNumber}] video={VideoUtcMs}ms method={Method} before={BeforeTimeMs} after={AfterTimeMs}",
                _tailNumber, synced.VideoUtcMs, synced.Method, synced.BeforeTimeMs, synced.AfterTimeMs);

            WriteSyncedFrame(synced);
        }

        private void WriteSyncedFrame(SyncedFrame synced)
        {
            JsonObject record = new()
            {
                ["videoUtcMs"] = synced.VideoUtcMs,
                ["frame"] = new JsonObject
                {
                    ["ptsTime"] = synced.Frame.PtsTime,
                    ["segmentNumber"] = synced.Frame.SegmentNumber,
                    ["segment"] = FlightOutput.FormatSegmentKey(synced.Frame.SegmentNumber)
                },
                ["telemetry"] = synced.Telemetry,
                ["sync"] = new JsonObject
                {
                    ["method"] = synced.Method.ToString(),
                    ["beforeTimeMs"] = synced.BeforeTimeMs,
                    ["afterTimeMs"] = synced.AfterTimeMs
                }
            };

            _flightOutput.Write(synced.VideoUtcMs, synced.Frame.SegmentNumber, synced.Frame.SegmentPath, record);
            _writtenFrames++;
        }

        private Task CreateWaitingFrameTimeoutTask(CancellationToken cancellationToken)
        {
            if (!_waitingFrames.TryPeek(out WaitingFrame? oldest))
            {
                return Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            }

            long elapsedMs = Environment.TickCount64 - oldest.EnqueuedAtMs;
            long remainingMs = Math.Max(FrameWaitTimeoutMs - elapsedMs, 0);
            return Task.Delay(TimeSpan.FromMilliseconds(remainingMs), cancellationToken);
        }

        // runs until the frame channel is complete and every waiting frame is written; cancellation is only the hard stop
        public async Task RunAsync(CancellationToken cancellationToken)
        {
            ChannelReader<TelemetryRecord> telemetryReader = _channel.TelemetryChannel.Reader;
            ChannelReader<FrameRecord> frameReader = _channel.FrameChannel.Reader;
            bool framesComplete = false;
            string outcome = "faulted";

            try
            {
                // video ends first; telemetry keeps flowing so the last frames still get the row after them
                while (!framesComplete || _waitingFrames.Count > 0)
                {
                    Task<bool> telemetryReady = telemetryReader.WaitToReadAsync(cancellationToken).AsTask();
                    Task<bool>? frameReady = framesComplete ? null : frameReader.WaitToReadAsync(cancellationToken).AsTask();
                    Task waitingFrameTimeout = CreateWaitingFrameTimeoutTask(cancellationToken);

                    Task completed = frameReady is null
                        ? await Task.WhenAny(telemetryReady, waitingFrameTimeout)
                        : await Task.WhenAny(telemetryReady, frameReady, waitingFrameTimeout);
                    cancellationToken.ThrowIfCancellationRequested();

                    if (completed == telemetryReady && telemetryReader.TryRead(out TelemetryRecord? telemetryRecord))
                    {
                        AddTelemetry(telemetryRecord);
                    }
                    else if (completed == frameReady)
                    {
                        if (frameReader.TryRead(out FrameRecord? frameRecord))
                            _waitingFrames.Enqueue(new WaitingFrame(frameRecord, frameRecord.VideoUtcMs, Environment.TickCount64));
                        else if (!frameReady.Result)
                            framesComplete = true; // complete and empty: stop waiting on it, or it returns false forever
                    }

                    EmitReadyFrames();
                }

                outcome = "drained";
            }
            catch (OperationCanceledException)
            {
                outcome = "timed out";
            }
            finally
            {
                int droppedFrames = _waitingFrames.Count + (frameReader.CanCount ? frameReader.Count : 0);
                _flightOutput.Dispose();
                _logger.LogInformation("[{TailNumber}] Flight {FlightId} closed ({Outcome}): {WrittenFrames} frames written, {DroppedFrames} dropped",
                    _tailNumber, _flightOutput.FlightId, outcome, _writtenFrames, droppedFrames);
            }
        }
    }
}
