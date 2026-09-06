using DataIngestor.Channels;

namespace DataIngestor.Synchronizing
{
    public record SyncedFrame(FrameRecord Frame, IReadOnlyList<TelemetryRecord> Telemetry);
}
