namespace DataIngestor.Channels
{
    public record FrameRecord(double PtsTime, long VideoUtcMs, string Payload);
}
