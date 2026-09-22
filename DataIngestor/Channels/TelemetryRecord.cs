namespace DataIngestor.Channels
{
    public record TelemetryRecord(long TimeMs, double PtsTime, string Payload);
}
