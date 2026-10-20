namespace DataIngestor.Channels
{
    // SegmentNumber counts the flight's segments from 1 in processing order; it names the segment's archive files
    public record FrameRecord(double PtsTime, long VideoUtcMs, int SegmentNumber, string Payload);
}
