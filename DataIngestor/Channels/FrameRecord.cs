namespace DataIngestor.Channels
{
    // SegmentNumber counts the flight's segments from 1 in processing order; it names the segment's archive files
    // SegmentPath is the staged .ts; once a frame carrying it is written, the flight output owns the file and moves it into the flight
    public record FrameRecord(double PtsTime, long VideoUtcMs, int SegmentNumber, string SegmentPath);
}
