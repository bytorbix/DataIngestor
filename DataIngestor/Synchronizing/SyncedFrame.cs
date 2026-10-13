using DataIngestor.Channels;
using System.Text.Json.Nodes;

namespace DataIngestor.Synchronizing
{
    public enum SyncMethod
    {
        Interpolated, // blended between the rows before and after the frame
        Exact,        // a row has exactly the frame's time
        Hold,         // no row after the frame arrived in time, reused the row before
        Nearest,      // rows too far apart to blend, used the closer one
        None          // no usable telemetry
    }

    public record SyncedFrame(FrameRecord Frame, long VideoUtcMs, JsonObject? Telemetry, SyncMethod Method, long? BeforeTimeMs, long? AfterTimeMs);
}
