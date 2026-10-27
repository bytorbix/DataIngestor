using DataIngestor.Channels;
using DataIngestor.Ingestion;
using DataIngestor.Processing;
using DataIngestor.Synchronizing;
using Microsoft.Extensions.Logging.Abstractions;
using System.Text.Json.Nodes;

// TelemetryDevice output for one ICD (MISB ST 0601) packet, as published to Kafka
public class IcdTelemetryTests
{
    private const string Packet = """
        {"sync_1":176,"sync_2":59,"sync_3":79,"Tail number":43,"correlator":0,"zero space":0,
         "PrecisionTimeStamp":1348087245993999,"PlatformHeadingAngle":198.5,"PlatformPitchAngle":4.4,"PlatformRollAngle":-9.0,
         "SensorLatitude":41.14287755,"SensorLongitude":-104.806088603,"SensorTrueAltitude":2936.7,"SensorEllipsoidHeight":2920.9,
         "SensorHorizontalFieldOfView":18.66,"SensorVerticalFieldOfView":10.49,"SensorRelativeAzimuthAngle":268.0,
         "SensorRelativeElevationAngle":-21.7,"SensorRelativeRollAngle":0.0,"Timestamp":"2026-10-27T10:00:00"}
        """;

    [Fact]
    public void Filter_StripsFramingAndTheCaptureTimestamp()
    {
        JsonObject stripped = JsonNode.Parse(new TelemetryFilter().Strip(Packet))!.AsObject();

        foreach (string framing in new[] { "sync_1", "sync_2", "sync_3", "correlator", "zero space", "Timestamp" })
            Assert.False(stripped.ContainsKey(framing), framing);
        Assert.True(stripped.ContainsKey("SensorLatitude"));
        Assert.True(stripped.ContainsKey("Tail number"));
    }

    [Fact]
    public void Processor_ReadsPrecisionTimeStampMicrosecondsAsMilliseconds()
    {
        ChannelRegistry registry = new(NullLogger<ChannelRegistry>.Instance);
        registry.Register("43");
        TelemetryProcessor processor = new(registry, new TelemetryFilter(), NullLogger<TelemetryProcessor>.Instance);

        processor.Process("43", Packet);

        Assert.True(registry.Get("43")!.TelemetryChannel.Reader.TryRead(out TelemetryRecord? record));
        Assert.Equal(1348087245994, record!.TimeMs);
    }

    [Fact]
    public void Interpolator_BlendsHeadingAcrossNorth_AndKeepsTheStampWhole()
    {
        // parsed from JSON, the way the synchronizer gets rows
        JsonObject before = JsonNode.Parse("""{"PrecisionTimeStamp":1000000,"PlatformHeadingAngle":359.0,"SensorLatitude":41.0}""")!.AsObject();
        JsonObject after = JsonNode.Parse("""{"PrecisionTimeStamp":1200002,"PlatformHeadingAngle":1.0,"SensorLatitude":41.2}""")!.AsObject();

        JsonObject mid = TelemetryInterpolator.Interpolate(before, after, 0.5);

        Assert.Equal(0.0, mid["PlatformHeadingAngle"]!.GetValue<double>(), 6);
        Assert.Equal(41.1, mid["SensorLatitude"]!.GetValue<double>(), 6);
        Assert.Equal(1_100_001L, mid["PrecisionTimeStamp"]!.GetValue<long>());
    }
}
