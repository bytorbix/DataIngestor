using System.Text.Json.Nodes;
using DataIngestor.Synchronizing;
using Microsoft.Extensions.Time.Testing;

public class FlightOutputTests : IDisposable
{
    // 2019-06-14T15:30:12.340Z, a Cheyenne-style archive stamp
    private const long FirstStampMs = 1560526212340;
    private static readonly DateTimeOffset IngestedAt = new(2026, 10, 17, 9, 15, 0, TimeSpan.Zero);

    private readonly string _outputDirectory = Path.Combine(Path.GetTempPath(), "FlightOutputTests", Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(_outputDirectory))
            Directory.Delete(_outputDirectory, recursive: true);
    }

    private FlightOutput NewOutput() => new(_outputDirectory, "1234", new FakeTimeProvider(IngestedAt));

    private string FlightDirectory(string flightId) => Path.Combine(_outputDirectory, "1234", flightId);

    private static JsonObject Manifest(string flightDirectory)
        => JsonNode.Parse(File.ReadAllText(Path.Combine(flightDirectory, "flight.json")))!.AsObject();

    [Fact]
    public void FlightId_IsTailPlusTheFirstStampToTheSecond()
    {
        Assert.Equal("1234-20190614T153012Z", FlightOutput.FormatFlightId("1234", FirstStampMs));
    }

    [Fact]
    public void NothingIsWritten_UntilTheFirstRecord()
    {
        using FlightOutput output = NewOutput();

        Assert.Null(output.FlightId);
        Assert.False(Directory.Exists(Path.Combine(_outputDirectory, "1234")));
    }

    [Fact]
    public void FirstRecord_OpensTheFlightFromTheDataStamp_NotTheIngestClock()
    {
        using FlightOutput output = NewOutput();
        output.Write(FirstStampMs, new JsonObject());

        Assert.Equal("1234-20190614T153012Z", output.FlightId);
        JsonObject manifest = Manifest(FlightDirectory(output.FlightId!));
        Assert.Equal("open", (string?)manifest["status"]);
        Assert.Equal("2019-06-14T15:30:12.3400000Z", (string?)manifest["recordedStartUtc"]);
        Assert.Equal("2026-10-17T09:15:00.0000000Z", (string?)manifest["ingestedAtUtc"]);
        Assert.Null(manifest["recordedEndUtc"]);
    }

    [Fact]
    public void EveryRecord_CarriesTheFlightId()
    {
        FlightOutput output = NewOutput();
        output.Write(FirstStampMs, new JsonObject { ["videoUtcMs"] = FirstStampMs });
        output.Write(FirstStampMs + 33, new JsonObject { ["videoUtcMs"] = FirstStampMs + 33 });
        output.Dispose(); // the open writer holds the file

        string[] lines = File.ReadAllLines(Path.Combine(FlightDirectory(output.FlightId!), "frames.jsonl"));
        Assert.Equal(2, lines.Length);
        Assert.All(lines, line => Assert.Equal("1234-20190614T153012Z", (string?)JsonNode.Parse(line)!["flightId"]));
    }

    [Fact]
    public void Dispose_ClosesTheFlightWithTheLastStamp()
    {
        FlightOutput output = NewOutput();
        output.Write(FirstStampMs, new JsonObject());
        output.Write(FirstStampMs + 60_000, new JsonObject());
        output.Dispose();

        JsonObject manifest = Manifest(FlightDirectory("1234-20190614T153012Z"));
        Assert.Equal("closed", (string?)manifest["status"]);
        Assert.Equal("2019-06-14T15:31:12.3400000Z", (string?)manifest["recordedEndUtc"]);
    }

    [Fact]
    public void ReplayingTheSameClip_ReplacesTheEarlierFlight()
    {
        using (FlightOutput first = NewOutput())
        {
            first.Write(FirstStampMs, new JsonObject());
            first.Write(FirstStampMs + 33, new JsonObject());
        }

        using (FlightOutput replay = NewOutput())
            replay.Write(FirstStampMs, new JsonObject());

        string[] lines = File.ReadAllLines(Path.Combine(FlightDirectory("1234-20190614T153012Z"), "frames.jsonl"));
        Assert.Single(lines);
    }
}
