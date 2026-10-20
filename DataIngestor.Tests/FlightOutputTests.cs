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

    // a staged path that was never written, for tests that aren't about segments
    private string NoSegment => Path.Combine(_outputDirectory, "missing.ts");

    private string StageSegment(int segmentNumber)
    {
        string directory = Path.Combine(_outputDirectory, "1234", ".incoming");
        Directory.CreateDirectory(directory);
        string path = Path.Combine(directory, $"{segmentNumber:D6}.ts");
        File.WriteAllText(path, $"segment {segmentNumber}");
        return path;
    }

    private FlightOutput NewOutput() => new(_outputDirectory, "1234", new FakeTimeProvider(IngestedAt));

    private string FlightDirectory(string flightId) => Path.Combine(_outputDirectory, "1234", flightId);

    private string ChunkPath(string flightId, string fileName) => Path.Combine(FlightDirectory(flightId), "chunks", fileName);

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
        output.Write(FirstStampMs, 1, NoSegment, new JsonObject());

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
        output.Write(FirstStampMs, 1, NoSegment, new JsonObject { ["videoUtcMs"] = FirstStampMs });
        output.Write(FirstStampMs + 33, 1, NoSegment, new JsonObject { ["videoUtcMs"] = FirstStampMs + 33 });
        output.Dispose(); // the open writer holds the file

        string[] lines = File.ReadAllLines(ChunkPath(output.FlightId!, "000001.jsonl"));
        Assert.Equal(2, lines.Length);
        Assert.All(lines, line => Assert.Equal("1234-20190614T153012Z", (string?)JsonNode.Parse(line)!["flightId"]));
    }

    [Fact]
    public void Dispose_ClosesTheFlightWithTheLastStamp()
    {
        FlightOutput output = NewOutput();
        output.Write(FirstStampMs, 1, NoSegment, new JsonObject());
        output.Write(FirstStampMs + 60_000, 1, NoSegment, new JsonObject());
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
            first.Write(FirstStampMs, 1, NoSegment, new JsonObject());
            first.Write(FirstStampMs + 33, 1, NoSegment, new JsonObject());
        }

        using (FlightOutput replay = NewOutput())
            replay.Write(FirstStampMs, 1, NoSegment, new JsonObject());

        string[] lines = File.ReadAllLines(ChunkPath("1234-20190614T153012Z", "000001.jsonl"));
        Assert.Single(lines);
    }

    [Fact]
    public void OpenChunk_KeepsItsTempName_UntilItIsComplete()
    {
        using FlightOutput output = NewOutput();
        output.Write(FirstStampMs, 1, NoSegment, new JsonObject());

        Assert.True(File.Exists(ChunkPath(output.FlightId!, "000001.jsonl.tmp")));
        Assert.False(File.Exists(ChunkPath(output.FlightId!, "000001.jsonl")));
    }

    [Fact]
    public void NextSegment_RenamesThePreviousChunk_AndStartsANewOne()
    {
        using FlightOutput output = NewOutput();
        output.Write(FirstStampMs, 1, NoSegment, new JsonObject());
        output.Write(FirstStampMs + 33, 1, NoSegment, new JsonObject());
        output.Write(FirstStampMs + 2000, 2, NoSegment, new JsonObject());

        Assert.Equal(2, File.ReadAllLines(ChunkPath(output.FlightId!, "000001.jsonl")).Length);
        Assert.False(File.Exists(ChunkPath(output.FlightId!, "000001.jsonl.tmp")));
        Assert.True(File.Exists(ChunkPath(output.FlightId!, "000002.jsonl.tmp")));
    }

    [Fact]
    public void Dispose_RenamesTheLastChunk()
    {
        FlightOutput output = NewOutput();
        output.Write(FirstStampMs, 1, NoSegment, new JsonObject());
        output.Write(FirstStampMs + 2000, 2, NoSegment, new JsonObject());
        output.Dispose();

        string[] chunks = Directory.GetFiles(Path.Combine(FlightDirectory(output.FlightId!), "chunks")).Select(Path.GetFileName).ToArray()!;
        Assert.Equal(new[] { "000001.jsonl", "000002.jsonl" }, chunks.Order());
    }

    [Fact]
    public void NextSegment_MovesThePreviousSegmentIntoTheFlight()
    {
        using FlightOutput output = NewOutput();
        string first = StageSegment(1);
        string second = StageSegment(2);
        output.Write(FirstStampMs, 1, first, new JsonObject());
        output.Write(FirstStampMs + 2000, 2, second, new JsonObject());

        string segmentPath = Path.Combine(FlightDirectory(output.FlightId!), "segments", "000001.ts");
        Assert.Equal("segment 1", File.ReadAllText(segmentPath));
        Assert.False(File.Exists(first));
        Assert.True(File.Exists(second));
    }

    [Fact]
    public void Dispose_MovesTheLastSegmentIntoTheFlight()
    {
        FlightOutput output = NewOutput();
        string staged = StageSegment(1);
        output.Write(FirstStampMs, 1, staged, new JsonObject());
        output.Dispose();

        Assert.Equal("segment 1", File.ReadAllText(Path.Combine(FlightDirectory(output.FlightId!), "segments", "000001.ts")));
        Assert.False(File.Exists(staged));
    }

    [Fact]
    public void SegmentKey_PointsIntoTheFlightsSegmentsFolder()
    {
        Assert.Equal("segments/000042.ts", FlightOutput.FormatSegmentKey(42));
    }
}
