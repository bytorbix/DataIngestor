using System.Text.Json.Nodes;
using DataIngestor.Channels;
using DataIngestor.Storage;
using DataIngestor.Synchronizing;
using Microsoft.Extensions.Logging.Abstractions;

public class FlightUploaderTests : IDisposable
{
    // 2019-06-14T15:30:12.340Z
    private const long FirstStampMs = 1560526212340;
    private const string FlightId = "1234-20190614T153012Z";
    private const string Prefix = "1234/" + FlightId + "/";

    private readonly string _outputDirectory = Path.Combine(Path.GetTempPath(), "FlightUploaderTests", Guid.NewGuid().ToString("N"));
    private readonly InMemoryFlightStore _store = new();
    private readonly FlightUploader _uploader;

    public FlightUploaderTests()
    {
        _uploader = new FlightUploader(_outputDirectory, _store, NullLogger<FlightUploader>.Instance);
    }

    public void Dispose()
    {
        if (Directory.Exists(_outputDirectory))
            Directory.Delete(_outputDirectory, recursive: true);
    }

    private string FlightDirectory => Path.Combine(_outputDirectory, "1234", FlightId);

    private static FrameRecord Frame(int segmentNumber) => new(0, 0, segmentNumber, "missing.ts", 2.0);

    // a flight with two finished chunks; the second segment's chunk stays open (.tmp) unless the flight is closed
    private FlightOutput WriteFlight(bool close)
    {
        FlightOutput output = new(_outputDirectory, "1234");
        output.Write(FirstStampMs, Frame(1), new JsonObject());
        output.Write(FirstStampMs + 2000, Frame(2), new JsonObject());
        if (close)
            output.Dispose();
        return output;
    }

    [Fact]
    public async Task FinishedFiles_GoToTheKeyOfTheirRelativePath_AndAreDeletedLocally()
    {
        WriteFlight(close: true);

        Assert.True(await _uploader.UploadPendingAsync(CancellationToken.None));

        Assert.Equal(new[] { Prefix + "chunks/000001.jsonl", Prefix + "chunks/000002.jsonl", Prefix + "flight.json", Prefix + "playlist.m3u8" }, _store.Objects.Keys.Order());
        Assert.Equal("application/x-ndjson", _store.Objects[Prefix + "chunks/000001.jsonl"].ContentType);
        Assert.Equal("application/vnd.apple.mpegurl", _store.Objects[Prefix + "playlist.m3u8"].ContentType);
        Assert.False(Directory.Exists(FlightDirectory));
    }

    [Fact]
    public async Task OpenFlight_UploadsFinishedChunks_ButNotTheTmpChunkOrTheOpenManifest()
    {
        using FlightOutput output = WriteFlight(close: false);

        await _uploader.UploadPendingAsync(CancellationToken.None);

        Assert.Equal(new[] { Prefix + "chunks/000001.jsonl" }, _store.Objects.Keys);
        Assert.True(File.Exists(Path.Combine(FlightDirectory, "chunks", "000002.jsonl.tmp")));
        Assert.True(File.Exists(Path.Combine(FlightDirectory, "flight.json")));
    }

    [Fact]
    public async Task ClosedManifest_GoesUpLast()
    {
        WriteFlight(close: true);

        await _uploader.UploadPendingAsync(CancellationToken.None);

        Assert.Equal($"put {Prefix}flight.json", _store.Operations[^1]);
        Assert.Equal("closed", (string?)JsonNode.Parse(_store.Text(Prefix + "flight.json"))!["status"]);
    }

    [Fact]
    public async Task FailedUpload_StopsTheScan_AndTheClosedManifestWaitsForIt()
    {
        WriteFlight(close: true);
        _store.FailingKeys.Add(Prefix + "chunks/000002.jsonl");

        Assert.False(await _uploader.UploadPendingAsync(CancellationToken.None));
        Assert.DoesNotContain(Prefix + "flight.json", _store.Objects.Keys);
        Assert.True(File.Exists(Path.Combine(FlightDirectory, "chunks", "000002.jsonl")));

        _store.FailingKeys.Clear();
        Assert.True(await _uploader.UploadPendingAsync(CancellationToken.None));
        Assert.Contains(Prefix + "chunks/000002.jsonl", _store.Objects.Keys);
        Assert.Equal($"put {Prefix}flight.json", _store.Operations[^1]);
    }

    [Fact]
    public async Task ResetMarker_ClearsTheOldRunFromTheArchive_BeforeAnythingOfTheNewRun()
    {
        await _store.PutAsync(Prefix + "chunks/000099.jsonl", new MemoryStream(), "application/x-ndjson", CancellationToken.None);
        await _store.PutAsync("1234/" + FlightId + "0/kept.json", new MemoryStream(), "application/json", CancellationToken.None);
        _store.Operations.Clear();
        WriteFlight(close: true);

        await _uploader.UploadPendingAsync(CancellationToken.None);

        Assert.Equal($"delete {Prefix}", _store.Operations[0]);
        Assert.DoesNotContain(Prefix + "chunks/000099.jsonl", _store.Objects.Keys);
        Assert.Contains("1234/" + FlightId + "0/kept.json", _store.Objects.Keys);
        Assert.DoesNotContain(Prefix + ".reset", _store.Objects.Keys);
    }

    [Fact]
    public async Task ResetMarker_IsHandledOnce()
    {
        using FlightOutput output = WriteFlight(close: false);

        await _uploader.UploadPendingAsync(CancellationToken.None);
        await _uploader.UploadPendingAsync(CancellationToken.None);

        Assert.Single(_store.Operations, operation => operation.StartsWith("delete"));
    }

    [Fact]
    public async Task StagingFolder_IsSkipped()
    {
        string staging = Path.Combine(_outputDirectory, "1234", ".incoming");
        Directory.CreateDirectory(staging);
        File.WriteAllText(Path.Combine(staging, "000001.ts"), "staged");

        Assert.True(await _uploader.UploadPendingAsync(CancellationToken.None));

        Assert.Empty(_store.Objects);
        Assert.True(File.Exists(Path.Combine(staging, "000001.ts")));
    }

    [Fact]
    public async Task MissingOutputDirectory_IsNotAFailure()
    {
        Assert.True(await _uploader.UploadPendingAsync(CancellationToken.None));
    }
}
