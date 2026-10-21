using DataIngestor.Channels;
using DataIngestor.Synchronizing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;

public class SynchronizerTests : IDisposable
{
    private const string Tail = "1234";
    private const long FirstStampMs = 1560526212340;

    private readonly string _outputDirectory = Path.Combine(Path.GetTempPath(), "SynchronizerTests", Guid.NewGuid().ToString("N"));
    private readonly ChannelRegistry _registry = new(NullLogger<ChannelRegistry>.Instance);
    private readonly Synchronizer _synchronizer;
    private readonly Channel _channel;

    public SynchronizerTests()
    {
        _registry.Register(Tail);
        _channel = _registry.Get(Tail)!;
        IConfiguration configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?> { ["Output:Directory"] = _outputDirectory })
            .Build();
        _synchronizer = new Synchronizer(_registry, Tail, NullLogger<Synchronizer>.Instance, configuration);
    }

    public void Dispose()
    {
        if (Directory.Exists(_outputDirectory))
            Directory.Delete(_outputDirectory, recursive: true);
    }

    private string ChunksDirectory() => Path.Combine(_outputDirectory, Tail, FlightOutput.FormatFlightId(Tail, FirstStampMs), "chunks");

    private void WriteTelemetry(long timeMs) => _channel.TelemetryChannel.Writer.TryWrite(new TelemetryRecord(timeMs, $"{{\"time\":{timeMs}}}"));

    private void WriteFrame(long videoUtcMs, int segmentNumber) => _channel.FrameChannel.Writer.TryWrite(new FrameRecord(0, videoUtcMs, segmentNumber, "segment.ts", 2.0));

    [Fact]
    public async Task CompletedFrameChannel_DrainsEveryFrame_AndClosesTheFlight()
    {
        WriteTelemetry(FirstStampMs - 100);
        WriteFrame(FirstStampMs, 1);
        WriteFrame(FirstStampMs + 33, 1);
        WriteFrame(FirstStampMs + 2000, 2);
        WriteTelemetry(FirstStampMs + 2100);
        _channel.FrameChannel.Writer.Complete();

        // finishes by itself: no cancellation, the timeout only guards the test
        await _synchronizer.RunAsync(CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(new[] { "000001.jsonl", "000002.jsonl" }, Directory.GetFiles(ChunksDirectory()).Select(Path.GetFileName).Order());
        Assert.Equal(2, File.ReadAllLines(Path.Combine(ChunksDirectory(), "000001.jsonl")).Length);
        Assert.Single(File.ReadAllLines(Path.Combine(ChunksDirectory(), "000002.jsonl")));
    }

    [Fact]
    public async Task LastFrameWithoutAnAfterRow_IsStillWritten_OnceItsWaitRunsOut()
    {
        WriteTelemetry(FirstStampMs - 100);
        WriteFrame(FirstStampMs, 1);
        _channel.FrameChannel.Writer.Complete();

        await _synchronizer.RunAsync(CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Single(File.ReadAllLines(Path.Combine(ChunksDirectory(), "000001.jsonl")));
    }

    [Fact]
    public async Task HardCancel_StillClosesTheFlight()
    {
        WriteTelemetry(FirstStampMs - 100);
        WriteFrame(FirstStampMs, 1);
        WriteTelemetry(FirstStampMs + 100);
        using CancellationTokenSource cancel = new();

        Task run = _synchronizer.RunAsync(cancel.Token);
        await Task.Delay(200);
        cancel.Cancel();
        await run.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(new[] { "000001.jsonl" }, Directory.GetFiles(ChunksDirectory()).Select(Path.GetFileName));
    }
}
