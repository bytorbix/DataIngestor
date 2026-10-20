using DataIngestor.Channels;
using DataIngestor.Synchronizing;
using Microsoft.Extensions.Configuration;
using System.Collections.Concurrent;

public class SynchronizerManager(ChannelRegistry channel, ILogger<SynchronizerManager> logger, ILogger<Synchronizer> synchronizerLogger, IConfiguration configuration)
{
    // token sources aren't disposed: StopAsync may still cancel after the run has finished
    private sealed class SynchronizerRun
    {
        public CancellationTokenSource Cancel { get; } = new();
        public Task Task { get; set; } = Task.CompletedTask;
    }

    private readonly ConcurrentDictionary<string, SynchronizerRun> _runs = new();

    public void Start(string tailNumber)
    {
        SynchronizerRun run = new();
        if (!_runs.TryAdd(tailNumber, run))
        {
            logger.LogWarning("Synchronizer already running for {TailNumber}", tailNumber);
            return;
        }

        Synchronizer sync = new(channel, tailNumber, synchronizerLogger, configuration);
        run.Task = Task.Run(async () =>
        {
            try { await sync.RunAsync(run.Cancel.Token); }
            catch (OperationCanceledException) { }
            catch (Exception ex) { logger.LogError(ex, "Synchronizer for {TailNumber} faulted.", tailNumber); }
            // only remove our own entry: a quick unregister -> register may already have added the next flight's
            finally { _runs.TryRemove(new KeyValuePair<string, SynchronizerRun>(tailNumber, run)); }
        });
    }

    // graceful: the synchronizer finishes by itself once the frame channel is complete; cancelled once the timeout passes
    public async Task StopAsync(string tailNumber, CancellationToken timeout)
    {
        if (!_runs.TryRemove(tailNumber, out SynchronizerRun? run))
        {
            logger.LogWarning("Attempted to stop unknown synchronizer for {TailNumber}", tailNumber);
            return;
        }

        try
        {
            await run.Task.WaitAsync(timeout);
            logger.LogInformation("Stopped synchronizer for {TailNumber}", tailNumber);
        }
        catch (OperationCanceledException)
        {
            logger.LogWarning("Synchronizer for {TailNumber} didn't drain in time, cancelling it", tailNumber);
            run.Cancel.Cancel();
            await run.Task;
        }
    }

    // hard stop, used when registration fails
    public void Stop(string tailNumber)
    {
        if (_runs.TryRemove(tailNumber, out SynchronizerRun? run))
        {
            run.Cancel.Cancel();
            logger.LogInformation("Stopped synchronizer for {TailNumber}", tailNumber);
        }
        else
        {
            logger.LogWarning("Attempted to stop unknown synchronizer for {TailNumber}", tailNumber);
        }
    }
}
