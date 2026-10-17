using DataIngestor.Channels;
using DataIngestor.Synchronizing;
using Microsoft.Extensions.Configuration;
using System.Collections.Concurrent;

public class SynchronizerManager(ChannelRegistry channel, ILogger<SynchronizerManager> logger, ILogger<Synchronizer> synchronizerLogger, IConfiguration configuration)
{
    private readonly ConcurrentDictionary<string, CancellationTokenSource> _tokens = new();

    public void Start(string tailNumber)
    {
        CancellationTokenSource cts = new();
        if (!_tokens.TryAdd(tailNumber, cts))
        {
            logger.LogWarning("Synchronizer already running for {TailNumber}", tailNumber);
            cts.Dispose();
            return;
        }

        Synchronizer sync = new(channel, tailNumber, synchronizerLogger, configuration);
        _ = Task.Run(async () =>
        {
            try { await sync.RunAsync(cts.Token); }
            catch (OperationCanceledException) { }
            catch (Exception ex) { logger.LogError(ex, "Synchronizer for {TailNumber} faulted.", tailNumber); }
            // only remove our own entry: a quick unregister -> register may already have added the next flight's
            finally { _tokens.TryRemove(new KeyValuePair<string, CancellationTokenSource>(tailNumber, cts)); }
        });
    }

    public void Stop(string tailNumber)
    {
        if (_tokens.TryRemove(tailNumber, out CancellationTokenSource? cts))
        {
            cts.Cancel();
            cts.Dispose();
            logger.LogInformation("Stopped synchronizer for {TailNumber}", tailNumber);
        }
        else
        {
            logger.LogWarning("Attempted to stop unknown synchronizer for {TailNumber}", tailNumber);
        }
    }
}