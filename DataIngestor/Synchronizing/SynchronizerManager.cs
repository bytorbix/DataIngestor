using DataIngestor.Channels;
using System.Collections.Concurrent;

namespace DataIngestor.Synchronizing
{
    public class SynchronizerManager(ChannelRegistry channel, ILogger<SynchronizerManager> logger, ILogger<Synchronizer> synchronizerLogger)
    {
        private readonly ConcurrentDictionary<string, CancellationTokenSource> _tokens = new();

        public void Start(string tailNumber)
        {
            CancellationTokenSource cts = new CancellationTokenSource();
            if(!_tokens.TryAdd(tailNumber, cts)) {
                logger.LogWarning("Synchronizer already running for {TailNumber}", tailNumber);
                cts.Dispose();
                return;
            }

            Synchronizer sync = new Synchronizer(channel, tailNumber, synchronizerLogger);
            _ = Task.Run(async () =>
            {
                try
                {
                    await sync.RunAsync(cts.Token);
                }
                catch (OperationCanceledException)
                {

                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Synchronizer for {TailNumber} faulted.", tailNumber);
                }
                finally
                {
                    _tokens.TryRemove(tailNumber, out _);
                }
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
}
