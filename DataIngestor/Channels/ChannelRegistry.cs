using System.Collections.Concurrent;

namespace DataIngestor.Channels
{
    public class ChannelRegistry
    {
        private readonly ConcurrentDictionary<string, Channel> _channels = new();
        private readonly ILogger<ChannelRegistry> _logger;

        public ChannelRegistry(ILogger<ChannelRegistry> logger)
        {
            _logger = logger;
        }
        public void Register(string tailNumber) 
        {
            if (_channels.TryAdd(tailNumber, new Channel { TailNumber=tailNumber }))
            {
                _logger.LogInformation("Channel registered: {TailNumber}", tailNumber);
            } else
            {
                _logger.LogWarning("Channel already registered: {TailNumber}", tailNumber);
            }
        }
        public void Unregister(string tailNumber) 
        {
            if (_channels.TryRemove(tailNumber, out _))
            {
                _logger.LogInformation("Channel unregistered: {TailNumber}", tailNumber);
            }
            else
            {
                _logger.LogWarning("Attempted to unregister unknown channel: {TailNumber}", tailNumber);
            }
        }
        public bool IsRegistered(string tailNumber) 
        {
            return _channels.ContainsKey(tailNumber);
        }
    }
}
