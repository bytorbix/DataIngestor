using System.Collections.Concurrent;

namespace DataIngestor.UavBlock
{
    public class UavRouter
    {
        private readonly ConcurrentDictionary<string, byte> _knownTailNumbers = new(); // byte for minimal value field (we don't need the value so we optimize)
        private readonly ILogger<UavRouter> _logger;

        public UavRouter(ILogger<UavRouter> logger)
        {
            _logger = logger;
        }

        public void Route(string tailNumber, string telemetryJson)
        {
            if (_knownTailNumbers.TryAdd(tailNumber, 0))
            {
                _logger.LogInformation("New UAV detected: {TailNumber}", tailNumber);
            }
            _logger.LogInformation("{TelemetryJson}", telemetryJson);
        }
    }
}
