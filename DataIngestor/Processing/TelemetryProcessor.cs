using DataIngestor.Channels;
using DataIngestor.Ingestion;

namespace DataIngestor.Processing
{
    public class TelemetryProcessor(ChannelRegistry channelRegistry, TelemetryFilter filter, ILogger<TelemetryProcessor> logger)
    {
        public void Process(string tailNumber, string telemetryJson)
        {
            if (!channelRegistry.IsRegistered(tailNumber))
            {
                logger.LogWarning("Dropping telemetry for unregistered channel: {TailNumber}", tailNumber);
                return;
            }

            logger.LogInformation("{TelemetryJson}", filter.Strip(telemetryJson));
        }
    }
}