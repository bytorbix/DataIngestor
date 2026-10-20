using DataIngestor.Channels;
using DataIngestor.Ingestion;
using DataIngestor.Synchronizing;
using Microsoft.AspNetCore.Mvc;

namespace DataIngestor.Controllers
{
    [ApiController]
    [Route("channels")]
    public class ChannelsController(ChannelRegistry channelRegistry, HlsListener hlsListener, SynchronizerManager synchronizerManager, ILogger<ChannelsController> logger) : ControllerBase
    {
        [HttpPost("{tailNumber}")]
        public IActionResult Register(string tailNumber)
        {
            try
            {
                channelRegistry.Register(tailNumber);
                hlsListener.Start(tailNumber);
                synchronizerManager.Start(tailNumber);
                return Ok();
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to register channel {TailNumber}", tailNumber);
                hlsListener.Stop(tailNumber);
                synchronizerManager.Stop(tailNumber);
                channelRegistry.Unregister(tailNumber);
                return Problem($"Failed to register channel {tailNumber}");
            }
        }

        [HttpDelete("{tailNumber}")]
        public async Task<IActionResult> Unregister(string tailNumber)
        {
            try
            {
                // video stops first (after its current segment); the channel stays registered so telemetry keeps
                // flowing until the synchronizer has drained; the whole drain is bounded by about one segment
                using CancellationTokenSource drainTimeout = new(hlsListener.GetDrainTimeout(tailNumber));
                await hlsListener.StopAsync(tailNumber, drainTimeout.Token);
                await synchronizerManager.StopAsync(tailNumber, drainTimeout.Token);
                channelRegistry.Unregister(tailNumber);
                return Ok();
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to unregister channel {TailNumber}", tailNumber);
                return Problem($"Failed to unregister channel {tailNumber}");
            }
        }
    }
}
