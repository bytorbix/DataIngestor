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
        public IActionResult Unregister(string tailNumber)
        {
            try
            {
                channelRegistry.Unregister(tailNumber);
                hlsListener.Stop(tailNumber);
                synchronizerManager.Stop(tailNumber);
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
