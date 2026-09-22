using DataIngestor.Channels;
using DataIngestor.Ingestion;
using DataIngestor.Synchronizing;
using Microsoft.AspNetCore.Mvc;

namespace DataIngestor.Controllers
{
    [ApiController]
    [Route("channels")]
    public class ChannelsController(ChannelRegistry channelRegistry, HlsListener hlsListener, SynchronizerManager synchronizerManager) : ControllerBase
    {
        [HttpPost("{tailNumber}")]
        public IActionResult Register(string tailNumber)
        {
            channelRegistry.Register(tailNumber);
            hlsListener.Start(tailNumber);
            synchronizerManager.Start(tailNumber);
            return Ok();
        }

        [HttpDelete("{tailNumber}")]
        public IActionResult Unregister(string tailNumber)
        {
            channelRegistry.Unregister(tailNumber);
            hlsListener.Stop(tailNumber);
            synchronizerManager.Stop(tailNumber);
            return Ok();
        }
    }
}
