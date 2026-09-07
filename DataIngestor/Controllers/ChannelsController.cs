using DataIngestor.Channels;
using DataIngestor.Ingestion;
using DataIngestor.Synchronizing;
using Microsoft.AspNetCore.Mvc;

namespace DataIngestor.Controllers
{
    [ApiController]
    [Route("channels")]
    public class ChannelsController(ChannelRegistry channelRegistry, RtspListener rtspListener, SynchronizerManager synchronizerManager) : ControllerBase 
    {
        [HttpPost("{tailNumber}")]
        public IActionResult Register(string tailNumber)
        {
            channelRegistry.Register(tailNumber);
            rtspListener.Start(tailNumber);
            synchronizerManager.Start(tailNumber);
            return Ok();
        }

        [HttpDelete("{tailNumber}")]
        public IActionResult Unregister(string tailNumber) 
        {
            channelRegistry.Unregister(tailNumber);
            rtspListener.Stop(tailNumber);
            synchronizerManager.Stop(tailNumber);
            return Ok();
        }
    }
}
