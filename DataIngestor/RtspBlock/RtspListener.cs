
namespace DataIngestor.RtspBlock
{
    public class RtspListener : BackgroundService
    {
        private readonly string _rtspUrl;
        private string _rtspPort;
        private readonly ILogger<RtspListener> _logger;

        public RtspListener(IConfiguration configuration, ILogger<RtspListener> logger)
        {
            _logger = logger;
            _rtspUrl = configuration["Rtsp:Url"] ?? throw new InvalidOperationException("Rtsp:Url is not configured"); 
            _rtspPort = configuration["Rtsp:Port"] ?? throw new InvalidOperationException("Rtsp:Port is not configured");
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            
        }
    }
}
