using System.Collections.Concurrent;
using System.Diagnostics;

namespace DataIngestor.Ingestion
{
    public class RtspListener(IConfiguration configuration, ILogger<RtspListener> logger)
    {
        private const string FrameInfoMarker = "Parsed_showinfo";
        private const string RTSP_HOST_FIELD = "Rtsp:Host";
        private const string RTSP_PORT_FIELD = "Rtsp:Port";
        private readonly string _rtspHost = configuration[RTSP_HOST_FIELD] ?? throw new InvalidOperationException("Rtsp:Host is not configured");
        private readonly string _rtspPort = configuration[RTSP_PORT_FIELD] ?? throw new InvalidOperationException("Rtsp:Port is not configured");
        private readonly ConcurrentDictionary<string, Process> _processes = new();
        private readonly ILogger<RtspListener> _logger = logger;


        public void Start(string tailNumber)
        {
            string rtspUrl = $"rtsp://{_rtspHost}:{_rtspPort}/{tailNumber}";

            ProcessStartInfo startInfo = new ProcessStartInfo()
            {
                FileName = "ffmpeg",
                Arguments = $"-rtsp_transport tcp -i {rtspUrl} -vf showinfo -f null -",
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            };

            Process process = new() { StartInfo = startInfo };
            process.Start();

            if (!_processes.TryAdd(tailNumber, process))
            {
                _logger.LogWarning("");
            } else
            {
                _logger.LogInformation("");
            }

            Task.Run(() => ReadOutput(tailNumber, process));
        }

        public void ReadOutput(string tailNumber, Process process)
        {
            string? line;
            while ((line = process.StandardError.ReadLine()) != null)
            {
                if (line.Contains(FrameInfoMarker))
                {
                    _logger.LogInformation("Frame received for {TailNumber}: {Line}", tailNumber, line);
                }
            }

            _processes.TryRemove(tailNumber, out _);
            _logger.LogInformation("RTSP process for {TailNumber} exited.", tailNumber);
        }

        public void Stop(string tailNumber)
        {
            if (_processes.TryRemove(tailNumber, out Process? process))
            {
                try { process.Kill(); } 
                catch (InvalidOperationException) { }

                _logger.LogInformation("Stopped RTSP listener for {TailNumber}", tailNumber);
            }
            else
            {
                _logger.LogWarning("Attempted to stop unknown RTSP listener for {TailNumber}", tailNumber);
            }
        }

    }
}
