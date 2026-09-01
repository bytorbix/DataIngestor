using DataIngestor.Channels;
using System.Collections.Concurrent;
using System.Diagnostics;

namespace DataIngestor.Ingestion
{
    public class RtspListener(IConfiguration configuration, ILogger<RtspListener> logger, ChannelRegistry channelRegistry)
    {
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
                FileName = "ffprobe",
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            };

            string[] args = { "-v", "quiet", "-select_streams", "v:0", "-show_entries", "frame=pts_time", "-of", "default=noprint_wrappers=1:nokey=1", "-rtsp_transport", "tcp", "-i", rtspUrl }; 
            foreach (var arg in args)
            {
                startInfo.ArgumentList.Add(arg);
            }

            Process process = new() { StartInfo = startInfo };
            process.Start();

            if (!_processes.TryAdd(tailNumber, process))
            {
                _logger.LogWarning("RTSP listener already running for {TailNumber}", tailNumber);
                process.Kill();
                return;
            } 

            _logger.LogInformation("Started RTSP listener for {TailNumber}", tailNumber);
            Task.Run(() => ReadOutput(tailNumber, process));
        }

        private void ReadOutput(string tailNumber, Process process)
        {
            var channel = channelRegistry.Get(tailNumber);

            string? line;
            while ((line = process.StandardOutput.ReadLine()) != null)
            {
                if (double.TryParse(line, out double ptsTime))
                {
                    channel?.FrameChannel.Writer.TryWrite(new FrameRecord(ptsTime, line));
                }
                else if (!string.IsNullOrWhiteSpace(line))
                {
                    _logger.LogWarning("Unparseable ffprobe output for {TailNumber}: {Line}", tailNumber, line);
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
