using System.Diagnostics;
using System.Globalization;
using System.Text.Json.Nodes;

namespace DataIngestor.Ingestion
{
    // runs ffprobe against a local segment file and returns its raw output, parsed
    public class FfprobeRunner(ILogger<FfprobeRunner> logger)
    {
        public async Task<List<(double PtsTimeSeconds, string HexDump)>> GetId3PacketsAsync(string segmentPath, CancellationToken cancellationToken)
        {
            string[] args = { "-v", "error", "-select_streams", "d:0", "-show_packets", "-show_data", "-of", "json", segmentPath };
            string json = await RunProcessCaptureOutputAsync("ffprobe", args, cancellationToken);

            JsonNode? root = JsonNode.Parse(json);
            JsonArray packets = root?["packets"]?.AsArray() ?? new JsonArray();

            List<(double, string)> result = new();
            foreach (JsonNode? packet in packets)
            {
                string? ptsTimeText = packet?["pts_time"]?.GetValue<string>();
                string? hexDump = packet?["data"]?.GetValue<string>();

                if (ptsTimeText is null || hexDump is null)
                    continue;

                result.Add((double.Parse(ptsTimeText, CultureInfo.InvariantCulture), hexDump));
            }

            return result;
        }

        public async Task<List<double>> GetVideoFramePtsTimesAsync(string segmentPath, CancellationToken cancellationToken)
        {
            string[] args = { "-v", "error", "-select_streams", "v:0", "-show_entries", "frame=pts_time", "-of", "default=noprint_wrappers=1:nokey=1", segmentPath };
            string output = await RunProcessCaptureOutputAsync("ffprobe", args, cancellationToken);

            List<double> ptsTimes = new();
            foreach (string rawLine in output.Split('\n'))
            {
                string line = rawLine.Trim();
                if (line.Length == 0) continue;

                if (double.TryParse(line, NumberStyles.Float, CultureInfo.InvariantCulture, out double ptsTime))
                    ptsTimes.Add(ptsTime);
                else
                    logger.LogWarning("Unparseable ffprobe frame output: {Line}", line);
            }

            return ptsTimes;
        }

        private static async Task<string> RunProcessCaptureOutputAsync(string fileName, IEnumerable<string> args, CancellationToken cancellationToken)
        {
            ProcessStartInfo startInfo = new()
            {
                FileName = fileName,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            };

            foreach (string arg in args)
                startInfo.ArgumentList.Add(arg);

            using Process process = new() { StartInfo = startInfo };
            process.Start();

            Task<string> stdoutTask = process.StandardOutput.ReadToEndAsync(cancellationToken);
            Task<string> stderrTask = process.StandardError.ReadToEndAsync(cancellationToken);

            await process.WaitForExitAsync(cancellationToken);
            string stdout = await stdoutTask;

            if (process.ExitCode != 0)
            {
                string stderr = await stderrTask;
                throw new InvalidOperationException($"{fileName} exited with code {process.ExitCode}: {stderr}");
            }

            return stdout;
        }
    }
}
