using DataIngestor.Synchronizing;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace DataIngestor.Storage
{
    // store-and-forward: finished files wait under the output directory and go to the archive under the same relative path,
    // and a local file is deleted only after its upload is confirmed, so whatever failed is simply still there for the next scan
    public class FlightUploader(string outputDirectory, IFlightStore store, ILogger<FlightUploader> logger)
    {
        private const string TEMP_SUFFIX = ".tmp";

        private static readonly Dictionary<string, string> ContentTypes = new(StringComparer.OrdinalIgnoreCase)
        {
            [".jsonl"] = "application/x-ndjson",
            [".json"] = "application/json",
            [".ts"] = "video/mp2t",
            [".m3u8"] = "application/vnd.apple.mpegurl"
        };

        // one pass over every flight; false means an upload failed and the pass stopped there, so the caller backs off
        public async Task<bool> UploadPendingAsync(CancellationToken cancellationToken)
        {
            if (!Directory.Exists(outputDirectory))
                return true;

            foreach (string tailDirectory in Directory.GetDirectories(outputDirectory).Where(directory => !IsHidden(directory)))
            {
                foreach (string flightDirectory in Directory.GetDirectories(tailDirectory).Where(directory => !IsHidden(directory)))
                {
                    if (!await UploadFlightAsync(flightDirectory, cancellationToken))
                        return false;
                }
            }

            return true;
        }

        private async Task<bool> UploadFlightAsync(string flightDirectory, CancellationToken cancellationToken)
        {
            string manifestPath = Path.Combine(flightDirectory, FlightOutput.ManifestFile);
            try
            {
                // a new run of this flight id: clear the old run from the archive before any of the new one goes up
                // the marker goes only after the delete succeeded, so a crash in between just deletes again
                string markerPath = Path.Combine(flightDirectory, FlightOutput.ResetMarkerFile);
                if (File.Exists(markerPath))
                {
                    await store.DeletePrefixAsync(Key(flightDirectory) + "/", cancellationToken);
                    File.Delete(markerPath);
                }

                foreach (string filePath in Directory.GetFiles(flightDirectory, "*", SearchOption.AllDirectories))
                {
                    if (filePath == manifestPath || !IsFinished(filePath))
                        continue;

                    await UploadFileAsync(filePath, cancellationToken);
                }

                // the closed flight.json says the flight is complete in the archive, so it goes last, once nothing else is left
                if (File.Exists(manifestPath) && IsClosed(manifestPath)
                    && Directory.GetFiles(flightDirectory, "*", SearchOption.AllDirectories).All(filePath => filePath == manifestPath))
                {
                    await UploadFileAsync(manifestPath, cancellationToken);
                    DeleteEmptyDirectories(flightDirectory);
                    logger.LogInformation("Archived flight {FlightId}", Path.GetFileName(flightDirectory));
                }

                return true;
            }
            // a replay wiped the folder mid-scan; its new run is picked up next time
            catch (Exception ex) when (ex is FileNotFoundException or DirectoryNotFoundException)
            {
                return true;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogWarning("Upload of flight {FlightId} failed ({Message}), retrying on a later scan", Path.GetFileName(flightDirectory), ex.Message);
                return false;
            }
        }

        private async Task UploadFileAsync(string filePath, CancellationToken cancellationToken)
        {
            await using (FileStream content = File.OpenRead(filePath))
                await store.PutAsync(Key(filePath), content, ContentType(filePath), cancellationToken);

            File.Delete(filePath);
        }

        // the archive key is the path under the output directory: {tail}/{flightId}/chunks/000001.jsonl
        private string Key(string path) => Path.GetRelativePath(outputDirectory, path).Replace('\\', '/');

        private static string ContentType(string filePath)
            => ContentTypes.TryGetValue(Path.GetExtension(filePath), out string? contentType) ? contentType : "application/octet-stream";

        private static bool IsHidden(string path) => Path.GetFileName(path).StartsWith('.');

        // .tmp files are still being written; dot-files are signals for the uploader, not part of the flight
        private static bool IsFinished(string filePath) => !IsHidden(filePath) && !filePath.EndsWith(TEMP_SUFFIX, StringComparison.Ordinal);

        private static bool IsClosed(string manifestPath)
        {
            try
            {
                return (string?)JsonNode.Parse(File.ReadAllText(manifestPath))?["status"] == "closed";
            }
            catch (JsonException)
            {
                return false;
            }
        }

        // only empty folders go: if a replay has already started writing here, its files stay
        private static void DeleteEmptyDirectories(string directory)
        {
            foreach (string subdirectory in Directory.GetDirectories(directory))
                DeleteEmptyDirectories(subdirectory);

            try
            {
                Directory.Delete(directory);
            }
            catch (IOException) { }
        }
    }
}
