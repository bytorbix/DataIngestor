namespace DataIngestor.Storage
{
    // runs the uploader's scan on a timer; while the archive keeps failing it waits longer between scans
    public class FlightUploadService(IConfiguration configuration, IServiceProvider services, ILogger<FlightUploader> logger) : BackgroundService
    {
        private const string OUTPUT_DIRECTORY_FIELD = "Output:Directory";
        private const string INTERVAL_FIELD = "Upload:IntervalSeconds";
        private const string ACCESS_KEY_FIELD = "Storage:AccessKey";
        private const string SECRET_KEY_FIELD = "Storage:SecretKey";
        private const int DefaultIntervalSeconds = 5;
        private const int MaxBackoffSeconds = 60;

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            // without keys the store can't be built; flights keep collecting locally and upload once keys are set and the service restarts
            if (string.IsNullOrWhiteSpace(configuration[ACCESS_KEY_FIELD]) || string.IsNullOrWhiteSpace(configuration[SECRET_KEY_FIELD]))
            {
                logger.LogWarning("Storage keys are not configured, flights stay on local disk and are not archived");
                return;
            }

            string outputDirectory = configuration[OUTPUT_DIRECTORY_FIELD] ?? throw new InvalidOperationException("Output:Directory is not configured");
            TimeSpan interval = TimeSpan.FromSeconds(configuration.GetValue(INTERVAL_FIELD, DefaultIntervalSeconds));
            FlightUploader uploader = new(outputDirectory, services.GetRequiredService<IFlightStore>(), logger);

            TimeSpan delay = interval;
            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    bool succeeded = await uploader.UploadPendingAsync(stoppingToken);
                    delay = succeeded ? interval : TimeSpan.FromSeconds(Math.Min(MaxBackoffSeconds, delay.TotalSeconds * 2));
                    await Task.Delay(delay, stoppingToken);
                }
            }
            catch (OperationCanceledException) { }
        }
    }
}
