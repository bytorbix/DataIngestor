using Confluent.Kafka;
using DataIngestor.Processing;

namespace DataIngestor.Ingestion
{
    
    public class KafkaConsumer : BackgroundService
    {
        private readonly IConfiguration _configuration;
        private readonly IConsumer<string, string> _consumer;
        private readonly ILogger<KafkaConsumer> _logger;
        private readonly TelemetryProcessor _processor;
        private readonly string KAFKA_TOPIC_NAME;


        public KafkaConsumer(IConfiguration configuration, TelemetryProcessor processor ,ILogger<KafkaConsumer> logger)
        {
            _configuration = configuration;
            _processor = processor;
            _logger = logger;
            KAFKA_TOPIC_NAME = configuration["Kafka:Topic"] ?? throw new InvalidOperationException("Kafka:Topic is not configured");

            ConsumerConfig config = new()
            {
                BootstrapServers = configuration["Kafka:BootstrapServers"] ?? throw new InvalidOperationException("Kafka:BootstrapServers is not configured"),
                GroupId = configuration["Kafka:GroupId"] ?? throw new InvalidOperationException("Kafka:GroupId is not configured")
            };

            _consumer = new ConsumerBuilder<string, string>(config).Build();
            _consumer.Subscribe(KAFKA_TOPIC_NAME);
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            return Task.Run(() => ConsumeLoop(stoppingToken), stoppingToken);
        }

        private void ConsumeLoop(CancellationToken stoppingToken)
        {
            try
            {
                ConsumeResult<string, string>? result = null;
                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        result = _consumer.Consume(stoppingToken);
                    }
                    catch (ConsumeException ex)
                    {
                        _logger.LogWarning(ex, "Kafka consume failed for topic {Topic}.", KAFKA_TOPIC_NAME);
                        continue;
                    }
                    try
                    {
                        _processor.Process(result.Message.Key, result.Message.Value);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Failed to process telemetry message for key {Key}, dropping.", result.Message.Key);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("Kafka consume loop stopped.");
            }
        }
    }
}
