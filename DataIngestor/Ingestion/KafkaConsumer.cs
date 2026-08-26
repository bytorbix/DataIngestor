using Confluent.Kafka;
using DataIngestor.Processing;

namespace DataIngestor.Ingestion
{
    
    public class KafkaConsumer : BackgroundService
    {
        private readonly IConfiguration configuration;
        private readonly IConsumer<string, string> consumer;
        private readonly ILogger<KafkaConsumer> _logger;
        private readonly TelemetryProcessor processor;
        private readonly string KAFKA_TOPIC_NAME;


        public KafkaConsumer(IConfiguration configuration, TelemetryProcessor processor ,ILogger<KafkaConsumer> logger)
        {
            this.configuration = configuration;
            this.processor = processor;
            _logger = logger;
            KAFKA_TOPIC_NAME = configuration["Kafka:Topic"] ?? throw new InvalidOperationException("Kafka:Topic is not configured");

            ConsumerConfig config = new()
            {
                BootstrapServers = configuration["Kafka:BootstrapServers"] ?? throw new InvalidOperationException("Kafka:BootstrapServers is not configured"),
                GroupId = configuration["Kafka:GroupId"] ?? throw new InvalidOperationException("Kafka:GroupId is not configured")
            };

            consumer = new ConsumerBuilder<string, string>(config).Build();
            consumer.Subscribe(KAFKA_TOPIC_NAME);
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
                while (true)
                {
                    result = null;
                    try
                    {
                        result = consumer.Consume(stoppingToken);
                    }
                    catch (ConsumeException ex)
                    {
                        _logger.LogWarning(ex, "Kafka consume failed for topic {Topic}.", KAFKA_TOPIC_NAME);
                        continue;
                    }
                    processor.Process(result.Message.Key, result.Message.Value);
                }
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("Kafka consume loop stopped.");
            }
        }
    }
}
