using Confluent.Kafka;
using static Confluent.Kafka.ConfigPropertyNames;

namespace DataIngestor.KafkaBlock
{
    
    public class KafkaBlock
    {
        public string KAFKA_TOPIC_NAME = "telemetry";
        public IConfiguration configuration;
        private IConsumer<string, string> consumer;
        private ILogger<KafkaBlock> _logger;

        public KafkaBlock(IConfiguration configuration, ILogger<KafkaBlock> logger)
        {
            this.configuration = configuration;
            this._logger = logger;

            ConsumerConfig config = new()
            {
                BootstrapServers = configuration["Kafka:BootstrapServers"],
                GroupId = configuration["Kafka:GroupId"],
                AutoOffsetReset = AutoOffsetReset.Latest
            };

            consumer = new ConsumerBuilder<string, string>(config).Build();
            consumer.Subscribe(KAFKA_TOPIC_NAME);

            Task.Run(ConsumeLoop);
        }

        private void ConsumeLoop()
        {
            while (true)
            {
                ConsumeResult<string, string> result = consumer.Consume();
                _logger.LogInformation("[{Key}] {Value}", result.Message.Key, result.Message.Value);
            }
        }


    }
}
