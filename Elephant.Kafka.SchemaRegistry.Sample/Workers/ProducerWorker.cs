using Elephant.Kafka.SchemaRegistry.Sample.Workers.Serdes;
using Take.Elephant.Kafka.SchemaRegistry;

namespace Elephant.Kafka.SchemaRegistry.Sample.Workers;

public class ProducerWorker(ILogger<ProducerWorker> logger, KafkaSchemaRegistrySenderQueue<TestItemAvro> queue)
    : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        logger.Log(LogLevel.Information, "Starting producer");

        while (!cancellationToken.IsCancellationRequested)
        {
            var item = new TestItemAvro
            {
                Id = Guid.NewGuid().ToString(),
                Name = Guid.NewGuid().ToString("N"),
                Value = new Random().Next(0, 1000),
                CreatedAt = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            };

            await queue.EnqueueAsync(item, cancellationToken);
            logger.Log(LogLevel.Information, "➡️ Message sent: {0}", item.ToString());
            await Task.Delay(1000, cancellationToken);
        }

        await Task.Delay(1000, cancellationToken);
        logger.Log(LogLevel.Information, "Stopping producer");
    }
}