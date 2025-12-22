using Avro.Generic;
using Take.Elephant.Kafka.SchemaRegistry;

namespace Elephant.Kafka.SchemaRegistry.Sample.Workers;

public class ConsumerWorker(ILogger<ConsumerWorker> logger, KafkaSchemaRegistryReceiverQueue<GenericRecord> queue)
    : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        logger.Log(LogLevel.Information, "Starting consumer");
        while (!cancellationToken.IsCancellationRequested)
        {
            var item = await queue.DequeueOrDefaultAsync(cancellationToken);
            if (item != null)
            {
                logger.Log(LogLevel.Information, "⬅️ Message received: {0}", item.ToString());
            }

            await Task.Delay(1000, cancellationToken);
        }

        await Task.Delay(1000, cancellationToken);
        logger.Log(LogLevel.Information, "Stopping consumer");
    }
}