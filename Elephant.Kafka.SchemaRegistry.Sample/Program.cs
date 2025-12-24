using Avro.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Elephant.Kafka.SchemaRegistry.Sample.Workers;
using Elephant.Kafka.SchemaRegistry.Sample.Workers.Serdes;
using Take.Elephant.Kafka.SchemaRegistry;

namespace Elephant.Kafka.SchemaRegistry.Sample;

public abstract class Program
{
    public static void Main(string[] args)
    {
        var builder = Host.CreateApplicationBuilder(args);

        const string topic = "sample-topic";

        var consumerConfig = builder.Configuration.GetSection("Kafka:Consumer").Get<ConsumerConfig>()
                             ?? throw new InvalidOperationException("Kafka:Consumer is not set.");

        var producerConfig = builder.Configuration.GetSection("Kafka:Producer").Get<ProducerConfig>()
                             ?? throw new InvalidOperationException("Kafka:Producer is not set.");

        var schemaRegistryConfig = builder.Configuration.GetSection("SchemaRegistry").Get<SchemaRegistryConfig>()
                                   ?? throw new InvalidOperationException("SchemaRegistry is not set.");

        var schemaRegistryOptions =
            new SchemaRegistryOptions(schemaRegistryConfig, SchemaRegistrySerializerType.AVRO);

        builder.Services.AddSingleton(
            new KafkaSchemaRegistrySenderQueue<TestItemAvro>(
                producerConfig,
                topic,
                schemaRegistryOptions
            )
        );

        builder.Services.AddSingleton(
            new KafkaSchemaRegistryReceiverQueue<GenericRecord>(
                consumerConfig,
                topic,
                schemaRegistryOptions
            )
        );

        builder.Services.AddHostedService<ConsumerWorker>();
        builder.Services.AddHostedService<ProducerWorker>();

        var host = builder.Build();
        host.Run();
    }
}