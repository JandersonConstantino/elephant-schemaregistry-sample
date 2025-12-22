using Avro.Generic;
using Confluent.Kafka;
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

        var schemaRegistryOptions =
            new SchemaRegistryOptions("http://localhost:8081", SchemaRegistrySerializerType.AVRO);

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "elephant-sample",
            AutoOffsetReset = AutoOffsetReset.Latest
        };

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = "localhost:9092"
        };

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