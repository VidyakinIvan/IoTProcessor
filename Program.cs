using Confluent.Kafka;
using IoTProcessor;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using OpenTelemetry.Metrics;

var builder = Host.CreateApplicationBuilder(args);

var kafkaBootstrap = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "my-cluster-kafka-bootstrap.kafka:9092";

builder.Services.AddSingleton<IConsumer<string, byte[]>>(sp =>
{
    var config = new ConsumerConfig
    {
        BootstrapServers = kafkaBootstrap,
        GroupId = "processor-group",
        AutoOffsetReset = AutoOffsetReset.Earliest,
        IsolationLevel = IsolationLevel.ReadCommitted,
        EnableAutoCommit = false
    };
    return new ConsumerBuilder<string, byte[]>(config).Build();
});

builder.Services.AddOpenTelemetry()
    .WithMetrics(metrics => metrics
        .AddMeter("IoTProcessor")
        .AddPrometheusExporter());

builder.Services.AddHostedService<TelemetryProcessor>();
builder.Logging.AddConsole();

var host = builder.Build();
await host.RunAsync();
