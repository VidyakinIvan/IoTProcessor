using Confluent.Kafka;
using IoTProcessor;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using OpenTelemetry;
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


builder.Services.AddHostedService<TelemetryProcessor>();
builder.Logging.AddConsole();

var host = builder.Build();

var meterProvider = Sdk.CreateMeterProviderBuilder()
    .AddMeter("IoTProcessor")
    .AddPrometheusHttpListener(options =>
    {
        options.UriPrefixes = new[] { "http://*:8080/" };
    })
    .Build();


await host.RunAsync();
