using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using ProtoBuf;
using RocksDbSharp;

namespace IoTProcessor;

public record State(double Sum, int Count);

public class TelemetryProcessor : BackgroundService
{
    private readonly IConsumer<string, byte[]> _consumer;
    private readonly ILogger<TelemetryProcessor> _logger;
    private readonly RocksDb _db;
    private readonly string _dbPath = "/data/rocksdb";

    public TelemetryProcessor(IConsumer<string, byte[]> consumer, ILogger<TelemetryProcessor> logger)
    {
        _consumer = consumer;
        _logger = logger;

        var options = new DbOptions().SetCreateIfMissing(true);
        _db = RocksDb.Open(options, _dbPath);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _consumer.Subscribe("raw.telemetry");
        _logger.LogInformation("Processor started, subscribed to raw.telemetry");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var result = _consumer.Consume(stoppingToken);

                using var ms = new MemoryStream(result.Message.Value);
                var telemetry = Serializer.Deserialize<TelemetryProtobuf>(ms);

                var windowStart = (telemetry.TimestampMs / 60000) * 60000;
                var key = $"{telemetry.DeviceId}:{windowStart}";

                var existing = _db.Get(key);
                State state;
                if (existing != null)
                {
                    var deserialized = System.Text.Json.JsonSerializer.Deserialize<State>(existing);
                    state = deserialized ?? new State(0, 0);
                }
                else
                {
                    state = new State(0, 0);
                }


                state = state with { Sum = state.Sum + telemetry.Value, Count = state.Count + 1 };

                var json = System.Text.Json.JsonSerializer.Serialize(state);
                _db.Put(key, json);

                _logger.LogDebug("Device {DeviceId}: sum={Sum}, count={Count}", telemetry.DeviceId, state.Sum, 
                    state.Count);

                _consumer.Commit(result);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing message");
            }
        }
    }

    public override void Dispose()
    {
        _db.Dispose();
        _consumer.Dispose();
        base.Dispose();
    }
}