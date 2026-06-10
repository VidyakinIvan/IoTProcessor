using ClickHouse.Client;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using ProtoBuf;
using RocksDbSharp;
using System.Globalization;
using System.Text;

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

        var state = new Dictionary<int, (double Sum, int Count, long WindowStart)>();

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var result = _consumer.Consume(stoppingToken);
                using var stream = new MemoryStream(result.Message.Value);
                var telemetry = Serializer.Deserialize<TelemetryProtobuf>(stream);

                var podName = Environment.GetEnvironmentVariable("HOSTNAME") ?? "unknown";
                _logger.LogInformation("Pod {PodName} processing device {DeviceId}, ts={Timestamp}, value={Value}",
                    podName, telemetry.DeviceId, telemetry.TimestampMs, telemetry.Value);

                long currentWindow = (telemetry.TimestampMs / 60000) * 60000;

                if (!state.TryGetValue(telemetry.DeviceId, out var agg))
                {
                    state[telemetry.DeviceId] = (telemetry.Value, 1, currentWindow);
                    _logger.LogInformation("New window for device {DeviceId}: WindowStart={Ws}, Count=1",
                        telemetry.DeviceId, currentWindow);
                }
                else if (currentWindow != agg.WindowStart)
                {
                    if (agg.Count > 0)
                    {
                        var avg = agg.Sum / agg.Count;
                        _logger.LogInformation("Closing window for device {DeviceId}: WindowStart={Ws}, Count={Cnt}, Avg={Avg}",
                            telemetry.DeviceId, agg.WindowStart, agg.Count, avg);

                        try
                        {
                            using var httpClient = new HttpClient();
                            var query = $"INSERT INTO telemetry_avg (device_id, window_start, avg_value) VALUES ({telemetry.DeviceId}, fromUnixTimestamp64Milli({agg.WindowStart}), {avg.ToString(CultureInfo.InvariantCulture)})";
                            var content = new StringContent(query, Encoding.UTF8, "application/x-www-form-urlencoded");

                            var auth = Convert.ToBase64String(Encoding.ASCII.GetBytes("default:qwerty"));
                            httpClient.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", auth);

                            var response = await httpClient.PostAsync("http://clickhouse.clickhouse:8123/", content, stoppingToken);
                            if (response.IsSuccessStatusCode)
                            {
                                _logger.LogInformation("Saved to ClickHouse: device={DeviceId}, avg={Avg}", telemetry.DeviceId, avg);
                            }
                            else
                            {
                                var error = await response.Content.ReadAsStringAsync(stoppingToken);
                                _logger.LogWarning("Failed to save to ClickHouse: {Error}", error);
                            }
                        }
                        catch (Exception chEx)
                        {
                            _logger.LogError(chEx, "HTTP error saving to ClickHouse");
                        }
                    }

                    state[telemetry.DeviceId] = (telemetry.Value, 1, currentWindow);
                    _logger.LogInformation("New window for device {DeviceId}: WindowStart={Ws}, Count=1",
                        telemetry.DeviceId, currentWindow);
                }
                else
                {
                    agg.Sum += telemetry.Value;
                    agg.Count++;
                    state[telemetry.DeviceId] = agg;
                    _logger.LogInformation("Updated window for device {DeviceId}: WindowStart={Ws}, Sum={Sum}, Count={Count}",
                        telemetry.DeviceId, agg.WindowStart, agg.Sum, agg.Count);
                }

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