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
    private DateTime _lastCheckpoint = DateTime.UtcNow;

    private readonly Dictionary<int, string> _deviceMetadata = new()
    {
        { 1, "Moscow" }, { 2, "SPb" }, { 3, "Kazan" }, { 4, "Novosibirsk" }, { 5, "Sochi" },
        { 6, "Sevastopol" }, { 7, "Vladivostok" }, { 8, "Ekaterinburg" }, { 9, "Perm" }, { 10, "Murmansk" }
    };

    public TelemetryProcessor(IConsumer<string, byte[]> consumer, ILogger<TelemetryProcessor> logger)
    {
        _consumer = consumer;
        _logger = logger;

        var options = new DbOptions().SetCreateIfMissing(true);
        _db = RocksDb.Open(options, _dbPath);
    }

    private string MakeKey(int deviceId, long windowStart) => $"{deviceId}:{windowStart}";

    private (double Sum, int Count, long WindowStart)? LoadFromRocksDb(int deviceId, long windowStart)
    {
        var key = MakeKey(deviceId, windowStart);
        var json = _db.Get(key);
        if (string.IsNullOrEmpty(json)) return null;
        var state = System.Text.Json.JsonSerializer.Deserialize<State>(json);
        return (state.Sum, state.Count, windowStart);
    }

    private void SaveToRocksDb(int deviceId, long windowStart, double sum, int count)
    {
        var key = MakeKey(deviceId, windowStart);
        var state = new State(sum, count);
        var json = System.Text.Json.JsonSerializer.Serialize(state);
        _db.Put(key, json);
    }

    private void DeleteFromRocksDb(int deviceId, long windowStart)
    {
        var key = MakeKey(deviceId, windowStart);
        _db.Remove(key);
    }

    private void SaveCheckpoint(Dictionary<int, (double Sum, int Count, long WindowStart)> state)
    {
        try
        {
            var checkpoint = state.Select(kv => new CheckpointEntry
            {
                Key = kv.Key,
                Sum = kv.Value.Sum,
                Count = kv.Value.Count,
                WindowStart = kv.Value.WindowStart
            }).ToList();
            var json = System.Text.Json.JsonSerializer.Serialize(checkpoint);
            _db.Put("checkpoint", json);
            _logger.LogInformation("Checkpoint saved: {Count} active windows", checkpoint.Count);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to save checkpoint");
        }
    }

    private Dictionary<int, (double Sum, int Count, long WindowStart)> LoadCheckpoint()
    {
        try
        {
            var json = _db.Get("checkpoint");
            if (string.IsNullOrEmpty(json)) return new Dictionary<int, (double Sum, int Count, long WindowStart)>();
            var checkpoint = System.Text.Json.JsonSerializer.Deserialize<List<CheckpointEntry>>(json);
            if (checkpoint == null) return new Dictionary<int, (double Sum, int Count, long WindowStart)>();
            return checkpoint.ToDictionary(e => e.Key, e => (e.Sum, e.Count, e.WindowStart));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to load checkpoint");
            return new Dictionary<int, (double Sum, int Count, long WindowStart)>();
        }
    }

    private class CheckpointEntry
    {
        public int Key { get; set; }
        public double Sum { get; set; }
        public int Count { get; set; }
        public long WindowStart { get; set; }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _consumer.Subscribe("raw.telemetry");
        _logger.LogInformation("Processor started, subscribed to raw.telemetry");

        var state = LoadCheckpoint();
        _logger.LogInformation("Loaded {Count} windows from checkpoint", state.Count);

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

                if (telemetry.Value < -50 || telemetry.Value > 150)
                {
                    _logger.LogWarning("Filtered out outlier from device {DeviceId}: {Value}", telemetry.DeviceId, telemetry.Value);
                    _consumer.Commit(result);
                    continue;
                }

                string location = _deviceMetadata.GetValueOrDefault(telemetry.DeviceId, "unknown");
                _logger.LogInformation("Enriched: device {DeviceId}, location {Location}", telemetry.DeviceId, location);

                if (!state.TryGetValue(telemetry.DeviceId, out var agg) || agg.WindowStart != currentWindow)
                {
                    var fromDb = LoadFromRocksDb(telemetry.DeviceId, currentWindow);
                    if (fromDb.HasValue)
                    {
                        agg = fromDb.Value;
                        state[telemetry.DeviceId] = agg;
                        _logger.LogInformation("Loaded from RocksDB: device {DeviceId}, WindowStart={Ws}, Sum={Sum}, Count={Count}",
                            telemetry.DeviceId, agg.WindowStart, agg.Sum, agg.Count);
                    }
                }

                if (!state.TryGetValue(telemetry.DeviceId, out var currentAgg) || currentAgg.WindowStart != currentWindow)
                {
                    state[telemetry.DeviceId] = (telemetry.Value, 1, currentWindow);
                    SaveToRocksDb(telemetry.DeviceId, currentWindow, telemetry.Value, 1);
                    _logger.LogInformation("New window for device {DeviceId}: WindowStart={Ws}, Count=1",
                        telemetry.DeviceId, currentWindow);
                }
                else if (currentWindow != currentAgg.WindowStart)
                {
                    if (currentAgg.Count > 0)
                    {
                        var avg = currentAgg.Sum / currentAgg.Count;
                        _logger.LogInformation("Closing window for device {DeviceId}: WindowStart={Ws}, Count={Cnt}, Avg={Avg}",
                            telemetry.DeviceId, currentAgg.WindowStart, currentAgg.Count, avg);

                        try
                        {
                            using var httpClient = new HttpClient();
                            var query = $"INSERT INTO telemetry_avg (device_id, window_start, avg_value, location) VALUES ({telemetry.DeviceId}, fromUnixTimestamp64Milli({currentAgg.WindowStart}), {avg.ToString(CultureInfo.InvariantCulture)}, '{location}')";
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

                        DeleteFromRocksDb(telemetry.DeviceId, currentAgg.WindowStart);
                    }

                    state[telemetry.DeviceId] = (telemetry.Value, 1, currentWindow);
                    SaveToRocksDb(telemetry.DeviceId, currentWindow, telemetry.Value, 1);
                    _logger.LogInformation("New window for device {DeviceId}: WindowStart={Ws}, Count=1",
                        telemetry.DeviceId, currentWindow);
                }
                else
                {
                    var sum = currentAgg.Sum + telemetry.Value;
                    var count = currentAgg.Count + 1;
                    state[telemetry.DeviceId] = (sum, count, currentWindow);
                    SaveToRocksDb(telemetry.DeviceId, currentWindow, sum, count);

                    if (count > 1)
                    {
                        var currentAvg = sum / count;
                        var deviation = Math.Abs(telemetry.Value - currentAvg);
                        if (deviation > 10.0)
                        {
                            _logger.LogWarning("Anomaly: device {DeviceId}, value {Value} deviates from avg {Avg} by {Dev}",
                                telemetry.DeviceId, telemetry.Value, currentAvg, deviation);
                        }
                    }

                    _logger.LogInformation("Updated window for device {DeviceId}: WindowStart={Ws}, Sum={Sum}, Count={Count}",
                        telemetry.DeviceId, currentWindow, sum, count);
                }

                _consumer.Commit(result);

                if ((DateTime.UtcNow - _lastCheckpoint).TotalSeconds >= 10)
                {
                    SaveCheckpoint(state);
                    _lastCheckpoint = DateTime.UtcNow;
                }
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