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

    private State? LoadFromRocksDb(int deviceId, long windowStart)
    {
        var key = MakeKey(deviceId, windowStart);
        var json = _db.Get(key);
        if (string.IsNullOrEmpty(json)) return null;
        return System.Text.Json.JsonSerializer.Deserialize<State>(json);
    }

    private void SaveToRocksDb(int deviceId, long windowStart, double sum, int count)
    {
        var key = MakeKey(deviceId, windowStart);
        var state = new State(sum, count);
        var json = System.Text.Json.JsonSerializer.Serialize(state);
        _db.Put(key, json);
        _logger.LogDebug("Saved to RocksDB: key={Key}, sum={Sum}, count={Count}", key, sum, count);
    }

    private void DeleteFromRocksDb(int deviceId, long windowStart)
    {
        var key = MakeKey(deviceId, windowStart);
        _db.Remove(key);
        _logger.LogDebug("Deleted from RocksDB: key={Key}", key);
    }

    private void SaveCheckpoint()
    {
        try
        {
            var iterator = _db.NewIterator();
            iterator.SeekToFirst();
            var checkpoint = new List<CheckpointEntry>();
            while (iterator.Valid())
            {
                var key = iterator.StringKey();
                if (!key.StartsWith("checkpoint_") && !key.StartsWith("meta_"))
                {
                    var parts = key.Split(':');
                    if (parts.Length == 2 && int.TryParse(parts[0], out var deviceId) && long.TryParse(parts[1], out var windowStart))
                    {
                        var state = System.Text.Json.JsonSerializer.Deserialize<State>(iterator.StringValue());
                        if (state != null)
                        {
                            checkpoint.Add(new CheckpointEntry
                            {
                                DeviceId = deviceId,
                                WindowStart = windowStart,
                                Sum = state.Sum,
                                Count = state.Count
                            });
                        }
                    }
                }
                iterator.Next();
            }
            var json = System.Text.Json.JsonSerializer.Serialize(checkpoint);
            _db.Put("checkpoint", json);
            _logger.LogInformation("Checkpoint saved: {Count} active windows", checkpoint.Count);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to save checkpoint");
        }
    }

    private void LoadCheckpoint()
    {
        try
        {
            var json = _db.Get("checkpoint");
            if (string.IsNullOrEmpty(json)) return;
            var checkpoint = System.Text.Json.JsonSerializer.Deserialize<List<CheckpointEntry>>(json);
            if (checkpoint == null) return;
            foreach (var entry in checkpoint)
            {
                var key = MakeKey(entry.DeviceId, entry.WindowStart);
                var state = new State(entry.Sum, entry.Count);
                var stateJson = System.Text.Json.JsonSerializer.Serialize(state);
                _db.Put(key, stateJson);
                _logger.LogInformation("Restored from checkpoint: device {DeviceId}, window {WindowStart}, sum {Sum}, count {Count}",
                    entry.DeviceId, entry.WindowStart, entry.Sum, entry.Count);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to load checkpoint");
        }
    }

    private class CheckpointEntry
    {
        public int DeviceId { get; set; }
        public long WindowStart { get; set; }
        public double Sum { get; set; }
        public int Count { get; set; }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _consumer.Subscribe("raw.telemetry");
        _logger.LogInformation("Processor started, subscribed to raw.telemetry");

        LoadCheckpoint();

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

                var currentState = LoadFromRocksDb(telemetry.DeviceId, currentWindow);

                if (currentState == null)
                {
                    SaveToRocksDb(telemetry.DeviceId, currentWindow, telemetry.Value, 1);
                    _logger.LogInformation("New window for device {DeviceId}: WindowStart={Ws}, Count=1",
                        telemetry.DeviceId, currentWindow);
                }
                else
                {
                    var newSum = currentState.Sum + telemetry.Value;
                    var newCount = currentState.Count + 1;
                    SaveToRocksDb(telemetry.DeviceId, currentWindow, newSum, newCount);
                    _logger.LogInformation("Updated window for device {DeviceId}: WindowStart={Ws}, Sum={Sum}, Count={Count}",
                        telemetry.DeviceId, currentWindow, newSum, newCount);

                    if (newCount > 1)
                    {
                        var currentAvg = newSum / newCount;
                        var deviation = Math.Abs(telemetry.Value - currentAvg);
                        if (deviation > 10.0)
                        {
                            _logger.LogWarning("Anomaly: device {DeviceId}, value {Value} deviates from avg {Avg} by {Dev}",
                                telemetry.DeviceId, telemetry.Value, currentAvg, deviation);
                        }
                    }
                }

                var prevWindow = currentWindow - 60000;
                var prevState = LoadFromRocksDb(telemetry.DeviceId, prevWindow);
                if (prevState != null)
                {
                    var avg = prevState.Sum / prevState.Count;
                    _logger.LogInformation("Closing window for device {DeviceId}: WindowStart={Ws}, Count={Cnt}, Avg={Avg}",
                        telemetry.DeviceId, prevWindow, prevState.Count, avg);

                    try
                    {
                        using var httpClient = new HttpClient();
                        var query = $"INSERT INTO telemetry_avg (device_id, window_start, avg_value, location) VALUES ({telemetry.DeviceId}, fromUnixTimestamp64Milli({prevWindow}), {avg.ToString(CultureInfo.InvariantCulture)}, '{location}')";
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

                    DeleteFromRocksDb(telemetry.DeviceId, prevWindow);
                }

                _consumer.Commit(result);

                if ((DateTime.UtcNow - _lastCheckpoint).TotalSeconds >= 10)
                {
                    SaveCheckpoint();
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
        SaveCheckpoint();
        _db.Dispose();
        _consumer.Dispose();
        base.Dispose();
    }
}