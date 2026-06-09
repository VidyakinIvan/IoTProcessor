using ProtoBuf;

namespace IoTProcessor;

[ProtoContract]
public class TelemetryProtobuf
{
    [ProtoMember(1)]
    public int DeviceId { get; set; }

    [ProtoMember(2)]
    public long TimestampMs { get; set; }

    [ProtoMember(3)]
    public double Value { get; set; }

    [ProtoMember(4)]
    public long ReceivedAtMs { get; set; }
}