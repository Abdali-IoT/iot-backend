namespace API.MQTT;

public sealed record MqttMessage(
    string Topic,
    string Payload
);