namespace API.MQTT;

public interface IMqttMessageHandler
{
    string TopicFilter { get; }

    Task HandleAsync(MqttMessage message, CancellationToken cancellationToken);
    
}