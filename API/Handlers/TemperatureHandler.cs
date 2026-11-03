using API.MQTT;

namespace API.Handlers;

public sealed class TemperatureHandler(ILogger<TemperatureHandler> logger) : IMqttMessageHandler
{
    public string TopicFilter => "sensors/+/temperature";
    
    public Task HandleAsync(MqttMessage message, CancellationToken cancellationToken)
    {
        logger.LogInformation("Temperature received: {Payload}", message.Payload);

        return Task.CompletedTask;
    }
}