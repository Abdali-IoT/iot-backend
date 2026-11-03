using System.Text.Json;
using API.MQTT;

namespace API.Handlers;

public sealed class TemperatureHandler(ILogger<TemperatureHandler> logger) : IMqttMessageHandler
{
    public string TopicFilter => "sensors/temperature";

    public Task HandleAsync(MqttMessage message, CancellationToken cancellationToken)
    {
        // logger.LogInformation("Temperature received: {Payload}", message.Payload);
        var temperatureMessage = JsonSerializer.Deserialize<TemperatureMessage>(message.Payload);
        logger.LogInformation("Temperature object: {temperatureMessage}", temperatureMessage);

        return Task.CompletedTask;
    }
}

public sealed record TemperatureMessage(string userId, double temperature);
