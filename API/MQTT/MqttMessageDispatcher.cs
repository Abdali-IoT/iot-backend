using MQTTnet;

namespace API.MQTT;

public sealed class MqttMessageDispatcher(
    IServiceScopeFactory scopeFactory,
    ILogger<MqttMessageDispatcher> logger)
{
    public async Task DispatchAsync(MqttMessage message, CancellationToken cancellationToken)
    {
        await using var scope = scopeFactory.CreateAsyncScope();

        var handlers = scope.ServiceProvider.GetServices<IMqttMessageHandler>();

        var matched = false;

        foreach (var handler in handlers)
        {
            var result = MqttTopicFilterComparer.Compare(message.Topic, handler.TopicFilter);

            if (result != MqttTopicFilterCompareResult.IsMatch)
                continue;

            matched = true;

            try
            {
                await handler.HandleAsync(message, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                logger.LogError(
                    ex,
                    "Handler {Handler} failed for {Topic}",
                    handler.GetType().Name,
                    message.Topic);
            }
        }

        if (!matched)
            logger.LogWarning("No handler found for topic {Topic}", message.Topic);
        
    }
}