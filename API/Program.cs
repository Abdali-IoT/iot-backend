using API.Handlers;
using API.MQTT;
using API.Schedular;
using Serilog;
using Serilog.Formatting.Compact;

Log.Logger = new LoggerConfiguration()
    .WriteTo.Console()
    .CreateLogger();
try
{
    var builder = WebApplication.CreateBuilder(args);
    builder.Services.AddSerilog((services, log) =>
    {
        log
            .Enrich.FromLogContext()
            .WriteTo.Console();
    });

    builder.Services.AddScoped<IMqttMessageHandler, TemperatureHandler>();
    
    builder.Services.AddSingleton<MqttMessageDispatcher>();
    builder.Services.AddHostedService<MqttBackgroundService>();

    var app = builder.Build();
    await app.RunAsync();
}
catch (Exception ex)
{
    Log.Error(ex, ex.Message);
}
finally
{
    await Log.CloseAndFlushAsync();    
}
