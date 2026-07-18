using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using PgWebhookRelay;

Console.OutputEncoding = System.Text.Encoding.UTF8;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.Configure<RelayOptions>(options =>
{
    options.DbConnectionString = builder.Configuration["DB_CONNECTION_STRING"]
        ?? throw new NullReferenceException("DB_CONNECTION_STRING");
    options.PgChannelName = builder.Configuration["PG_CHANNEL_NAME"]
        ?? throw new NullReferenceException("PG_CHANNEL_NAME");
    options.ConvoyWebhookUrl = builder.Configuration["CONVOY_WEBHOOK_URL"]
        ?? throw new NullReferenceException("CONVOY_WEBHOOK_URL");
    options.ConvoySecret = builder.Configuration["CONVOY_SECRET"];

    if (int.TryParse(builder.Configuration["MAX_CAPACITY"], out var maxCap))
    {
        options.MaxCapacity = maxCap;
    }
});

builder.Services.AddHttpClient("ConvoyClient", client =>
{
    client.Timeout = TimeSpan.FromSeconds(10);
});

builder.Services.AddHostedService<PostgresNotificationService>();

var host = builder.Build();
await host.RunAsync();