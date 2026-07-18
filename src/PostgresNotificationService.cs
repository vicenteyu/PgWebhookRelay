using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Channels;

namespace PgWebhookRelay;


public class PostgresNotificationService : BackgroundService
{
    private readonly ILogger<PostgresNotificationService> _logger;
    private readonly RelayOptions _options;
    private readonly HttpClient _httpClient;
    private readonly Channel<PostgresMessage> _memoryChannel;

    public PostgresNotificationService(
        ILogger<PostgresNotificationService> logger,
        IOptions<RelayOptions> options,
        IHttpClientFactory httpClientFactory)
    {
        _logger = logger;
        _options = options.Value;
        _httpClient = httpClientFactory.CreateClient("ConvoyClient");

        _memoryChannel = Channel.CreateBounded<PostgresMessage>(new BoundedChannelOptions(_options.MaxCapacity)
        {
            SingleReader = true,
            SingleWriter = true,
            FullMode = BoundedChannelFullMode.Wait
        });
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (_logger.IsEnabled(LogLevel.Information))
            _logger.LogInformation("Postgres 到 Convoy 转发服务已启动。目标通道: {Channel}", _options.PgChannelName);

        var dbListenerTask = ListenPostgresLoopAsync(stoppingToken);
        var webhookSenderTask = ProcessAndSendWebhookLoopAsync(stoppingToken);

        await Task.WhenAny(dbListenerTask, webhookSenderTask).ConfigureAwait(false);
    }

    /// <summary>
    /// 轨道一：死磕 Postgres 监听（包含无限重连、指数退避、抖动、假死检测）
    /// </summary>
    private async Task ListenPostgresLoopAsync(CancellationToken stoppingToken)
    {
        var retryDelay = TimeSpan.FromSeconds(1);
        const int maxRetryDelaySeconds = 30;

        var connStringBuilder = new NpgsqlConnectionStringBuilder(_options.DbConnectionString)
        {
            KeepAlive = 15,
            Pooling = false
        };

        while (!stoppingToken.IsCancellationRequested)
        {
            NpgsqlConnection? connection = null;
            try
            {
                connection = new NpgsqlConnection(connStringBuilder.ConnectionString);
                await connection.OpenAsync(stoppingToken).ConfigureAwait(false);

                connection.Notification += OnNotificationReceived;

                using (var command = new NpgsqlCommand($"LISTEN {_options.PgChannelName};", connection))
                {
                    await command.ExecuteNonQueryAsync(stoppingToken).ConfigureAwait(false);
                }

                if (_logger.IsEnabled(LogLevel.Information))
                    _logger.LogInformation("成功连接并开始监听 Postgres 通道: {Channel}", _options.PgChannelName);
                retryDelay = TimeSpan.FromSeconds(1);

                while (!stoppingToken.IsCancellationRequested)
                {
                    await connection.WaitAsync(stoppingToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "数据库连接中断或异常。将在 {Delay} 秒后尝试重新连接...", retryDelay.TotalSeconds);

                var jitter = TimeSpan.FromMilliseconds(Random.Shared.Next(0, 500));
                await Task.Delay(retryDelay + jitter, stoppingToken).ConfigureAwait(false);

                retryDelay = TimeSpan.FromSeconds(Math.Min(retryDelay.TotalSeconds * 2, maxRetryDelaySeconds));
            }
            finally
            {
                if (connection != null)
                {
                    connection.Notification -= OnNotificationReceived;
                    await connection.DisposeAsync().ConfigureAwait(false);
                }
            }
        }
    }

    private void OnNotificationReceived(object sender, NpgsqlNotificationEventArgs e)
    {
        var msg = new PostgresMessage(e.Channel, e.Payload);

        if (!_memoryChannel.Writer.TryWrite(msg))
        {
            _logger.LogWarning("内部缓冲区已满（当前上限: {Max}）。正在暂停接收新数据，直至旧数据发送完毕。", _options.MaxCapacity);
        }
    }

    /// <summary>
    /// 轨道二：高可靠 HTTP 转发泵
    /// </summary>
    private async Task ProcessAndSendWebhookLoopAsync(CancellationToken stoppingToken)
    {
        await foreach (var message in _memoryChannel.Reader.ReadAllAsync(stoppingToken).ConfigureAwait(false))
        {
            var retryCount = 0;
            const int maxHttpRetries = 3;
            bool success = false;

            while (!success && retryCount < maxHttpRetries && !stoppingToken.IsCancellationRequested)
            {
                try
                {
                    using var request = new HttpRequestMessage(HttpMethod.Post, _options.ConvoyWebhookUrl);

                    request.Content = new StringContent(message.Payload, Encoding.UTF8, "application/json");

                    if (!string.IsNullOrEmpty(_options.ConvoySecret))
                    {
                        var signature = ComputeHmacSha256(message.Payload, _options.ConvoySecret);
                        request.Headers.Add("X-Convoy-Signature", signature);
                    }

                    using var response = await _httpClient.SendAsync(request, stoppingToken).ConfigureAwait(false);

                    if (response.IsSuccessStatusCode)
                    {
                        success = true;
                    }
                    else
                    {
                        _logger.LogWarning("Webhook 转发失败，状态码: {Code}。准备重试 ({Count}/{Max})...", response.StatusCode, retryCount + 1, maxHttpRetries);
                        retryCount++;
                        await Task.Delay(TimeSpan.FromSeconds(retryCount * 2), stoppingToken).ConfigureAwait(false);
                    }
                }
                catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
                {
                    _logger.LogError(ex, "向 Convoy 发送请求时发生网络异常。准备重试 ({Count}/{Max})...", retryCount + 1, maxHttpRetries);
                    retryCount++;
                    await Task.Delay(TimeSpan.FromSeconds(retryCount * 2), stoppingToken).ConfigureAwait(false);
                }
            }

            if (!success)
            {
                if (_logger.IsEnabled(LogLevel.Critical))
                    _logger.LogCritical("一条消息在连续重试 {Max} 次后依然无法送达 Convoy。丢弃该消息。Payload: {Payload}", maxHttpRetries, message.Payload);
            }
        }
    }

    private static string ComputeHmacSha256(string secret, string message)
    {
        var keyBytes = Encoding.UTF8.GetBytes(secret);
        var messageBytes = Encoding.UTF8.GetBytes(message);
        using var hmac = new HMACSHA256(keyBytes);
        var hashBytes = hmac.ComputeHash(messageBytes);
        return Convert.ToHexString(hashBytes).ToLowerInvariant();
    }
}

public record PostgresMessage(string Channel, string Payload);

public class RelayOptions
{
    public string DbConnectionString { get; set; } = string.Empty;
    public string PgChannelName { get; set; } = string.Empty;
    public string ConvoyWebhookUrl { get; set; } = string.Empty;
    public string? ConvoySecret { get; set; }
    public int MaxCapacity { get; set; } = 20000;
}