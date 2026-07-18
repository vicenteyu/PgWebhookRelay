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
        _logger.LogInformation("正在初始化 Postgres 到 Convoy 转发服务...");

        var dbListenerTask = ListenPostgresLoopAsync(stoppingToken);
        var webhookSenderTask = ProcessAndSendWebhookLoopAsync(stoppingToken);

        var firstCompletedTask = await Task.WhenAny(dbListenerTask, webhookSenderTask).ConfigureAwait(false);
        await firstCompletedTask;
    }

    /// <summary>
    /// 轨道一：死磕 Postgres 监听（包含细粒度状态、主动心跳监控及重连）
    /// </summary>
    private async Task ListenPostgresLoopAsync(CancellationToken stoppingToken)
    {
        var retryDelay = TimeSpan.FromSeconds(1);
        const int maxRetryDelaySeconds = 30;

        // 基础 KeepAlive 设为 15 秒，但依赖底层 TCP 往往不可靠，后续配合应用层心跳
        var connStringBuilder = new NpgsqlConnectionStringBuilder(_options.DbConnectionString)
        {
            KeepAlive = 15,
            Pooling = false,
            Timeout = 10 // 限制物理握手建立连接的超时为 10 秒
        };

        while (!stoppingToken.IsCancellationRequested)
        {
            NpgsqlConnection? connection = null;
            using var ctsForLoop = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);

            try
            {
                _logger.LogInformation("正在尝试与 Postgres 数据库建立连接...");
                connection = new NpgsqlConnection(connStringBuilder.ConnectionString);
                await connection.OpenAsync(ctsForLoop.Token).ConfigureAwait(false);

                _logger.LogInformation("数据库连接已成功开启，正在绑定 Notification 事件监听器...");
                connection.Notification += OnNotificationReceived;

                if (_logger.IsEnabled(LogLevel.Information))
                    _logger.LogInformation("正在向 Postgres 订阅通道命令: LISTEN {Channel}", _options.PgChannelName);
                using (var command = new NpgsqlCommand($"LISTEN {_options.PgChannelName};", connection))
                {
                    await command.ExecuteNonQueryAsync(ctsForLoop.Token).ConfigureAwait(false);
                }

                if (_logger.IsEnabled(LogLevel.Information))
                    _logger.LogInformation("✅ 【服务正式就绪】成功连接数据库并订阅通道: {Channel}，正在监听事件...", _options.PgChannelName);
                retryDelay = TimeSpan.FromSeconds(1);

                while (!ctsForLoop.IsCancellationRequested)
                {
                    // 挂起等待物理连接抛出通知。如果心跳由于假死超时失败，ctsForLoop 会取消，这里会被打断。
                    await connection.WaitAsync(ctsForLoop.Token).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation("收到服务停止信号，正在安全退出监听任务。");
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "【连接失效】发现数据库链路中断、假死或无法建立连接。将在 {Delay} 秒后尝试第 N 次重连...", retryDelay.TotalSeconds);

                var jitter = TimeSpan.FromMilliseconds(Random.Shared.Next(0, 500));
                await Task.Delay(retryDelay + jitter, stoppingToken).ConfigureAwait(false);

                retryDelay = TimeSpan.FromSeconds(Math.Min(retryDelay.TotalSeconds * 2, maxRetryDelaySeconds));
            }
            finally
            {
                if (connection != null)
                {
                    _logger.LogInformation("正在清理当前失效连接并移除监听器...");
                    connection.Notification -= OnNotificationReceived;
                    await connection.DisposeAsync().ConfigureAwait(false);
                }
            }
        }
    }

    private void OnNotificationReceived(object sender, NpgsqlNotificationEventArgs e)
    {
        if (_logger.IsEnabled(LogLevel.Information))
            _logger.LogInformation("【事件命中】从 Postgres 通道 [{Channel}] 收到原始消息，长度: {Len} 字节。正在推入缓冲区...", e.Channel, e.Payload?.Length ?? 0);

        var msg = new PostgresMessage(e.Channel, e.Payload ?? string.Empty);

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

            _logger.LogInformation("从缓冲区提取消息，开始组织转发至 Convoy Endpoint...");

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
                        if (_logger.IsEnabled(LogLevel.Information))
                            _logger.LogInformation("【转发成功】Webhook 成功送达 Convoy！状态码: {Code}", response.StatusCode);
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