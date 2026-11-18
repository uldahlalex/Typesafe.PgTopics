using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;

namespace Typesafe.PgTopics;

/// <summary>
/// Topics on Postgres LISTEN/NOTIFY. Each topic is its own Postgres channel and every message is plain JSON, so SQL
/// (a trigger, psql, another app) can publish with <c>pg_notify('topic', json)</c>.
/// <para>
/// One connection per process LISTENs on every topic that has subscribers: it LISTENs when the first subscriber
/// arrives and UNLISTENs when the last one leaves. Each subscriber has its own bounded buffer, so a slow reader never
/// holds up the others. Use one instance per app (<c>AddPostgresTopics</c> registers it as a singleton).
/// </para>
/// </summary>
public sealed class PostgresPubSub : ITopicEventSender, ITopicEventReceiver, IAsyncDisposable
{
    // Postgres rejects notification payloads of 8000 bytes or more.
    private const int MaxPayloadBytes = 7999;

    private readonly PostgresTopicsOptions _options;
    private readonly ILogger _log;
    private readonly NpgsqlDataSource _dataSource;

    // Guards _topics, the subscriber lists and _loop. Never held while awaiting.
    private readonly object _gate = new();
    private readonly Dictionary<string, ActiveTopic> _topics = new(StringComparer.Ordinal);

    // LISTEN/UNLISTEN statements for the listening connection, run in order by the loop.
    private readonly Channel<Command> _commands = Channel.CreateUnbounded<Command>();
    private readonly CancellationTokenSource _shutdown = new();
    private CancellationTokenSource? _wakeUp; // cancels the loop's WaitAsync so it runs new commands
    private Task? _loop;
    private volatile bool _disposed;

    public PostgresPubSub(PostgresTopicsOptions options, ILoggerFactory? loggerFactory = null)
    {
        if (string.IsNullOrWhiteSpace(options.ConnectionString))
            throw new ArgumentException("PostgresTopicsOptions.ConnectionString is required.", nameof(options));
        if (options.TopicBufferCapacity < 1)
            throw new ArgumentException("PostgresTopicsOptions.TopicBufferCapacity must be at least 1.", nameof(options));

        _options = options;
        _log = loggerFactory?.CreateLogger("Typesafe.PgTopics") ?? NullLogger.Instance;

        var builder = new NpgsqlDataSourceBuilder(options.ConnectionString);
        if (builder.ConnectionStringBuilder.KeepAlive == 0)
            builder.ConnectionStringBuilder.KeepAlive = options.KeepAliveSeconds;
        _dataSource = builder.Build();
    }

    public async ValueTask SendAsync<TMessage>(
        Topic<TMessage> topic, TMessage message, CancellationToken cancellationToken = default)
    {
        if (topic is null) throw new ArgumentNullException(nameof(topic));
        if (_disposed) throw new ObjectDisposedException(nameof(PostgresPubSub));

        var payload = Serialize(topic, message);
        var bytes = Encoding.UTF8.GetByteCount(payload);
        if (bytes > MaxPayloadBytes)
            throw new ArgumentException(
                $"The message for topic '{topic.Name}' is {bytes} bytes; Postgres notifications must be shorter than 8000 "
                + "bytes. Send an id and load the data in the subscriber instead.", nameof(message));

        await using var command = _dataSource.CreateCommand("SELECT pg_notify($1, $2)");
        command.Parameters.Add(new NpgsqlParameter { Value = topic.Name });
        command.Parameters.Add(new NpgsqlParameter { Value = payload });
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public ValueTask<ISourceStream<TMessage>> SubscribeAsync<TMessage>(
        Topic<TMessage> topic, CancellationToken cancellationToken = default) =>
        SubscribeAsync(topic, null, null, cancellationToken);

    public async ValueTask<ISourceStream<TMessage>> SubscribeAsync<TMessage>(
        Topic<TMessage> topic, int? bufferCapacity, TopicBufferFullMode? bufferFullMode,
        CancellationToken cancellationToken = default)
    {
        if (topic is null) throw new ArgumentNullException(nameof(topic));
        var capacity = bufferCapacity ?? _options.TopicBufferCapacity;
        if (capacity < 1) throw new ArgumentOutOfRangeException(nameof(bufferCapacity), "Must be at least 1.");

        Task listening;
        Subscription<TMessage> subscription;
        lock (_gate)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(PostgresPubSub));

            if (!_topics.TryGetValue(topic.Name, out var active))
            {
                active = new ActiveTopic(topic.Name);
                _topics.Add(topic.Name, active);
                _commands.Writer.TryWrite(new Command($"LISTEN {Quote(topic.Name)}", active.Listening));
            }

            subscription = new Subscription<TMessage>(this, active, capacity, bufferFullMode ?? _options.TopicBufferFullMode);
            active.Subscribers.Add(subscription);
            listening = active.Listening.Task;
            _loop ??= Task.Run(RunAsync);
        }
        WakeUp();

        try
        {
            // Return only once Postgres is listening, so the caller can load the current state afterwards safely.
            await listening.WaitAsync(cancellationToken);
        }
        catch
        {
            await subscription.DisposeAsync();
            throw;
        }
        return subscription;
    }

    public async ValueTask DisposeAsync()
    {
        List<ISubscriber> subscribers;
        lock (_gate)
        {
            if (_disposed) return;
            _disposed = true;
            subscribers = _topics.Values.SelectMany(topic => topic.Subscribers).ToList();
            foreach (var topic in _topics.Values) topic.Listening.TrySetCanceled();
            _topics.Clear();
        }

        _shutdown.Cancel();
        WakeUp();
        if (_loop is not null)
        {
            try { await _loop; }
            catch (Exception ex) { _log.LogDebug(ex, "Listener loop ended with an error during shutdown"); }
        }

        // Shutting down is not an error: streams just end.
        foreach (var subscriber in subscribers) subscriber.Complete();
        while (_commands.Reader.TryRead(out var command)) command.Done?.TrySetCanceled();

        await _dataSource.DisposeAsync();
        _shutdown.Dispose();
    }

    /// <summary>Number of topics with subscribers (LISTENed, or about to be). For tests.</summary>
    internal int ActiveTopicCount
    {
        get { lock (_gate) return _topics.Count; }
    }

    // The single listening connection: runs queued LISTEN/UNLISTEN statements, otherwise waits for notifications.
    // If the connection fails, every subscriber gets TopicConnectionLostException and the loop reconnects.
    private async Task RunAsync()
    {
        var shutdown = _shutdown.Token;
        while (!shutdown.IsCancellationRequested)
        {
            Command? running = null;
            try
            {
                await using var connection = await _dataSource.OpenConnectionAsync(shutdown);
                connection.Notification += OnNotification;
                _log.LogDebug("LISTEN connection open");

                while (true)
                {
                    while (_commands.Reader.TryRead(out running))
                    {
                        await using (var sql = new NpgsqlCommand(running.Sql, connection))
                            await sql.ExecuteNonQueryAsync(shutdown);
                        running.Done?.TrySetResult(true);
                        running = null;
                    }

                    using var wakeUp = CancellationTokenSource.CreateLinkedTokenSource(shutdown);
                    Interlocked.Exchange(ref _wakeUp, wakeUp);
                    try
                    {
                        // A command queued before _wakeUp was set would not cancel the wait; check once more.
                        if (_commands.Reader.TryPeek(out _)) continue;
                        await connection.WaitAsync(wakeUp.Token);
                    }
                    catch (OperationCanceledException) when (!shutdown.IsCancellationRequested)
                    {
                        // Woken up for new commands. Cancelling WaitAsync leaves the connection usable.
                    }
                    finally
                    {
                        Interlocked.Exchange(ref _wakeUp, null);
                    }
                }
            }
            catch (OperationCanceledException) when (shutdown.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _log.LogWarning(ex, "The Postgres LISTEN connection failed; open topic streams end with "
                    + "TopicConnectionLostException. Reconnecting in {Delay}.", _options.ReconnectDelay);
                var error = new TopicConnectionLostException(ex);
                running?.Done?.TrySetException(error);
                FailAll(error);

                try { await Task.Delay(_options.ReconnectDelay, shutdown); }
                catch (OperationCanceledException) { break; }
            }
        }
    }

    private void OnNotification(object sender, NpgsqlNotificationEventArgs e)
    {
        ISubscriber[] subscribers;
        lock (_gate)
        {
            if (!_topics.TryGetValue(e.Channel, out var active)) return;
            subscribers = active.Subscribers.ToArray();
        }
        foreach (var subscriber in subscribers) subscriber.Deliver(e.Payload);
    }

    private void Unsubscribe(ISubscriber subscriber, ActiveTopic active)
    {
        lock (_gate)
        {
            if (!active.Subscribers.Remove(subscriber) || active.Subscribers.Count > 0) return;
            if (!_topics.TryGetValue(active.Name, out var current) || !ReferenceEquals(current, active)) return;

            _topics.Remove(active.Name);
            if (!_disposed) _commands.Writer.TryWrite(new Command($"UNLISTEN {Quote(active.Name)}", null));
        }
        WakeUp();
    }

    private void FailAll(Exception error)
    {
        List<ISubscriber> subscribers;
        lock (_gate)
        {
            subscribers = _topics.Values.SelectMany(topic => topic.Subscribers).ToList();
            foreach (var topic in _topics.Values) topic.Listening.TrySetException(error);
            _topics.Clear(); // the next subscriber to any topic starts over with a fresh LISTEN
            while (_commands.Reader.TryRead(out var command)) command.Done?.TrySetException(error);
        }
        foreach (var subscriber in subscribers) subscriber.Fail(error);
    }

    private void WakeUp()
    {
        try { Volatile.Read(ref _wakeUp)?.Cancel(); }
        catch (ObjectDisposedException) { } // the loop already moved on
    }

    private string Serialize<TMessage>(Topic<TMessage> topic, TMessage message)
    {
        if (message is null)
            throw new ArgumentNullException(nameof(message), $"Can't send null to topic '{topic.Name}'.");
        return message is string text && typeof(TMessage) == typeof(string)
            ? text
            : JsonSerializer.Serialize(message, _options.JsonSerializerOptions);
    }

    // The payload comes from Postgres, possibly written by SQL, so this is where the type system ends: a payload that
    // isn't a valid TMessage is logged as an error and skipped.
    private bool TryDeserialize<TMessage>(string topic, string payload, out TMessage message)
    {
        if (typeof(TMessage) == typeof(string))
        {
            message = (TMessage)(object)payload;
            return true;
        }

        try
        {
            var value = JsonSerializer.Deserialize<TMessage>(payload, _options.JsonSerializerOptions);
            if (value is not null)
            {
                message = value;
                return true;
            }
            _log.LogError("Skipped a notification on topic {Topic}: the payload {Payload} is null, not a {Type}",
                topic, payload, typeof(TMessage).Name);
        }
        catch (JsonException ex)
        {
            _log.LogError(ex, "Skipped a notification on topic {Topic}: the payload {Payload} is not a valid {Type}",
                topic, payload, typeof(TMessage).Name);
        }

        message = default!;
        return false;
    }

    // Channel names are identifiers in LISTEN/UNLISTEN; quoting keeps them case-sensitive, like pg_notify's argument.
    private static string Quote(string name) => "\"" + name.Replace("\"", "\"\"") + "\"";

    private sealed record Command(string Sql, TaskCompletionSource<bool>? Done);

    private sealed class ActiveTopic(string name)
    {
        public string Name { get; } = name;
        public List<ISubscriber> Subscribers { get; } = new();
        public TaskCompletionSource<bool> Listening { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private interface ISubscriber
    {
        void Deliver(string payload);
        void Fail(Exception error);
        void Complete();
    }

    private sealed class Subscription<TMessage> : ISourceStream<TMessage>, ISubscriber
    {
        private readonly PostgresPubSub _owner;
        private readonly ActiveTopic _topic;
        private readonly Channel<TMessage> _buffer;
        private int _disposed;

        public Subscription(PostgresPubSub owner, ActiveTopic topic, int capacity, TopicBufferFullMode fullMode)
        {
            _owner = owner;
            _topic = topic;
            _buffer = Channel.CreateBounded<TMessage>(new BoundedChannelOptions(capacity)
            {
                FullMode = fullMode switch
                {
                    TopicBufferFullMode.DropNewest => BoundedChannelFullMode.DropNewest,
                    TopicBufferFullMode.DropWrite => BoundedChannelFullMode.DropWrite,
                    _ => BoundedChannelFullMode.DropOldest,
                },
                SingleReader = true,
            });
        }

        public IAsyncEnumerable<TMessage> ReadEventsAsync() => _buffer.Reader.ReadAllAsync();

        public void Deliver(string payload)
        {
            if (_owner.TryDeserialize<TMessage>(_topic.Name, payload, out var message))
                _buffer.Writer.TryWrite(message);
        }

        public void Fail(Exception error) => _buffer.Writer.TryComplete(error);

        public void Complete() => _buffer.Writer.TryComplete();

        public ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                _buffer.Writer.TryComplete();
                _owner.Unsubscribe(this, _topic);
            }
            return default;
        }
    }
}
