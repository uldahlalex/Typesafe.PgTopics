using Grpc.Core;
using Npgsql;

namespace Typesafe.PgTopics.Tests;

// Integration tests for the live-query helpers, against a real Postgres (same connection as TopicsTests).
public sealed class LiveQueryTests : IAsyncLifetime
{
    private static readonly string ConnectionString =
        Environment.GetEnvironmentVariable("PGTOPICS_TEST_CONNECTION")
        ?? "Host=localhost;Port=5432;Database=postgres;Username=postgres;Password=postgres";

    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(5);

    private readonly TestLogger _log = new();
    private PostgresPubSub _pubSub = null!;

    public Task InitializeAsync()
    {
        _pubSub = new PostgresPubSub(new PostgresTopicsOptions { ConnectionString = ConnectionString }, _log.Factory);
        return Task.CompletedTask;
    }

    public async Task DisposeAsync() => await _pubSub.DisposeAsync();

    private static Topic<T> NewTopic<T>() => new($"live_{Guid.NewGuid():N}");

    private static async Task<T> Next<T>(IAsyncEnumerator<T> stream)
    {
        if (!await stream.MoveNextAsync().AsTask().WaitAsync(Timeout))
            throw new InvalidOperationException("The stream ended.");
        return stream.Current;
    }

    private static async Task NotifyFromSql(string channel, string payload)
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand("SELECT pg_notify(@channel, @payload)", connection);
        command.Parameters.AddWithValue("channel", channel);
        command.Parameters.AddWithValue("payload", payload);
        await command.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task Snapshot_comes_first_then_mapped_changes_and_null_skips()
    {
        var topic = NewTopic<RowChanged<long>>();
        using var cts = new CancellationTokenSource();
        var events = _pubSub.WatchAsync(topic,
                snapshot: _ => Task.FromResult("snapshot"),
                change: (c, _) => Task.FromResult(c.Id == 2 ? null : $"row {c.Id}"),
                cts.Token)
            .GetAsyncEnumerator();

        Assert.Equal("snapshot", await Next(events));   // the subscription is active once the snapshot is out

        await _pubSub.SendAsync(topic, new RowChanged<long>(1));
        await _pubSub.SendAsync(topic, new RowChanged<long>(2));   // skipped
        await _pubSub.SendAsync(topic, new RowChanged<long>(3));
        Assert.Equal("row 1", await Next(events));
        Assert.Equal("row 3", await Next(events));

        await cts.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Next(events));
        await events.DisposeAsync();
    }

    [Fact]
    public async Task Change_committed_while_the_snapshot_loads_is_not_missed()
    {
        var topic = NewTopic<RowChanged<long>>();
        var events = _pubSub.WatchAsync(topic,
                snapshot: async _ =>
                {
                    // Subscribed before the snapshot, so this lands in the stream instead of falling in between.
                    await NotifyFromSql(topic.Name, """{"id": 7}""");
                    return "snapshot";
                },
                change: (c, _) => Task.FromResult<string?>($"row {c.Id}"))
            .GetAsyncEnumerator();

        Assert.Equal("snapshot", await Next(events));
        Assert.Equal("row 7", await Next(events));
        await events.DisposeAsync();
    }

    [Fact]
    public async Task RowChanged_reads_the_id_sent_by_a_trigger()
    {
        var longs = NewTopic<RowChanged<long>>();
        var guids = NewTopic<RowChanged<Guid>>();
        await using var longStream = await _pubSub.SubscribeAsync(longs);
        await using var guidStream = await _pubSub.SubscribeAsync(guids);
        var longEvents = longStream.ReadEventsAsync().GetAsyncEnumerator();
        var guidEvents = guidStream.ReadEventsAsync().GetAsyncEnumerator();
        var id = Guid.NewGuid();

        // What json_build_object('id', NEW.id) produces for bigint and uuid keys.
        await NotifyFromSql(longs.Name, """{"id": 42}""");
        await NotifyFromSql(guids.Name, $$"""{"id": "{{id}}"}""");

        Assert.Equal(42, (await Next(longEvents)).Id);
        Assert.Equal(id, (await Next(guidEvents)).Id);
    }

    [Fact]
    public async Task Grpc_stream_gets_the_snapshot_and_changes_until_the_client_leaves()
    {
        var topic = NewTopic<RowChanged<long>>();
        var writer = new RecordingStreamWriter();
        var context = new FakeServerCallContext();

        var call = writer.WatchAsync(_pubSub, topic, context,
            snapshot: _ => Task.FromResult("snapshot"),
            change: (c, _) => Task.FromResult<string?>($"row {c.Id}"));

        await writer.WaitForCount(1);   // the snapshot is out, so the subscription is active
        await _pubSub.SendAsync(topic, new RowChanged<long>(5));
        await writer.WaitForCount(2);
        Assert.Equal(["snapshot", "row 5"], writer.Messages);

        await context.Cancel();   // the client leaves
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => call.WaitAsync(Timeout));
    }

    private sealed class RecordingStreamWriter : IServerStreamWriter<string>
    {
        private readonly List<string> _messages = [];
        public List<string> Messages { get { lock (_messages) return [.. _messages]; } }
        public WriteOptions? WriteOptions { get; set; }

        public Task WriteAsync(string message)
        {
            lock (_messages) _messages.Add(message);
            return Task.CompletedTask;
        }

        // Grpc.Core.Api's default implementation throws for a cancellable token; ASP.NET Core's writer overrides it.
        public Task WriteAsync(string message, CancellationToken cancellationToken) => WriteAsync(message);

        public async Task WaitForCount(int count)
        {
            var deadline = DateTime.UtcNow + Timeout;
            while (Messages.Count < count)
            {
                if (DateTime.UtcNow > deadline) throw new TimeoutException($"Got {Messages.Count} of {count} messages.");
                await Task.Delay(20);
            }
        }
    }

    private sealed class FakeServerCallContext : ServerCallContext
    {
        private readonly CancellationTokenSource _cts = new();
        public Task Cancel() => _cts.CancelAsync();

        protected override CancellationToken CancellationTokenCore => _cts.Token;
        protected override string MethodCore => "/test.Live/Watch";
        protected override string HostCore => "localhost";
        protected override string PeerCore => "test";
        protected override DateTime DeadlineCore => DateTime.MaxValue;
        protected override Metadata RequestHeadersCore { get; } = [];
        protected override Metadata ResponseTrailersCore { get; } = [];
        protected override Status StatusCore { get; set; }
        protected override WriteOptions? WriteOptionsCore { get; set; }
        protected override AuthContext AuthContextCore { get; } = new(null, []);
        protected override Task WriteResponseHeadersAsyncCore(Metadata responseHeaders) => Task.CompletedTask;
        protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options) =>
            throw new NotSupportedException();
    }
}
