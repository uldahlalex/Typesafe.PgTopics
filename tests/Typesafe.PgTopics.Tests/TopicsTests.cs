using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Typesafe.PgTopics.Tests;

public sealed record OrderChanged(string Op, long Id);

// Integration tests against a real Postgres. Connection: PGTOPICS_TEST_CONNECTION, or the default below (the postgres
// service in .github/workflows/release.yml). Every test uses its own topic names, and LISTEN/NOTIFY needs no tables,
// so any database and any role work.
public sealed class TopicsTests : IAsyncLifetime
{
    private static readonly string ConnectionString =
        Environment.GetEnvironmentVariable("PGTOPICS_TEST_CONNECTION")
        ?? "Host=localhost;Port=5432;Database=postgres;Username=postgres;Password=postgres";

    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(5);

    // Identifies this test's connections in pg_stat_activity (to kill the LISTEN connection on purpose).
    private readonly string _applicationName = "pgtopics-test-" + Guid.NewGuid().ToString("N")[..8];
    private readonly TestLogger _log = new();
    private PostgresPubSub _pubSub = null!;

    public Task InitializeAsync()
    {
        _pubSub = new PostgresPubSub(new PostgresTopicsOptions
        {
            ConnectionString = $"{ConnectionString};Application Name={_applicationName}",
            ReconnectDelay = TimeSpan.FromMilliseconds(200),
        }, _log.Factory);
        return Task.CompletedTask;
    }

    public async Task DisposeAsync() => await _pubSub.DisposeAsync();

    private static Topic<T> NewTopic<T>(string prefix = "t") => new($"{prefix}_{Guid.NewGuid():N}");

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
    public async Task Sent_message_arrives_typed()
    {
        var topic = NewTopic<OrderChanged>();
        await using var stream = await _pubSub.SubscribeAsync(topic);
        var events = stream.ReadEventsAsync().GetAsyncEnumerator();

        await _pubSub.SendAsync(topic, new OrderChanged("INSERT", 42));

        Assert.Equal(new OrderChanged("INSERT", 42), await Next(events));
    }

    [Fact]
    public async Task Json_from_sql_pg_notify_arrives_typed()
    {
        var topic = NewTopic<OrderChanged>();
        await using var stream = await _pubSub.SubscribeAsync(topic);
        var events = stream.ReadEventsAsync().GetAsyncEnumerator();

        // What a trigger sends: json_build_object('op', TG_OP, 'id', NEW.id)
        await NotifyFromSql(topic.Name, """{"op": "UPDATE", "id": 7}""");

        Assert.Equal(new OrderChanged("UPDATE", 7), await Next(events));
    }

    [Fact]
    public async Task Every_subscriber_gets_every_message()
    {
        var topic = NewTopic<OrderChanged>();
        await using var first = await _pubSub.SubscribeAsync(topic);
        await using var second = await _pubSub.SubscribeAsync(topic);
        var a = first.ReadEventsAsync().GetAsyncEnumerator();
        var b = second.ReadEventsAsync().GetAsyncEnumerator();

        await _pubSub.SendAsync(topic, new OrderChanged("INSERT", 1));
        await _pubSub.SendAsync(topic, new OrderChanged("INSERT", 2));

        Assert.Equal(new[] { 1L, 2L }, new[] { (await Next(a)).Id, (await Next(a)).Id });
        Assert.Equal(new[] { 1L, 2L }, new[] { (await Next(b)).Id, (await Next(b)).Id });
    }

    [Fact]
    public async Task Topics_are_separate_and_case_sensitive()
    {
        var suffix = Guid.NewGuid().ToString("N");
        var upper = new Topic<string>($"Orders_{suffix}");
        var lower = new Topic<string>($"orders_{suffix}");
        await using var upperStream = await _pubSub.SubscribeAsync(upper);
        await using var lowerStream = await _pubSub.SubscribeAsync(lower);
        var upperEvents = upperStream.ReadEventsAsync().GetAsyncEnumerator();
        var lowerEvents = lowerStream.ReadEventsAsync().GetAsyncEnumerator();

        await _pubSub.SendAsync(upper, "to upper");
        await _pubSub.SendAsync(lower, "to lower");

        Assert.Equal("to upper", await Next(upperEvents));
        Assert.Equal("to lower", await Next(lowerEvents));
    }

    [Fact]
    public async Task String_topic_carries_raw_text()
    {
        var topic = NewTopic<string>();
        await using var stream = await _pubSub.SubscribeAsync(topic);
        var events = stream.ReadEventsAsync().GetAsyncEnumerator();

        await NotifyFromSql(topic.Name, "plain text, not JSON");

        Assert.Equal("plain text, not JSON", await Next(events));
    }

    [Fact]
    public async Task Payload_that_does_not_match_the_type_is_logged_and_skipped()
    {
        var topic = NewTopic<OrderChanged>();
        await using var stream = await _pubSub.SubscribeAsync(topic);
        var events = stream.ReadEventsAsync().GetAsyncEnumerator();

        await NotifyFromSql(topic.Name, """{"op": "INSERT"}""");             // id missing
        await NotifyFromSql(topic.Name, """{"op": null, "id": 1}""");        // null in a non-nullable string
        await NotifyFromSql(topic.Name, "not json");
        await NotifyFromSql(topic.Name, """{"op": "INSERT", "id": 3}""");

        Assert.Equal(new OrderChanged("INSERT", 3), await Next(events));
        Assert.Equal(3, _log.Entries.Count(e => e.Level == LogLevel.Error && e.Message.Contains(topic.Name)));
    }

    [Fact]
    public async Task Topic_is_unlistened_when_the_last_subscriber_leaves()
    {
        var topic = NewTopic<OrderChanged>();
        var first = await _pubSub.SubscribeAsync(topic);
        var second = await _pubSub.SubscribeAsync(topic);
        Assert.Equal(1, _pubSub.ActiveTopicCount);

        await first.DisposeAsync();
        Assert.Equal(1, _pubSub.ActiveTopicCount);

        await second.DisposeAsync();
        Assert.Equal(0, _pubSub.ActiveTopicCount);

        // Subscribing again after the UNLISTEN works.
        await using var third = await _pubSub.SubscribeAsync(topic);
        var events = third.ReadEventsAsync().GetAsyncEnumerator();
        await _pubSub.SendAsync(topic, new OrderChanged("INSERT", 9));
        Assert.Equal(9, (await Next(events)).Id);
    }

    [Fact]
    public async Task Many_topics_subscribed_concurrently_all_work()
    {
        var topics = Enumerable.Range(0, 25).Select(_ => NewTopic<OrderChanged>()).ToArray();
        var streams = await Task.WhenAll(topics.Select(topic => _pubSub.SubscribeAsync(topic).AsTask()));
        try
        {
            for (var i = 0; i < topics.Length; i++)
                await _pubSub.SendAsync(topics[i], new OrderChanged("INSERT", i));

            for (var i = 0; i < topics.Length; i++)
                Assert.Equal(i, (await Next(streams[i].ReadEventsAsync().GetAsyncEnumerator())).Id);
        }
        finally
        {
            foreach (var stream in streams) await stream.DisposeAsync();
        }
    }

    [Fact]
    public async Task Slow_subscriber_keeps_the_newest_messages_by_default()
    {
        var topic = NewTopic<OrderChanged>();
        await using var stream = await _pubSub.SubscribeAsync(topic, bufferCapacity: 2, bufferFullMode: null);

        for (var i = 1; i <= 5; i++)
            await _pubSub.SendAsync(topic, new OrderChanged("INSERT", i));
        await Task.Delay(300); // let all five arrive before reading

        var events = stream.ReadEventsAsync().GetAsyncEnumerator();
        Assert.Equal(new[] { 4L, 5L }, new[] { (await Next(events)).Id, (await Next(events)).Id });
    }

    [Fact]
    public async Task Too_large_message_is_rejected_before_sending()
    {
        var topic = NewTopic<string>();
        var error = await Assert.ThrowsAsync<ArgumentException>(
            () => _pubSub.SendAsync(topic, new string('x', 8000)).AsTask());
        Assert.Contains("8000", error.Message);
    }

    [Fact]
    public void Topic_name_longer_than_63_bytes_is_rejected()
    {
        Assert.Throws<ArgumentException>(() => new Topic<string>(new string('x', 64)));
        Assert.Throws<ArgumentException>(() => new Topic<string>(""));
        _ = new Topic<string>(new string('x', 63));
    }

    [Fact]
    public async Task Lost_connection_ends_streams_with_an_error_and_resubscribing_works()
    {
        var topic = NewTopic<OrderChanged>();
        var stream = await _pubSub.SubscribeAsync(topic);
        var events = stream.ReadEventsAsync().GetAsyncEnumerator();

        await using (var admin = new NpgsqlConnection(ConnectionString))
        {
            await admin.OpenAsync();
            await using var kill = new NpgsqlCommand(
                "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity "
                + "WHERE application_name = @app AND query LIKE '%LISTEN%'", admin);
            kill.Parameters.AddWithValue("app", _applicationName);
            Assert.Equal(1L, await kill.ExecuteScalarAsync());
        }

        await Assert.ThrowsAsync<TopicConnectionLostException>(() => Next(events));
        await stream.DisposeAsync();

        await using var again = await _pubSub.SubscribeAsync(topic).AsTask().WaitAsync(Timeout);
        var newEvents = again.ReadEventsAsync().GetAsyncEnumerator();
        await _pubSub.SendAsync(topic, new OrderChanged("INSERT", 5));
        Assert.Equal(5, (await Next(newEvents)).Id);
    }

    [Fact]
    public async Task Dispose_ends_open_streams_without_an_error()
    {
        var topic = NewTopic<OrderChanged>();
        var stream = await _pubSub.SubscribeAsync(topic);
        var events = stream.ReadEventsAsync().GetAsyncEnumerator();

        await _pubSub.DisposeAsync();

        Assert.False(await events.MoveNextAsync().AsTask().WaitAsync(Timeout));
    }

    [Fact]
    public async Task AddPostgresTopics_registers_one_instance_as_sender_and_receiver()
    {
        var services = new ServiceCollection()
            .AddLogging()
            .AddPostgresTopics(options => options.ConnectionString = ConnectionString);
        await using var provider = services.BuildServiceProvider();

        var sender = provider.GetRequiredService<ITopicEventSender>();
        var receiver = provider.GetRequiredService<ITopicEventReceiver>();
        Assert.Same(provider.GetRequiredService<PostgresPubSub>(), sender);
        Assert.Same(sender, receiver);

        var topic = NewTopic<OrderChanged>();
        await using var stream = await receiver.SubscribeAsync(topic);
        var events = stream.ReadEventsAsync().GetAsyncEnumerator();
        await sender.SendAsync(topic, new OrderChanged("INSERT", 11));
        Assert.Equal(11, (await Next(events)).Id);
    }
}
