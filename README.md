# Typesafe.PgTopics

Typed publish/subscribe on Postgres `LISTEN`/`NOTIFY`, with the API of Hot Chocolate's subscriptions
(`ITopicEventSender`, `ITopicEventReceiver`, `ISourceStream<T>`) but without GraphQL. Use it to feed gRPC server
streams, SSE or WebSockets from database changes.

Each topic is declared once with its message type, and every send and subscribe goes through that declaration, so a
typo in a topic name or a mismatch between sender and subscriber breaks the build instead of failing silently.

```csharp
public static class Topics
{
    public static readonly Topic<OrderChanged> OrderChanges = new("order_changes");
}

await sender.SendAsync(Topics.OrderChanges, new OrderChanged("INSERT", 42));

await using var stream = await receiver.SubscribeAsync(Topics.OrderChanges, ct);
await foreach (var change in stream.ReadEventsAsync().WithCancellation(ct))   // change is an OrderChanged
    Console.WriteLine(change.Id);
```

A topic is a plain Postgres channel and the message is plain JSON, so SQL can publish too, for example from a trigger:

```sql
PERFORM pg_notify('order_changes', json_build_object('op', TG_OP, 'id', NEW.id)::text);
```

## Setup

### 1. Add this repository as a git submodule

From the root of your repository:

```bash
git submodule add https://github.com/uldahlalex/Typesafe.PgTopics external/Typesafe.PgTopics
```

### 2. Reference it from your `.csproj`

```bash
dotnet add package Npgsql
```

Then add this inside `<Project>`, with the path from your `.csproj` to the submodule:

```xml
<Import Project="../external/Typesafe.PgTopics/build/Typesafe.PgTopics.targets" />
```

The generator is built from the submodule as part of your build. It adds the library to your project as source code
(visible in the IDE under Dependencies > Analyzers, and you can step into it while debugging). It compiles against
your own Npgsql, so there is no version to keep in sync. Without Npgsql the build fails with `PGTOPICS001`.

Add the `Import` only to the project that uses the topics, not also to projects that reference it (like a test
project): both would then contain the same public types.

### 3. Register it in `Program.cs`

```csharp
builder.Services.AddPostgresTopics(options =>
    options.ConnectionString = builder.Configuration.GetConnectionString("Postgres")!);
```

That registers one `PostgresPubSub` as `ITopicEventSender` and `ITopicEventReceiver`. Outside dependency injection,
`new PostgresPubSub(options)` works the same; dispose it when the app stops.

### 4. Declare topics and messages

```csharp
using Typesafe.PgTopics;

public sealed record OrderChanged(string Op, long Id);

public static class Topics
{
    public static readonly Topic<OrderChanged> OrderChanges = new("order_changes");
    public static Topic<OrderChanged> Order(long id) => new($"order:{id}");   // a topic per order
    public static readonly Topic<string> Announcements = new("announcements"); // raw text, no JSON
}
```

Topic names are Postgres channel names: case-sensitive, at most 63 bytes.

### 5. Subscribe, then load the current state

```csharp
public override async Task WatchOrders(WatchOrdersRequest request, IServerStreamWriter<OrderEvent> response,
    ServerCallContext context)
{
    var ct = context.CancellationToken;
    await using var changes = await receiver.SubscribeAsync(Topics.OrderChanges, ct);

    await response.WriteAsync(await LoadSnapshotAsync(ct), ct);       // after subscribing: nothing is missed

    await foreach (var change in changes.ReadEventsAsync().WithCancellation(ct))
        await response.WriteAsync(await LoadChangeAsync(change, ct), ct);
}
```

`SubscribeAsync` returns once Postgres is listening, so everything committed after that is delivered. Load the
current state after subscribing, not before.

## What is checked when

| Mistake | Caught |
|---|---|
| Typo in a topic name | Build: topics are fields, not strings |
| Sending the wrong message type to a topic | Build: `CS0411` ("type arguments cannot be inferred") on `SendAsync` |
| Reading a stream as the wrong type | Build: `ISourceStream<T>` has the topic's type |
| Topic name too long or empty | Startup, when the `Topic<T>` is created |
| Message of 8000 bytes or more | `SendAsync` throws, before anything is sent |
| JSON from SQL that doesn't match the type (missing property, `null` in a non-nullable property, not JSON) | At runtime: logged as an error and skipped. SQL is outside the C# type system, so this is as early as it gets |

The strict JSON checks (`RespectRequiredConstructorParameters`, `RespectNullableAnnotations`) are on by default on
.NET 9 and later. Set `options.JsonSerializerOptions` to change how messages are (de)serialized.

## Behaviour

- **One connection per app** listens on every topic that has subscribers: it `LISTEN`s when the first subscriber of a
  topic arrives and `UNLISTEN`s when the last one leaves. Sending uses the normal connection pool.
- **Every subscriber has its own buffer** (`TopicBufferCapacity`, default 64), so a slow reader never holds up the
  others. When it is full, the oldest message is dropped (`TopicBufferFullMode`, per subscription or in the options).
- **NOTIFY is not durable.** If the listening connection is lost, notifications sent in the meantime are gone. So
  every open stream ends with `TopicConnectionLostException`, and the library reconnects for the next subscriber:
  subscribe again and reload the state (a gRPC client that reconnects and gets a fresh snapshot does exactly that).
- **Delivered on commit.** Postgres sends a notification when the transaction commits, and never for a rolled-back one.
  A `SendAsync` runs in its own implicit transaction; to tie a message to your own transaction, `pg_notify` in SQL
  inside it (or in a trigger).
- **Keepalive** (default 30 s, unless the connection string sets `Keepalive`) makes a silently dropped listening
  connection fail instead of waiting forever.
- Disposing `PostgresPubSub` (DI does it at shutdown) ends every open stream normally.

## Compared with Hot Chocolate

| Hot Chocolate | Typesafe.PgTopics |
|---|---|
| `SendAsync<T>(string topic, T message)` | `SendAsync(Topic<T> topic, T message)` |
| `SubscribeAsync<T>(string topic)` | `SubscribeAsync(Topic<T> topic)` |
| `ISourceStream<T>.ReadEventsAsync()` | the same |
| `AddPostgresSubscriptions(...)` | `AddPostgresTopics(...)` |
| All topics share one channel; the topic name is inside an envelope in the payload | One Postgres channel per topic; the payload is just the JSON, so SQL can publish |
| In-memory, Redis, NATS and Postgres providers | Postgres only |
| `CompleteAsync(topic)` | Not available: dispose the stream instead |

## Development

The tests are integration tests against a real Postgres (LISTEN/NOTIFY needs no tables, so any database and role do):

```bash
PGTOPICS_TEST_CONNECTION="Host=localhost;Port=5432;Database=postgres;Username=postgres;Password=postgres" \
  dotnet test
```

The library itself is `src/Typesafe.PgTopics.Generator/Runtime/*.cs`. Those files are embedded in the generator and
not compiled there; the test project compiles them through the generator, exactly like an app does.

## Versions

Your repository records the exact commit of the submodule, so every build uses the version you committed.

| To | Run |
|---|---|
| Update to the latest commit | `git submodule update --remote external/Typesafe.PgTopics` |
| Pin a tag or commit | `git -C external/Typesafe.PgTopics checkout v1.1` (or a commit hash) |
| Follow another branch | `git submodule set-branch -b dev external/Typesafe.PgTopics`, then update |

Then commit the submodule change in your repository.

Clones and CI need the submodule too: `git clone --recurse-submodules`, or `git submodule update --init` in an
existing clone (in GitHub Actions: `actions/checkout` with `submodules: true`). A Docker build must copy the
`external/` folder along with the project.

Versions are git tags (`git tag v1.1 && git push --tags`); there is no release step, so run the tests (see
Development) before tagging.
