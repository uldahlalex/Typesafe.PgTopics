using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Typesafe.PgTopics;

/// <summary>
/// The message of a row-change trigger: only the id of the inserted, updated or deleted row. A live query reloads the
/// row when it handles the message, so one message type serves every table, a row that is gone means it was deleted,
/// and a late or out-of-order notification can never bring back old data. From SQL:
/// <code>PERFORM pg_notify('order_changes', json_build_object('id', NEW.id)::text);</code>
/// </summary>
public sealed record RowChanged<TId>(TId Id);

public static class LiveQueryExtensions
{
    /// <summary>
    /// A live query: subscribes to <paramref name="topic"/>, then yields the <paramref name="snapshot"/>, then one event
    /// per message from <paramref name="change"/> (return <c>null</c> to skip a message, e.g. a row this subscriber may
    /// not see), until <paramref name="cancellationToken"/> fires.
    /// <para>
    /// The snapshot is loaded after the subscription is active, so no change can fall between the two; a change that
    /// races with the snapshot may arrive twice, so clients should apply events as idempotent upserts. If the
    /// connection to Postgres is lost, the sequence throws <see cref="TopicConnectionLostException"/>: the caller
    /// starts over, and the fresh snapshot covers whatever was missed.
    /// </para>
    /// Works for any transport that writes a sequence: SSE, WebSockets, or gRPC (see GrpcLiveQueryExtensions).
    /// </summary>
    public static async IAsyncEnumerable<TEvent> WatchAsync<TMessage, TEvent>(
        this ITopicEventReceiver receiver,
        Topic<TMessage> topic,
        Func<CancellationToken, Task<TEvent>> snapshot,
        Func<TMessage, CancellationToken, Task<TEvent?>> change,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
        where TEvent : class
    {
        var changes = await receiver.SubscribeAsync(topic, cancellationToken).ConfigureAwait(false);
        await using (changes.ConfigureAwait(false))
        {
            yield return await snapshot(cancellationToken).ConfigureAwait(false);

            await foreach (var message in changes.ReadEventsAsync().WithCancellation(cancellationToken)
                               .ConfigureAwait(false))
            {
                if (await change(message, cancellationToken).ConfigureAwait(false) is { } @event)
                    yield return @event;
            }
        }
    }
}
