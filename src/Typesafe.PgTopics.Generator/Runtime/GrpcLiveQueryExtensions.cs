using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;

namespace Typesafe.PgTopics;

// Only generated when the project references Grpc.Core.Api (every gRPC server does, through Grpc.AspNetCore).
public static class GrpcLiveQueryExtensions
{
    /// <summary>
    /// Runs a live query (<see cref="LiveQueryExtensions.WatchAsync{TMessage, TEvent}"/>) on a gRPC server stream until
    /// the client leaves, so a Watch method only says how to load the snapshot and how to turn one message into an event:
    /// <code>
    /// public override Task WatchOrders(WatchOrdersRequest request, IServerStreamWriter&lt;OrderEvent&gt; stream,
    ///     ServerCallContext context) =>
    ///     stream.WatchAsync(receiver, Topics.OrderChanges, context,
    ///         snapshot: async ct => OrderEvent.FromSnapshot(await queries.ListOrdersAsync(ct)),
    ///         change: async (c, ct) => OrderEvent.FromChange(c.Id, await queries.GetOrderAsync(new(c.Id), ct)));
    /// </code>
    /// If the connection to Postgres is lost, the call fails (<see cref="TopicConnectionLostException"/>), and the
    /// client reconnects and gets a fresh snapshot.
    /// </summary>
    public static async Task WatchAsync<TMessage, TEvent>(
        this IServerStreamWriter<TEvent> stream,
        ITopicEventReceiver receiver,
        Topic<TMessage> topic,
        ServerCallContext context,
        Func<CancellationToken, Task<TEvent>> snapshot,
        Func<TMessage, CancellationToken, Task<TEvent?>> change)
        where TEvent : class
    {
        var cancellationToken = context.CancellationToken;
        await foreach (var @event in receiver.WatchAsync(topic, snapshot, change, cancellationToken).ConfigureAwait(false))
            await stream.WriteAsync(@event, cancellationToken).ConfigureAwait(false);
    }
}
