using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Typesafe.PgTopics;

/// <summary>Publishes messages to a topic (Postgres NOTIFY).</summary>
public interface ITopicEventSender
{
    /// <summary>
    /// Sends <paramref name="message"/> to everyone subscribed to <paramref name="topic"/>, in every process connected
    /// to the database. The message is sent as JSON (a <c>Topic&lt;string&gt;</c> sends the string as is) and must be
    /// shorter than 8000 bytes, the Postgres limit for notifications.
    /// </summary>
    ValueTask SendAsync<TMessage>(Topic<TMessage> topic, TMessage message, CancellationToken cancellationToken = default);
}

/// <summary>Subscribes to topics (Postgres LISTEN).</summary>
public interface ITopicEventReceiver
{
    /// <summary>
    /// Subscribes to <paramref name="topic"/>. When this returns, the subscription is active in Postgres: every message
    /// committed from then on is delivered. So subscribe first and load the current state second, and nothing slips
    /// through in between. Dispose the stream to unsubscribe.
    /// </summary>
    ValueTask<ISourceStream<TMessage>> SubscribeAsync<TMessage>(
        Topic<TMessage> topic, CancellationToken cancellationToken = default);

    /// <inheritdoc cref="SubscribeAsync{TMessage}(Topic{TMessage}, CancellationToken)"/>
    /// <param name="bufferCapacity">How many messages may wait for a slow reader (default: the options).</param>
    /// <param name="bufferFullMode">What happens when the buffer is full (default: the options).</param>
    ValueTask<ISourceStream<TMessage>> SubscribeAsync<TMessage>(
        Topic<TMessage> topic, int? bufferCapacity, TopicBufferFullMode? bufferFullMode,
        CancellationToken cancellationToken = default);
}

/// <summary>One subscription to a topic. Dispose it to unsubscribe.</summary>
public interface ISourceStream<out TMessage> : IAsyncDisposable
{
    /// <summary>
    /// The messages, as they arrive. Ends when the stream is disposed or the app shuts down. Throws
    /// <see cref="TopicConnectionLostException"/> if the connection to Postgres is lost: messages may have been missed,
    /// so subscribe again and reload the current state. Use <c>.WithCancellation(token)</c> to stop reading.
    /// </summary>
    IAsyncEnumerable<TMessage> ReadEventsAsync();
}

/// <summary>What happens when a subscriber reads slower than messages arrive and its buffer is full.</summary>
public enum TopicBufferFullMode
{
    /// <summary>Drop the oldest waiting message to make room.</summary>
    DropOldest,

    /// <summary>Drop the newest waiting message to make room.</summary>
    DropNewest,

    /// <summary>Drop the message that is arriving.</summary>
    DropWrite,
}

/// <summary>
/// The connection Postgres notifications arrive on was lost (or could not be opened). Notifications sent in the
/// meantime are not delivered, so every open stream ends with this exception: subscribe again and reload the state.
/// </summary>
public sealed class TopicConnectionLostException(Exception innerException)
    : Exception("The Postgres connection for topic notifications was lost; messages may have been missed. "
        + "Subscribe again and reload the current state.", innerException);
