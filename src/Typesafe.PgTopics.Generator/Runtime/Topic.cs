using System;
using System.Text;

namespace Typesafe.PgTopics;

/// <summary>
/// A Postgres notification channel together with the type of the messages on it. Declare every topic once, and send
/// and subscribe only through that declaration, so the compiler checks that both sides agree on the message type:
/// <code>
/// public static class Topics
/// {
///     public static readonly Topic&lt;OrderChanged&gt; OrderChanges = new("order_changes");
///     public static Topic&lt;OrderChanged&gt; Order(long id) => new($"order:{id}");   // one topic per order
/// }
/// </code>
/// The name is the Postgres channel, so SQL can publish to it too: <c>pg_notify('order_changes', json)</c>.
/// Names are case-sensitive and at most 63 bytes (the Postgres limit for channel names).
/// </summary>
public sealed class Topic<TMessage>
{
    public Topic(string name)
    {
        if (string.IsNullOrEmpty(name))
            throw new ArgumentException("A topic needs a name.", nameof(name));
        if (Encoding.UTF8.GetByteCount(name) > 63)
            throw new ArgumentException(
                $"The topic name '{name}' is longer than 63 bytes, the Postgres limit for channel names.", nameof(name));

        Name = name;
    }

    /// <summary>The Postgres channel name.</summary>
    public string Name { get; }

    public override string ToString() => Name;
}
