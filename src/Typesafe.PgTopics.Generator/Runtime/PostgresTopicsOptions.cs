using System;
using System.Text.Json;

namespace Typesafe.PgTopics;

public sealed class PostgresTopicsOptions
{
    /// <summary>Required. Npgsql connection string of the database the topics live in.</summary>
    public string ConnectionString { get; set; } = "";

    /// <summary>How many messages may wait for a slow subscriber. Default 64.</summary>
    public int TopicBufferCapacity { get; set; } = 64;

    /// <summary>What happens when a subscriber's buffer is full. Default: drop the oldest message.</summary>
    public TopicBufferFullMode TopicBufferFullMode { get; set; } = TopicBufferFullMode.DropOldest;

    /// <summary>
    /// How messages are (de)serialized. Default: web defaults (camelCase, case-insensitive), and on .NET 9+ strict about
    /// missing constructor parameters and nulls in non-nullable properties, so JSON from SQL that doesn't match the
    /// message type is rejected (logged and skipped) instead of arriving with default values.
    /// </summary>
    public JsonSerializerOptions JsonSerializerOptions { get; set; } = new(JsonSerializerDefaults.Web)
    {
#if NET9_0_OR_GREATER
        RespectRequiredConstructorParameters = true,
        RespectNullableAnnotations = true,
#endif
    };

    /// <summary>Wait between attempts to reconnect the LISTEN connection. Default 2 seconds.</summary>
    public TimeSpan ReconnectDelay { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>
    /// Npgsql keepalive in seconds, used unless the connection string sets <c>Keepalive</c> itself. The LISTEN
    /// connection is idle most of the time; without keepalives a silently dropped connection is never noticed.
    /// Default 30.
    /// </summary>
    public int KeepAliveSeconds { get; set; } = 30;
}
