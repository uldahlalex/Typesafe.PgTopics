using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace Typesafe.PgTopics.Tests;

/// <summary>Collects log entries so tests can assert on them.</summary>
public sealed class TestLogger : ILoggerProvider, ILogger
{
    public ConcurrentQueue<(LogLevel Level, string Message)> Entries { get; } = new();

    public ILoggerFactory Factory => LoggerFactory.Create(builder => builder.SetMinimumLevel(LogLevel.Debug).AddProvider(this));

    public ILogger CreateLogger(string categoryName) => this;

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
        Func<TState, Exception?, string> formatter) => Entries.Enqueue((logLevel, formatter(state, exception)));

    public void Dispose() { }
}
