using System;
using Microsoft.Extensions.Logging;
using Typesafe.PgTopics;

// In the DI namespace, like other Add* methods, so no extra using is needed in Program.cs.
// Only generated when the project references Microsoft.Extensions.DependencyInjection (every ASP.NET Core app does).
namespace Microsoft.Extensions.DependencyInjection;

public static class PostgresTopicsServiceCollectionExtensions
{
    /// <summary>
    /// Registers <see cref="PostgresPubSub"/> as a singleton, and as <see cref="ITopicEventSender"/> and
    /// <see cref="ITopicEventReceiver"/>.
    /// </summary>
    public static IServiceCollection AddPostgresTopics(
        this IServiceCollection services, Action<PostgresTopicsOptions> configure) =>
        services.AddPostgresTopics((_, options) => configure(options));

    /// <inheritdoc cref="AddPostgresTopics(IServiceCollection, Action{PostgresTopicsOptions})"/>
    public static IServiceCollection AddPostgresTopics(
        this IServiceCollection services, Action<IServiceProvider, PostgresTopicsOptions> configure)
    {
        services.AddSingleton(provider =>
        {
            var options = new PostgresTopicsOptions();
            configure(provider, options);
            return new PostgresPubSub(options, provider.GetService<ILoggerFactory>());
        });
        services.AddSingleton<ITopicEventSender>(provider => provider.GetRequiredService<PostgresPubSub>());
        services.AddSingleton<ITopicEventReceiver>(provider => provider.GetRequiredService<PostgresPubSub>());
        return services;
    }
}
