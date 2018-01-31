namespace Orleans.EventSourcing.EventStorage
{
    using System;
    using Orleans.Hosting;
    using Orleans.Runtime.Configuration;

    public static class EventStorageExtensions
    {
        public static ISiloHostBuilder UseEventStorageLogProvider(this ISiloHostBuilder builder, Action<EventStorageOptions> options)
        {
            return builder.Configure(options);
        }

        public static void AddEventStorageLogProvider(this ClusterConfiguration config, string name)
        {
            config.Globals.RegisterLogConsistencyProvider<EventStorageLogConsistencyProvider>(name);
        }
    }
}
