namespace Orleans.EventSourcing.EventStorage
{
    using Orleans;
    using Orleans.LogConsistency;
    using Orleans.Providers;
    using Orleans.Runtime;
    using Orleans.Storage;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// A log-consistency provider that stores individual events and snapshots of those events, using any standard storage provider.
    /// </summary>
    public class EventStorageLogConsistencyProvider : ILogConsistencyProvider
    {
        private static int counter; // used for constructing a unique id
        private int id;
        private IGrainFactory _grainFactory;
        private EventStorageOptions _config;

        /// <inheritdoc/>
        public Logger Log { get; private set; }

        /// <inheritdoc/>
        public string Name { get; private set; }

        /// <inheritdoc/>
        public bool UsesStorageProvider { get { return true; } }

        /// <summary>
        /// Init method
        /// </summary>
        /// <param name="name">Consistency Provider Name</param>
        /// <param name="providerRuntime">Provider runtime</param>
        /// <param name="config">Provider config</param>
        /// <returns></returns>
        public Task Init(string name, IProviderRuntime providerRuntime, IProviderConfiguration config)
        {
            Name = name;
            id = Interlocked.Increment(ref counter); // unique id for this provider; matters only for tracing

            Log = providerRuntime.GetLogger(GetLoggerName());
            Log.Info($"Init (Severity{Log.SeverityLevel}");

            _grainFactory = providerRuntime.GrainFactory;
            _config = EventStorageOptions.FromDictionary(config.Properties);

            return Task.CompletedTask;
        }

        /// <summary>
        /// Close method
        /// </summary>
        /// <returns></returns>
        public Task Close()
        {
            return Task.CompletedTask;
        }

        /// <summary>
        /// Make log view adaptor 
        /// </summary>
        /// <typeparam name="TView">The type of the view</typeparam>
        /// <typeparam name="TEntry">The type of the log entries</typeparam>
        /// <param name="hostGrain">The grain that is hosting this adaptor</param>
        /// <param name="initialState">The initial state for this view</param>
        /// <param name="grainTypeName">The type name of the grain</param>
        /// <param name="storageProvider">Storage provider</param>
        /// <param name="services">Runtime services for multi-cluster coherence protocols</param>
        public ILogViewAdaptor<TLogView, TLogEntry> MakeLogViewAdaptor<TLogView, TLogEntry>(ILogViewAdaptorHost<TLogView, TLogEntry> hostGrain, TLogView initialState, string grainTypeName, IStorageProvider storageProvider, ILogConsistencyProtocolServices services)
            where TLogView : class, new()
            where TLogEntry : class
        {
            return new EventStorageLogViewAdaptor<TLogView, TLogEntry>(hostGrain, initialState, storageProvider, grainTypeName, services, _grainFactory, _config);
        }

        /// <inheritdoc/>
        protected virtual string GetLoggerName()
        {
            return string.Format($"LogViews.{GetType().Name}.{id}");
        }
    }
}
