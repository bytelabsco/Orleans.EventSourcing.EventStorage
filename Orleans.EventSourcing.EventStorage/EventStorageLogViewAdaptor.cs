namespace Orleans.EventSourcing.EventStorage
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;
    using Orleans;
    using Orleans.EventSourcing.Common;
    using Orleans.EventSourcing.EventStorage.Grains;
    using Orleans.LogConsistency;
    using Orleans.Storage;

    /// <summary>
    /// A log view adaptor that wraps around a traditional storage adaptor, and uses batching and e-tags
    /// to append entries.
    ///<para>
    /// The log itself is transient, i.e. not actually saved to storage - only the latest view and some 
    /// metadata (the log position, and write flags) are stored. 
    /// </para>
    /// </summary>
    /// <typeparam name="TLogView">Type of log view</typeparam>
    /// <typeparam name="TLogEntry">Type of log entry</typeparam>
    internal class LogViewAdaptor<TLogView, TLogEntry> : PrimaryBasedLogViewAdaptor<TLogView, TLogEntry, SubmissionEntry<TLogEntry>> where TLogView : class, new() where TLogEntry : class
    {
        /// <summary>
        /// Initialize a StorageProviderLogViewAdaptor class
        /// </summary>
        public LogViewAdaptor(ILogViewAdaptorHost<TLogView, TLogEntry> host, TLogView initialState, IStorageProvider globalStorageProvider, string grainTypeName, ILogConsistencyProtocolServices services, IGrainFactory grainFactory, EventStorageOptions config)
            : base(host, initialState, services)
        {
            _globalStorageProvider = globalStorageProvider;
            _grainTypeName = grainTypeName;
            _grainFactory = grainFactory;
            _config = config;
        }

        private const int maxEntriesInNotifications = 200;

        private IStorageProvider _globalStorageProvider;
        private string _grainTypeName;
        private IGrainFactory _grainFactory;
        private EventStorageOptions _config;

        // the confirmed view
        private TLogView _view;
        private int _confirmedVersion = 0;

        /// <inheritdoc/>
        protected override TLogView LastConfirmedView()
        {
            return _view;
        }

        /// <inheritdoc/>
        protected override int GetConfirmedVersion()
        {
            return _confirmedVersion;
        }

        /// <inheritdoc/>
        protected override void InitializeConfirmedView(TLogView initialState)
        {
            _view = initialState;
        }

        private void ApplyEventToView(TLogEntry @event)
        {
            try
            {
                Host.UpdateView(_view, @event);
            }
            catch (Exception e)
            {
                Services.CaughtUserCodeException("ApplyEventToView", nameof(ApplyEventToView), e);
            }
        }


        /// <inheritdoc/>
        public override async Task<IReadOnlyList<TLogEntry>> RetrieveLogSegment(int fromVersion, int toVersion)
        {
            // enter_operation("RetrieveLogSegment");

            var grainReference = Services.GrainReference;
            var baseId = grainReference.ToKeyString();

            var summary = _grainFactory.GetGrain<IStreamSummary>(baseId);
            var summaryVersion = await summary.GetCurrentVersion();

            if (toVersion > summaryVersion)
            {
                toVersion = summaryVersion;
            }

            // Load every commit, add it to the list
            var eventList = new List<TLogEntry>();

            for (var i = fromVersion; i <= toVersion; i++)
            {
                var grainCommit = _grainFactory.GetGrain<IStreamCommit>($"{baseId}{EventStorageConstants.CommitVersionPrefix}{i}");
                var commitNumber = await grainCommit.GetCommitNumber();

                var commit = _grainFactory.GetGrain<ICommit>(commitNumber);
                var events = await commit.GetEvents();

                if (events != null)
                {
                    foreach (var @event in events)
                    {
                        eventList.Add(@event as TLogEntry);
                    }
                }
            }

            // exit_operation("RetrieveLogSegment");

            return eventList.AsReadOnly();
        }

        // no special tagging is required, thus we create a plain submission entry
        /// <inheritdoc/>
        protected override SubmissionEntry<TLogEntry> MakeSubmissionEntry(TLogEntry entry)
        {
            return new SubmissionEntry<TLogEntry>() { Entry = entry };
        }

        /// <inheritdoc/>
        protected override async Task ReadAsync()
        {
            // enter_operation("ReadAsync");

            var grainReference = Services.GrainReference;

            var baseId = grainReference.ToKeyString();

            if (_config.TakeSnapshots)
            {
                try
                {
                    // Look for a snapshot
                    var snapshot = _grainFactory.GetGrain<IStreamSnapshot<TLogView>>(baseId);
                    var state = await snapshot.GetSnapshot();

                    if (state.Snapshot != null)
                    {
                        _view = state.Snapshot;
                        _confirmedVersion = state.Version;
                    }
                }
                catch (Exception e)
                {
                    // Something wrong with the snapshot.   Probably a different version of the state than what was persisted                
                }
            }

            var startVersion = _confirmedVersion + 1;

            var summary = _grainFactory.GetGrain<IStreamSummary>(baseId);
            var currentVersion = await summary.GetCurrentVersion();

            if (currentVersion > 0)
            {

                for (var i = startVersion; i <= currentVersion; i++)
                {
                    var grainCommit = _grainFactory.GetGrain<IStreamCommit>($"{baseId}{EventStorageConstants.CommitVersionPrefix}{i}");
                    var commitNumber = await grainCommit.GetCommitNumber();

                    var commit = _grainFactory.GetGrain<ICommit>(commitNumber);
                    var events = await commit.GetEvents();

                    if (events != null)
                    {
                        foreach (var @event in events)
                        {
                            ApplyEventToView(@event as TLogEntry);
                        }
                    }

                    _confirmedVersion = i;
                }
            }

            // exit_operation("ReadAsync");
        }

        /// <inheritdoc/>
        protected override async Task<int> WriteAsync()
        {
            // enter_operation("WriteAsync");

            string eTag = string.Empty;
            bool batchsuccessfullywritten = false;

            var grainReference = Services.GrainReference;

            var updates = GetCurrentBatchOfUpdates();
            var events = updates.Select(u => u.Entry)?.ToList();

            // Log the events for the grain, along with it's local (Grain instance specific) and global (all commits ever) numbers
            var baseId = grainReference.ToKeyString();

            var globalSummary = _grainFactory.GetGrain<IStreamSummary>(EventStorageConstants.GlobalCommitStreamName);
            var streamSummary = _grainFactory.GetGrain<IStreamSummary>(baseId);

            var newGlobalVersion = await globalSummary.BumpVersion();
            var newstreamVersion = await streamSummary.BumpVersion();

            var commit = _grainFactory.GetGrain<ICommit>(newGlobalVersion);
            var convertedEvents = events.Select(s => s as object).ToList();

            await commit.RecordEvents(convertedEvents);

            var grainCommit = _grainFactory.GetGrain<IStreamCommit>($"{baseId}{EventStorageConstants.CommitVersionPrefix}{newstreamVersion}");
            await grainCommit.SetCommitNumber(newGlobalVersion);

            foreach (var @event in events)
            {
                ApplyEventToView(@event);
            }

            _confirmedVersion = newstreamVersion;

            if (_config.TakeSnapshots)
            {
                if (_confirmedVersion % _config.CommitsPerSnapshot == 0)
                {
                    // Take state snapshot
                    var snapshot = _grainFactory.GetGrain<IStreamSnapshot<TLogView>>(baseId);
                    await snapshot.TakeSnapshot(_view, _confirmedVersion);
                }
            }

            batchsuccessfullywritten = true;

            // broadcast notifications to all other clusters
            if (batchsuccessfullywritten)
            {
                BroadcastNotification(new UpdateNotificationMessage()
                {
                    Version = _confirmedVersion,
                    Updates = updates.Select(se => se.Entry).ToList(),
                    Origin = Services.MyClusterId,
                    // ETag = eTag                    
                });
            }

            // Stream the events, if it's enabled
            if (_config.StreamCommits)
            {
                await commit.StreamEvents(_config.StreamProvider, _config.StreamName);
            }

            // exit_operation("WriteAsync");

            if (!batchsuccessfullywritten)
            {
                return 0;
            }

            return updates.Length;
        }

        /// <summary>
        /// Describes a connection issue that occurred when updating the primary storage.
        /// </summary>
        [Serializable]
        public class UpdateLogStorageFailed : PrimaryOperationFailed
        {
            /// <inheritdoc/>
            public override string ToString()
            {
                return $"write entire log to storage failed: caught {Exception.GetType().Name}: {Exception.Message}";
            }
        }


        /// <summary>
        /// Describes a connection issue that occurred when reading from the primary storage.
        /// </summary>
        [Serializable]
        public class ReadFromLogStorageFailed : PrimaryOperationFailed
        {
            /// <inheritdoc/>
            public override string ToString()
            {
                return $"read entire log from storage failed: caught {Exception.GetType().Name}: {Exception.Message}";
            }
        }


        /// <summary>
        /// A notification message sent to remote instances after updating this grain in storage.
        /// </summary>
        [Serializable]
        protected class UpdateNotificationMessage : INotificationMessage
        {
            /// <inheritdoc/>
            public int Version { get; set; }

            /// <summary> The cluster that performed the update </summary>
            public string Origin { get; set; }

            /// <summary> The list of updates that were applied </summary>
            public List<TLogEntry> Updates { get; set; }

            /// <inheritdoc/>
            public override string ToString()
            {
                return string.Format("v{0} ({1} updates by {2})", Version, Updates.Count, Origin);
            }
        }

        /// <inheritdoc/>
        protected override INotificationMessage Merge(INotificationMessage earlierMessage, INotificationMessage laterMessage)
        {
            var earlier = earlierMessage as UpdateNotificationMessage;
            var later = laterMessage as UpdateNotificationMessage;

            if (earlier != null
                && later != null
                && earlier.Origin == later.Origin
                && earlier.Version + later.Updates.Count == later.Version
                && earlier.Updates.Count + later.Updates.Count < maxEntriesInNotifications)

                return new UpdateNotificationMessage()
                {
                    Version = later.Version,
                    Origin = later.Origin,
                    Updates = earlier.Updates.Concat(later.Updates).ToList()
                };

            else
                return base.Merge(earlierMessage, laterMessage); // keep only the version number
        }

        private SortedList<long, UpdateNotificationMessage> notifications = new SortedList<long, UpdateNotificationMessage>();

        /// <inheritdoc/>
        protected override void OnNotificationReceived(INotificationMessage payload)
        {
            var um = payload as UpdateNotificationMessage;
            if (um != null)
            {
                notifications.Add(um.Version - um.Updates.Count, um);
            }
            else
            {
                base.OnNotificationReceived(payload);
            }
        }

        /// <inheritdoc/>
        protected override void ProcessNotifications()
        {
            var orderedNotifications = notifications.OrderBy(n => n.Key).ToList();

            // discard notifications that are behind our already confirmed state
            while (orderedNotifications.Count > 0 && orderedNotifications.ElementAt(0).Key < _confirmedVersion)
            {
                Services.Log(LogLevel.Information, "discarding notification {0}", notifications.ElementAt(0).Value);
                notifications.RemoveAt(0);
            }

            // process notifications that reflect next global version
            while (orderedNotifications.Count > 0)
            {
                var updateNotification = notifications.ElementAt(0).Value;
                notifications.RemoveAt(0);

                // append all operations in pending 
                foreach (var u in updateNotification.Updates)
                {
                    ApplyEventToView(u);
                }

                _confirmedVersion = updateNotification.Version;
                Services.Log(LogLevel.Information, "notification success ({0} updates)", updateNotification.Updates.Count);
            }

            Services.Log(LogLevel.Information, "unprocessed notifications in queue: {0}", notifications.Count);

            base.ProcessNotifications();
        }
    }
}
