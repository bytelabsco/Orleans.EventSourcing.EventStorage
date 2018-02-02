namespace Orleans.EventSourcing.EventStorage
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Orleans.EventSourcing.Common;
    using Orleans.EventSourcing.EventStorage.States;
    using Orleans.LogConsistency;
    using Orleans.Storage;

    internal class LogViewAdaptor<TLogView, TLogEntry> : PrimaryBasedLogViewAdaptor<TLogView, TLogEntry, SubmissionEntry<TLogEntry>> where TLogView : class, new() where TLogEntry : class
    {
        private IStorageProvider _globalStorageProvider;
        private string _grainTypeName;
        private EventStorageOptions _config;

        // the confirmed view
        private TLogView _view;
        private int _confirmedVersion = 0;

        /// <summary>
        /// Initialize a LogViewAdaptor class
        /// </summary>
        public LogViewAdaptor(ILogViewAdaptorHost<TLogView, TLogEntry> host, TLogView initialState, IStorageProvider globalStorageProvider, string grainTypeName, ILogConsistencyProtocolServices services, EventStorageOptions config)
            : base(host, initialState, services)
        {
            _globalStorageProvider = globalStorageProvider;
            _grainTypeName = grainTypeName;
            _config = config;
        }

        protected override int GetConfirmedVersion()
        {
            return _confirmedVersion;
        }

        protected override void InitializeConfirmedView(TLogView initialState)
        {
            _view = initialState;
        }

        protected override TLogView LastConfirmedView()
        {
            return _view;
        }

        protected override SubmissionEntry<TLogEntry> MakeSubmissionEntry(TLogEntry entry)
        {
            return new SubmissionEntry<TLogEntry>() { Entry = entry };
        }

        protected override async Task ReadAsync()
        {
            var grainReference = Services.GrainReference;

            var baseId = grainReference.ToKeyString();

            if (_config.TakeSnapshots)
            {
                try
                {
                    // Look for a snapshot
                    var snapshot = new StreamSnapshot<TLogView>();
                    await _globalStorageProvider.ReadStateAsync(_grainTypeName, grainReference, snapshot);

                    var state = snapshot.StreamSnapshotState;
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

            var summary = new StreamSummary();
            await _globalStorageProvider.ReadStateAsync(nameof(StreamSummary), grainReference, summary);
            var currentVersion = summary.StreamSummaryState.CommitNumber;

            if (currentVersion > 0)
            {
                for (var i = startVersion; i <= currentVersion; i++)
                {
                    var decoratedGrainReference = new CustomIdGrainReferenceDecorator(grainReference, $"{grainReference.ToKeyString()}{EventStorageConstants.CommitVersionPrefix}{i}");
                    var grainCommit = new StreamCommit();
                    await _globalStorageProvider.ReadStateAsync(_grainTypeName, decoratedGrainReference, grainCommit);
                    var commitNumber = grainCommit.StreamCommitState.GlobalNumber;

                    var decoratedCommitReference = new CustomIdGrainReferenceDecorator(grainReference, commitNumber.ToString());
                    var commit = new Commit<TLogEntry>();
                    await _globalStorageProvider.ReadStateAsync(nameof(Commit<TLogEntry>), decoratedCommitReference, commit);
                    var events = commit.CommitState.Events;

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
        }

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

            // TODO - memory cache these in the adaptor?
            var globalSummary = new StreamSummary();
            var decoratedGlobalSummaryGrainReference = new CustomIdGrainReferenceDecorator(grainReference, EventStorageConstants.GlobalCommitStreamName);
            await _globalStorageProvider.ReadStateAsync(nameof(StreamSummary), decoratedGlobalSummaryGrainReference, globalSummary);

            var streamSummary = new StreamSummary();
            await _globalStorageProvider.ReadStateAsync(nameof(StreamSummary), grainReference, streamSummary);

            var globalWriteBit = globalSummary.StreamSummaryState.FlipBit(Services.MyClusterId);
            var streamWriteBit = streamSummary.StreamSummaryState.FlipBit(Services.MyClusterId);

            var newGlobalVersion = globalSummary.StreamSummaryState.CommitNumber++;
            var newStreamVersion = streamSummary.StreamSummaryState.CommitNumber++;

            var commit = new Commit<TLogEntry>();
            //var convertedEvents = events.Select(s => s as object).ToList();

            commit.CommitState = new CommitState<TLogEntry>()
            {
                Events = events,
                GlobalNumber = newGlobalVersion
            };

            var commitDecoratedGrainReference = new CustomIdGrainReferenceDecorator(grainReference, newGlobalVersion.ToString());
            await _globalStorageProvider.WriteStateAsync(nameof(Commit<TLogEntry>), commitDecoratedGrainReference, commit);


            
            var grainCommit = new StreamCommit();

            grainCommit.StreamCommitState = new StreamCommitState()
            {
                GlobalNumber = newGlobalVersion,
                Number = newStreamVersion
            };

            var grainCommitDecoreatedGrainReference = new CustomIdGrainReferenceDecorator(grainReference, $"{baseId}{EventStorageConstants.CommitVersionPrefix}{newStreamVersion}");
            await _globalStorageProvider.WriteStateAsync(nameof(StreamCommit), grainCommitDecoreatedGrainReference, grainCommit);

            foreach (var @event in events)
            {
                ApplyEventToView(@event);
            }

            _confirmedVersion = (int)newStreamVersion;

            if (_config.TakeSnapshots)
            {
                if (_confirmedVersion % _config.CommitsPerSnapshot == 0)
                {
                    // Take state snapshot
                    var snapshot = new StreamSnapshot<TLogView>();

                    snapshot.StreamSnapshotState = new StreamSnapshotState<TLogView>()
                    {
                        Snapshot = _view,
                        Version = _confirmedVersion
                    };

                    await _globalStorageProvider.WriteStateAsync(nameof(StreamSnapshot<TLogView>), grainReference, snapshot);
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
            //if (_config.StreamCommits)
            //{
            //    await commit.StreamEvents(_config.StreamProvider, _config.StreamName);
            //}

            // exit_operation("WriteAsync");

            if (!batchsuccessfullywritten)
            {
                return 0;
            }

            return updates.Length;
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

        /// <summary>
        /// A notification message sent to remote instances after updating this grain in storage.
        /// </summary>
        [Serializable]
        internal class UpdateNotificationMessage : INotificationMessage
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
    }
}