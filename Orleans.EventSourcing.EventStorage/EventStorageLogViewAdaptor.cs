namespace Orleans.EventSourcing.EventStorage
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;
    using Orleans.EventSourcing.Common;
    using Orleans.EventSourcing.EventStorage.Grains;
    using Orleans.EventSourcing.EventStorage.States;
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
    internal class EventStorageLogViewAdaptor<TLogView, TLogEntry> : PrimaryBasedLogViewAdaptor<TLogView, TLogEntry, SubmissionEntry<TLogEntry>> where TLogView : class, new() where TLogEntry : class
    {
        /// <summary>
        /// Initialize a StorageProviderLogViewAdaptor class
        /// </summary>
        public EventStorageLogViewAdaptor(ILogViewAdaptorHost<TLogView, TLogEntry> host, TLogView initialState, IStorageProvider globalStorageProvider, string grainTypeName, ILogConsistencyProtocolServices services, IGrainFactory grainFactory, EventStorageOptions config)
            : base(host, initialState, services)
        {
            _globalStorageProvider = globalStorageProvider;
            _grainTypeName = grainTypeName;
            _config = config;

            _grainFactory = grainFactory;
        }

        private const int maxEntriesInNotifications = 200;

        private IStorageProvider _globalStorageProvider;
        private string _grainTypeName;
        private EventStorageOptions _config;
        private IGrainFactory _grainFactory;

        // the confirmed view
        private TLogView _view;

        // Summaries
        private StreamSummary _streamSummary;
        private IGlobalCommit _globalCommit;

        /// <inheritdoc/>
        protected override TLogView LastConfirmedView()
        {
            return _view;
        }

        /// <inheritdoc/>
        protected override int GetConfirmedVersion()
        {
            return _streamSummary.StreamSummaryState.CurrentCommitNumber;
        }

        /// <inheritdoc/>
        protected override void InitializeConfirmedView(TLogView initialState)
        {
            _view = initialState;
            _streamSummary = new StreamSummary();
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
        public override Task<IReadOnlyList<TLogEntry>> RetrieveLogSegment(int fromVersion, int toVersion)
        {
            throw new NotImplementedException();
            //// enter_operation("RetrieveLogSegment");

            //var grainReference = Services.GrainReference;
            //var baseId = grainReference.ToKeyString();

            //var summary = _grainFactory.GetGrain<IStreamSummary>(baseId);
            //var summaryVersion = await summary.GetCurrentVersion();

            //if (toVersion > summaryVersion)
            //{
            //    toVersion = summaryVersion;
            //}

            //// Load every commit, add it to the list
            //var eventList = new List<TLogEntry>();

            //for (var i = fromVersion; i <= toVersion; i++)
            //{
            //    var grainCommit = _grainFactory.GetGrain<IStreamCommit>($"{baseId}{EventStorageConstants.CommitVersionPrefix}{i}");
            //    var commitNumber = await grainCommit.GetCommitNumber();

            //    var commit = _grainFactory.GetGrain<ICommit>(commitNumber);
            //    var events = await commit.GetEvents();

            //    if (events != null)
            //    {
            //        foreach (var @event in events)
            //        {
            //            eventList.Add(@event as TLogEntry);
            //        }
            //    }
            //}

            //// exit_operation("RetrieveLogSegment");

            //return eventList.AsReadOnly();
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
            var grainReference = Services.GrainReference;
            var originalGrainId = grainReference.GetGrainId();

            var streamSummary = new StreamSummary();
            await _globalStorageProvider.ReadStateAsync(nameof(StreamSummary), grainReference, streamSummary);

            var streamCommitNumber = streamSummary.StreamSummaryState.CurrentCommitNumber;
            
            if (_config.TakeSnapshots)
            {
                try
                {
                    var snapshot = new StreamSnapshot<TLogView>();
                    await _globalStorageProvider.ReadStateAsync(nameof(StreamSnapshot<TLogView>), grainReference, snapshot);

                    if(snapshot.StreamSnapshotState.Snapshot != null)
                    {
                        _view = snapshot.StreamSnapshotState.Snapshot;
                    }
                }
                catch(Exception e)
                {
                    // Something wrong with the snapshot.   Probably a different version of the state than what was persisted   
                }
            }

            if(streamCommitNumber > 0 && streamCommitNumber > _streamSummary.StreamSummaryState.CurrentCommitNumber)
            {
                var startCommit = _streamSummary.StreamSummaryState.CurrentCommitNumber + 1;
                var grainKeyString = grainReference.ToKeyString();

                for (var i = startCommit; i <= streamCommitNumber; i++)
                {
                    // Update the grain reference number to match the version we're looking for
                    grainReference.ChangeGrainId($"{grainKeyString}{EventStorageConstants.CommitVersionPrefix}{i}");

                    var streamCommit = new StreamCommit();
                    await _globalStorageProvider.ReadStateAsync(nameof(StreamCommit), grainReference, streamCommit);

                    var commitNumber = streamCommit.StreamCommitState.GlobalCommitNumber;

                    // Now look up the actual commit
                    grainReference.ChangeGrainId(EventStorageConstants.GlobalCommitStreamName);

                    var commit = new Commit();
                    await _globalStorageProvider.ReadStateAsync(nameof(Commit), grainReference, commit);

                    // Apply the events to the view
                    if (commit.CommitState.Events != null && commit.CommitState.Events.Count > 0)
                    {
                        foreach (var @event in commit.CommitState.Events)
                        {
                            ApplyEventToView(@event as TLogEntry);
                        }
                    }
                }
            }

            // Set the grainid of the grainReference back to the original
            grainReference.ReplaceGrainId(originalGrainId);

            _streamSummary = streamSummary;
        }

        /// <inheritdoc/>
        protected override async Task<int> WriteAsync()
        {
            if (_globalCommit == null)
            {
                _globalCommit = _grainFactory.GetGrain<IGlobalCommit>(Guid.Empty);
            }

            var batchSuccessfullyWritten = false;

            var updates = GetCurrentBatchOfUpdates();
            var events = updates.Select(u => u.Entry)?.ToList();

            try
            {
                var grainReference = Services.GrainReference;
                var originalGrainId = grainReference.GetGrainId();
                var grainKeyString = grainReference.ToKeyString();

                _streamSummary.StreamSummaryState.CurrentCommitNumber++;
                await _globalStorageProvider.WriteStateAsync(nameof(StreamSummary), grainReference, _streamSummary);

                // Read, bump and save the global summary
                //grainReference.ChangeGrainId(EventStorageConstants.GlobalCommitStreamName);
                //var globalSummary = new StreamSummary();
                //await _globalStorageProvider.ReadStateAsync(nameof(StreamSummary), grainReference, globalSummary);
                //globalSummary.StreamSummaryState.CurrentCommitNumber++;
                //await _globalStorageProvider.WriteStateAsync(nameof(StreamSummary), grainReference, globalSummary);
                var globalCommitNumber = await _globalCommit.Bump();

                // Save the commit
                var commit = new Commit();
                commit.CommitState.Number = globalCommitNumber;
                commit.CommitState.Events = events.Select(s => s as object).ToList();

                grainReference.ChangeGrainId((long)commit.CommitState.Number);
                await _globalStorageProvider.WriteStateAsync(nameof(Commit), grainReference, commit);

                // Save the stream commit
                var streamCommit = new StreamCommit();
                streamCommit.StreamCommitState.GlobalCommitNumber = globalCommitNumber;
                streamCommit.StreamCommitState.StreamCommitNumber = _streamSummary.StreamSummaryState.CurrentCommitNumber;

                grainReference.ChangeGrainId($"{grainKeyString}{EventStorageConstants.CommitVersionPrefix}{_streamSummary.StreamSummaryState.CurrentCommitNumber}");
                await _globalStorageProvider.WriteStateAsync(nameof(StreamCommit), grainReference, streamCommit);

                // Reset the grain reference id
                grainReference.ReplaceGrainId(originalGrainId);


                // Apply changes to local view
                foreach (var @event in events)
                {
                    ApplyEventToView(@event);
                }

                // Check to see if we need to snapshot
                if (_config.TakeSnapshots)
                {
                    if (_streamSummary.StreamSummaryState.CurrentCommitNumber > 0 &&_streamSummary.StreamSummaryState.CurrentCommitNumber % _config.CommitsPerSnapshot == 0)
                    {
                        var snapshot = new StreamSnapshot<TLogView>();
                        snapshot.StreamSnapshotState.CommitNumber = _streamSummary.StreamSummaryState.CurrentCommitNumber;
                        snapshot.StreamSnapshotState.Snapshot = _view;

                        await _globalStorageProvider.WriteStateAsync(nameof(StreamSnapshot<TLogView>), grainReference, snapshot);
                    }
                }

                batchSuccessfullyWritten = true;
            }
            catch(Exception e)
            {
                
            }

            if (batchSuccessfullyWritten)
            {
                BroadcastNotification(new UpdateNotificationMessage()
                {
                    Version = _streamSummary.StreamSummaryState.CurrentCommitNumber,
                    Updates = updates.Select(se => se.Entry).ToList(),
                    Origin = Services.MyClusterId,
                    ETag = _streamSummary.ETag
                });
            }


            // TODO - Post Commit callback

            if (!batchSuccessfullyWritten)
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
            public int Version { get; set; }

            /// <summary> The cluster that performed the update </summary>
            public string Origin { get; set; }

            /// <summary> The list of updates that were applied </summary>
            public List<TLogEntry> Updates { get; set; }

            public string ETag { get; set; }

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

            if (earlier != null && later != null && earlier.Origin == later.Origin && earlier.Version + 1 == later.Version && earlier.Updates.Count + later.Updates.Count < maxEntriesInNotifications)

                return new UpdateNotificationMessage()
                {
                    Version = later.Version,
                    Origin = later.Origin,
                    Updates = earlier.Updates.Concat(later.Updates).ToList(),
                    ETag = later.ETag
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

            // process notifications that reflect next global version
            while (orderedNotifications.Count > 0 )
            {
                var notificationPair = notifications.ElementAt(0);
                notifications.RemoveAt(0);

                if(notificationPair.Key < _streamSummary.StreamSummaryState.CurrentCommitNumber)
                {
                    continue;
                }

                var updateNotification = notificationPair.Value;
                
                // append all operations in pending 
                foreach (var u in updateNotification.Updates)
                {
                    ApplyEventToView(u);
                }

                _streamSummary.StreamSummaryState.CurrentCommitNumber = updateNotification.Version;
                _streamSummary.ETag = updateNotification.ETag;

                Services.Log(LogLevel.Information, "notification success ({0} updates)", updateNotification.Updates.Count);
            }

            Services.Log(LogLevel.Information, "unprocessed notifications in queue: {0}", notifications.Count);

            base.ProcessNotifications();
        }
    }
}
