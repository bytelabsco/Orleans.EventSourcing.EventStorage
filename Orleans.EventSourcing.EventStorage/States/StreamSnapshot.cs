namespace Orleans.EventSourcing.EventStorage.States
{
    using System;

    [Serializable]
    public class StreamSnapshot<TLogView> : IGrainState where TLogView: class
    {
        public StreamSnapshot()
        {
            StreamSnapshotState = new StreamSnapshotState<TLogView>();
        }

        public StreamSnapshotState<TLogView> StreamSnapshotState { get; set; }

        public string ETag { get; set; }

        public object State
        {
            get
            {
                return StreamSnapshotState;
            }
            set
            {
                StreamSnapshotState = (StreamSnapshotState<TLogView>)value;
            }
        }
    }

    [Serializable]
    public class StreamSnapshotState<TLogView>
    {
        public TLogView Snapshot { get; set; }

        public int CommitNumber { get; set; } = 0;
    }
}
