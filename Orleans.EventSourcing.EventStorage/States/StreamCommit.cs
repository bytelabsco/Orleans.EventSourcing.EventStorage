namespace Orleans.EventSourcing.EventStorage.States
{
    using System;
    using Orleans.EventSourcing.Common;

    public class StreamCommit : IGrainState
    {
        public StreamCommit()
        {
            State = new StreamCommitState();
        }

        public StreamCommitState StreamCommitState { get; set; }

        public object State {
            get
            {
                return StreamCommitState;
            }
            set
            {
                StreamCommitState = (StreamCommitState)value;
            }
        }

        public string ETag { get; set; }
    }

    [Serializable]
    public class StreamCommitState
    {
        public long Number { get; set; }

        public long GlobalNumber { get; set; }

        public StreamCommitState()
        {
            Number = -1;
            GlobalNumber = -1;
        }
    }
}
