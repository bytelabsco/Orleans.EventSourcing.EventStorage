namespace Orleans.EventSourcing.EventStorage.States
{
    using System;

    [Serializable]
    public class StreamCommit : IGrainState
    {
        public StreamCommit()
        {
            StreamCommitState = new StreamCommitState();
        }

        public StreamCommitState StreamCommitState { get; set; }

        public string ETag { get; set; }

        public object State
        {
            get
            {
                return StreamCommitState;
            }
            set
            {
                StreamCommitState = (StreamCommitState)value;
            }
        }
    }

    [Serializable]
    public class StreamCommitState
    {
        public int StreamCommitNumber { get; set; } = 0;

        public int GlobalCommitNumber { get; set; } = 0;
    }
}
