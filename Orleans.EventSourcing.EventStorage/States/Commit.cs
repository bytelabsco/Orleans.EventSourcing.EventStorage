namespace Orleans.EventSourcing.EventStorage.States
{
    using System;
    using System.Collections.Generic;
    using Orleans.EventSourcing.Common;

    public class Commit<TEntry> : IGrainState where TEntry : class
    {
        public Commit()
        {
            State = new CommitState<TEntry>();
        }

        public CommitState<TEntry> CommitState { get; set; }
        public object State
        {
            get
            {
                return CommitState;
            }
            set
            {
                CommitState = (CommitState<TEntry>)value;
            }
        }

        public string ETag { get; set; }
    }

    [Serializable]
    public class CommitState<TEntry> where TEntry : class
    {        
        public List<TEntry> Events { get; set; }

        public long GlobalNumber { get; set; }

        public CommitState()
        {
            Events = new List<TEntry>();
            GlobalNumber = 0;
        }
    }
}
