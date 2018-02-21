namespace Orleans.EventSourcing.EventStorage.States
{
    using System;
    using System.Collections.Generic;

    [Serializable]
    public class Commit : IGrainState
    {
        public Commit()
        {
            CommitState = new CommitState();
        }

        public CommitState CommitState { get; set; }

        public string ETag { get; set; }

        public object State
        {
            get
            {
                return CommitState;
            }
            set
            {
                CommitState = (CommitState)value;
            }
        }
    }

    [Serializable]
    public class CommitState
    {
        public int Number { get; set; } = 0;

        public IList<object> Events { get; set; }
    }
}
