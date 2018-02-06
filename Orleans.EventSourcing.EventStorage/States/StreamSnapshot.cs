namespace Orleans.EventSourcing.EventStorage.States
{
    using System;
    using Orleans.EventSourcing.Common;

    public class StreamSnapshot<TLogView> : IGrainState where TLogView : class
    {
        public StreamSnapshot()
        {
            State = new StreamSnapshotState<TLogView>();
        }

        public StreamSnapshotState<TLogView> StreamSnapshotState { get; set; }

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

        public string ETag { get; set; }
    }

    [Serializable]
    public class StreamSnapshotState<TLogView> where TLogView : class{

        public TLogView Snapshot { get; set; }

        public int Version { get; set; }

        public string WriteVector { get; set; }

        public StreamSnapshotState()
        {
            Version = 0;
            WriteVector = "";
        }

        public bool GetBit(string replica)
        {
            return StringEncodedWriteVector.GetBit(WriteVector, replica);
        }

        public bool FlipBit(string replica)
        {
            var str = WriteVector;
            var rval = StringEncodedWriteVector.FlipBit(ref str, replica);
            WriteVector = str;
            return rval;
        }
    }
}
