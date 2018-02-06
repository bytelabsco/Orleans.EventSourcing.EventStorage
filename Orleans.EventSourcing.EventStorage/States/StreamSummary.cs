namespace Orleans.EventSourcing.EventStorage.States
{
    using System;
    using Orleans.EventSourcing.Common;

    public class StreamSummary : IGrainState
    {
        public StreamSummary()
        {
            State = new StreamSummaryState();
        }

        public StreamSummaryState StreamSummaryState { get; set; }

        public object State
        {
            get
            {
                return StreamSummaryState;
            }
            set
            {
                StreamSummaryState = (StreamSummaryState)value;
            }
        }

        public string ETag { get; set; }
    }

    [Serializable]
    public class StreamSummaryState
    {
        public long CommitNumber { get; set; }

        public string WriteVector { get; set; }

        public StreamSummaryState()
        {
            CommitNumber = 0;
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
