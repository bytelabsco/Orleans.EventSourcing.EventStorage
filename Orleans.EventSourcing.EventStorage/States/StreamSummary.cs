namespace Orleans.EventSourcing.EventStorage.States
{
    using System;
    using Orleans.EventSourcing.Common;

    [Serializable]
    public class StreamSummary : IGrainState
    {
        public StreamSummary()
        {
            StreamSummaryState = new StreamSummaryState();
        }

        public StreamSummaryState StreamSummaryState { get; set; }

        public string ETag { get; set; }

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
    }

    [Serializable]
    public class StreamSummaryState
    {
        public StreamSummaryState()
        {
            WriteVector = "";
            CurrentCommitNumber = 0;
        }

        public int CurrentCommitNumber { get; set; }

        public string WriteVector { get; set; }

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
