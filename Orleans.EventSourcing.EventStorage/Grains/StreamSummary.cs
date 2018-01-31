namespace Orleans.EventSourcing.EventStorage.Grains
{
    using Orleans;
    using Orleans.MultiCluster;
    using System.Threading.Tasks;

    [OneInstancePerCluster]
    internal class StreamSummary : Grain<StreamSummaryState>, IStreamSummary
    {
        public async Task<int> BumpVersion()
        {
            await ReadStateAsync();
            State.CommitNumber++;
            await WriteStateAsync();

            return State.CommitNumber;
        }

        public async Task<int> GetCurrentVersion()
        {
            await ReadStateAsync();
            return State.CommitNumber;
        }
    }

    internal class StreamSummaryState
    {
        public int CommitNumber { get; set; }
    }

    internal interface IStreamSummary : IGrainWithStringKey
    {
        Task<int> BumpVersion();
        Task<int> GetCurrentVersion();
    }
}
