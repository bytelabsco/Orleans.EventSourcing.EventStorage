namespace Orleans.EventSourcing.EventStorage.Grains
{
    using Orleans;
    using System.Threading.Tasks;

    internal class StreamCommit : Grain<StreamCommitState>, IStreamCommit
    {
        public Task<int> GetCommitNumber()
        {
            return Task.FromResult(State.CommitNumber);
        }

        public async Task SetCommitNumber(int number)
        {
            State.CommitNumber = number;
            await WriteStateAsync();
        }
    }

    internal class StreamCommitState
    {
        public int CommitNumber { get; set; }
    }

    public interface IStreamCommit : IGrainWithStringKey
    {
        Task SetCommitNumber(int number);

        Task<int> GetCommitNumber();
    }
}
