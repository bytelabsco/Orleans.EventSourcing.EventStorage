namespace Orleans.EventSourcing.EventStorage.Grains
{
    using System.Threading.Tasks;
    using Orleans.MultiCluster;

    [OneInstancePerCluster]
    public class GlobalCommit : Grain<GlobalCommitState>, IGlobalCommit
    {
        public async Task<int> Bump()
        {
            State.Number++;
            await base.WriteStateAsync();
            return State.Number;
        }

        public Task<int> GetCurrentCommitNumber()
        {
            return Task.FromResult(State.Number);
        }
    }

    public class GlobalCommitState
    {
        public int Number { get; set; }
    }

    public interface IGlobalCommit : IGrainWithGuidKey
    {
        Task<int> Bump();

        Task<int> GetCurrentCommitNumber();
    }
}
