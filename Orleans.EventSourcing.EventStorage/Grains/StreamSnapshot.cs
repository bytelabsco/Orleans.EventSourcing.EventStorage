namespace Orleans.EventSourcing.EventStorage.Grains
{
    using Orleans;
    using System.Threading.Tasks;

    internal class StreamSnapshot<TLogView> : Grain<StreamSnapshotState<TLogView>>, IStreamSnapshot<TLogView>
    {
        public async Task<StreamSnapshotState<TLogView>> GetSnapshot()
        {
            await ReadStateAsync();
            return State;
        }

        public async Task TakeSnapshot(TLogView state, int version)
        {
            State.Snapshot = state;
            State.Version = version;

            await WriteStateAsync();
        }
    }

    internal class StreamSnapshotState<TLogView>
    {
        public TLogView Snapshot { get; set; }

        public int Version { get; set; }
    }

    internal interface IStreamSnapshot<TLogView> : IGrainWithStringKey
    {
        Task TakeSnapshot(TLogView state, int version);

        Task<StreamSnapshotState<TLogView>> GetSnapshot();
    }
}
