namespace Orleans.EventSourcing.EventStorage.Grains
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Orleans;

    public class Commit : Grain<CommitData>, ICommit
    {
        public override async Task OnActivateAsync()
        {
            await ReadStateAsync();

            if (State.Number == 0)
            {
                State.Number = (int)this.GetPrimaryKeyLong();
                await WriteStateAsync();
            }
        }

        public async Task<IList<object>> GetEvents()
        {
            await ReadStateAsync();

            var s = State;
            return State.Events;
        }

        public async Task RecordEvents(IList<object> events)
        {
            if (State.Events != null)
            {
                throw new InvalidOperationException("Cannot record events to a previously saved commit");
            }

            State.Events = events;
            await WriteStateAsync();
        }

        public async Task StreamEvents(string provider, string name)
        {
            if (State.Events == null || State.Events.Count == 0)
            {
                throw new InvalidOperationException("No events have been recorded");
            }

            if (State.Number <= 0)
            {
                throw new InvalidOperationException("No commit number was specified");
            }

            if (!string.IsNullOrEmpty(provider) && !string.IsNullOrEmpty(name))
            {
                var streamProvider = base.GetStreamProvider(provider);
                var stream = streamProvider.GetStream<CommitData>(Guid.Empty, name);

                await stream.OnNextAsync(State);
            }
        }
    }

    [Serializable]
    public class CommitData
    {
        public int Number { get; set; }

        public IList<object> Events { get; set; }
    }

    public interface ICommit : IGrainWithIntegerKey
    {
        Task RecordEvents(IList<object> events);

        Task<IList<object>> GetEvents();

        Task StreamEvents(string streamProvider, string streamName);
    }
}
