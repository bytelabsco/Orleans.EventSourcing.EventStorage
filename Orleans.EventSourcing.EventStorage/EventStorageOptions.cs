namespace Orleans.EventSourcing.EventStorage
{
    using System.Collections.Generic;

    public class EventStorageOptions
    {
        public bool TakeSnapshots { get; set; } = true;
        public int CommitsPerSnapshot { get; set; } = 100;

        public bool StreamCommits { get; set; } = false;
        public string StreamProvider { get; set; }
        public string StreamName { get; set; }


        internal IDictionary<string, string> AsDictionary()
        {
            var dictionary = new Dictionary<string, string>();

            dictionary.Add(nameof(TakeSnapshots), TakeSnapshots.ToString());
            dictionary.Add(nameof(CommitsPerSnapshot), CommitsPerSnapshot.ToString());

            dictionary.Add(nameof(StreamCommits), StreamCommits.ToString());
            dictionary.Add(nameof(StreamProvider), StreamProvider ?? string.Empty);
            dictionary.Add(nameof(StreamName), StreamName ?? string.Empty);

            return dictionary;
        }

        internal static EventStorageOptions FromDictionary(IDictionary<string, string> dictionary)
        {
            var options = new EventStorageOptions();

            foreach (var key in dictionary.Keys)
            {
                var value = dictionary[key];

                switch (key)
                {
                    case nameof(TakeSnapshots):
                        if (bool.TryParse(value, out bool takeSnapshots))
                        {
                            options.TakeSnapshots = takeSnapshots;
                        }
                        break;

                    case nameof(CommitsPerSnapshot):
                        if (int.TryParse(value, out int commitsPerSnapshot))
                        {
                            options.CommitsPerSnapshot = commitsPerSnapshot;
                        }
                        break;

                    case nameof(StreamCommits):
                        if (bool.TryParse(value, out bool streamCommits))
                        {
                            options.StreamCommits = streamCommits;
                        }
                        break;

                    case nameof(StreamProvider):
                        options.StreamProvider = value;
                        break;

                    case nameof(StreamName):
                        options.StreamName = value;
                        break;
                }
            }

            return options;
        }
    }
}
