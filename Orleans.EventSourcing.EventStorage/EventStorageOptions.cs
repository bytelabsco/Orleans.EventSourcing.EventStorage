namespace Orleans.EventSourcing.EventStorage
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    public class EventStorageOptions
    {
        public bool TakeSnapshots { get; set; } = true;
        public int CommitsPerSnapshot { get; set; } = 100;

        public Func<object, Task> PostCommit;

        internal IDictionary<string, string> AsDictionary()
        {
            var dictionary = new Dictionary<string, string>();

            dictionary.Add(nameof(TakeSnapshots), TakeSnapshots.ToString());
            dictionary.Add(nameof(CommitsPerSnapshot), CommitsPerSnapshot.ToString());

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
                }
            }

            return options;
        }
    }
}
