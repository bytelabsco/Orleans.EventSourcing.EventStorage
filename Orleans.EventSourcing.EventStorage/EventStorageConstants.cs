namespace Orleans.EventSourcing.EventStorage
{
    public static class EventStorageConstants
    {
        public static string CommitVersionPrefix { get { return "+v"; } }

        public static string GlobalCommitStreamName { get { return "GlobalStream"; } }
    }
}
