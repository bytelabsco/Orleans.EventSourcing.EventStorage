namespace Orleans.EventSourcing.EventStorage
{
    public static class EventStorageConstants
    {
        public static string CommitVersionPrefix { get { return "+v"; } }

        public static string GlobalCommitStreamName { get { return "global"; } }

        public static class ReflectionHelpers
        {
            public static string GrainIdAssemblyName { get { return "Orleans.Core.Abstractions"; } }

            public static string GrainIdClassName { get { return "Orleans.Runtime.GrainId"; } }

            public static string GrainIdPropertyName { get { return "GrainId"; } }

            public static string GetGrainIdMethodName { get { return "GetGrainId"; } }

            public static string TypeCodePropertyName { get { return "TypeCode"; } }
            
        }
    }
}
