namespace Orleans.EventSourcing.EventStorage
{
    using Orleans.Runtime;

    internal class CustomIdGrainReferenceDecorator : GrainReference
    {
        private string _id;

        public Orleans.Runtime.UniqueKey GrainId { get; set; }

        public CustomIdGrainReferenceDecorator(GrainReference other, string id): base(other)
        {
            _id = id;
        }

        public new string ToKeyString()
        {
            return _id;
        }
    }
}
