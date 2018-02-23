namespace Orleans.EventSourcing.EventStorage
{
    using System;
    using System.Linq.Expressions;
    using System.Reflection;
    using Orleans.Core;
    using Orleans.Hosting;
    using Orleans.Runtime;
    using Orleans.Runtime.Configuration;

    public static class EventStorageExtensions
    {
        public static ISiloHostBuilder UseEventStorageLogProvider(this ISiloHostBuilder builder, Action<EventStorageOptions> options)
        {
            return builder.Configure(options);
        }

        public static void AddEventStorageLogProvider(this ClusterConfiguration config, string name)
        {
            config.Globals.RegisterLogConsistencyProvider<EventStorageLogConsistencyProvider>(name);
        }
    }

    public static class GrainReferenceExtensions
    {
        private static Func<long, long, string, object> _getGrainIdLong;
        private static Func<long, Guid, string, object> _getGrainIdGuid;
        private static Func<long, string, object> _getGrainIdString;
        private static PropertyInfo _grainIdPropertyInfo;
        private static PropertyInfo _grainIdTypeCodePropertyInfo;

        static GrainReferenceExtensions()
        {
            var assembly = Assembly.Load(EventStorageConstants.ReflectionHelpers.GrainIdAssemblyName);
            var grainIdType = assembly.GetType(EventStorageConstants.ReflectionHelpers.GrainIdClassName);


            MethodInfo getGrainIdLongMethodInfo = grainIdType.GetMethod(
                EventStorageConstants.ReflectionHelpers.GetGrainIdMethodName,
                BindingFlags.Static | BindingFlags.NonPublic,
                Type.DefaultBinder,
                new[] { typeof(long), typeof(long), typeof(String) },
                null);

            MethodInfo getGrainIdGuidMethodInfo = grainIdType.GetMethod(
                EventStorageConstants.ReflectionHelpers.GetGrainIdMethodName,
                BindingFlags.Static | BindingFlags.NonPublic,
                Type.DefaultBinder,
                new[] { typeof(long), typeof(Guid), typeof(String) },
                null);

            MethodInfo getGrainIdStringMethodInfo = grainIdType.GetMethod(
                EventStorageConstants.ReflectionHelpers.GetGrainIdMethodName,
                BindingFlags.Static | BindingFlags.NonPublic,
                Type.DefaultBinder,
                new[] { typeof(long), typeof(String) },
                null);

            _getGrainIdLong = (Func<long, long, String, object>)Delegate.CreateDelegate(typeof(Func<long, long, String, object>), getGrainIdLongMethodInfo);
            _getGrainIdGuid = (Func<long, Guid, String, object>)Delegate.CreateDelegate(typeof(Func<long, Guid, String, object>), getGrainIdGuidMethodInfo);
            _getGrainIdString = (Func<long, String, object>)Delegate.CreateDelegate(typeof(Func<long, String, object>), getGrainIdStringMethodInfo);


            _grainIdPropertyInfo = typeof(GrainReference).GetProperty(EventStorageConstants.ReflectionHelpers.GrainIdPropertyName, BindingFlags.Instance | BindingFlags.NonPublic);
            _grainIdTypeCodePropertyInfo = grainIdType.GetProperty(EventStorageConstants.ReflectionHelpers.TypeCodePropertyName);
        }

        public static object GetGrainId(this GrainReference grainReference)
        {
            var grainId = _grainIdPropertyInfo.GetValue(grainReference);
            return grainId;
        }

        public static void ChangeGrainId(this GrainReference grainReference, long key, string keyExt = null)
        {
            var newGrainId = _getGrainIdLong(grainReference.GetGrainIdTypeCode(), key, keyExt);
            grainReference.ReplaceGrainId(newGrainId);
        }

        public static void ChangeGrainId(this GrainReference grainReference, Guid key, string keyExt = null)
        {
            var newGrainId = _getGrainIdGuid(grainReference.GetGrainIdTypeCode(), key, keyExt);
            grainReference.ReplaceGrainId(newGrainId);
        }

        public static void ChangeGrainId(this GrainReference grainReference, string key)
        {
            var newGrainId = _getGrainIdString(grainReference.GetGrainIdTypeCode(), key);
            grainReference.ReplaceGrainId(newGrainId);
        }

        public static void ReplaceGrainId(this GrainReference grainReference, object grainId)
        {
            _grainIdPropertyInfo.SetValue(grainReference, grainId);
        }

        public static long GetGrainIdTypeCode(this GrainReference grainReference)
        {
            var grainId = grainReference.GetGrainId();
            var typeCode = _grainIdTypeCodePropertyInfo.GetValue(grainId);

            return Convert.ToInt64(typeCode);
        }
    }
}
