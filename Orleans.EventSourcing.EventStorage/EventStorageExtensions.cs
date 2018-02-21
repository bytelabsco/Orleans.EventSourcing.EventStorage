namespace Orleans.EventSourcing.EventStorage
{
    using System;
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
        public static object GetGrainId(this GrainReference grainReference)
        {
            var grainReferenceType = grainReference.GetType();
            var grainIdProperty = grainReferenceType.GetProperty(EventStorageConstants.ReflectionHelpers.GrainIdPropertyName, BindingFlags.Instance | BindingFlags.NonPublic);
            var grainId = grainIdProperty.GetValue(grainReference);
            return grainId;
        }

        public static void ChangeGrainId(this GrainReference grainReference, long key, string keyExt = null)
        {
            object[] parameters = { grainReference.GetGrainIdTypeCode(), key, keyExt };

            var assembly = Assembly.Load(EventStorageConstants.ReflectionHelpers.GrainIdAssemblyName);
            var grainIdType = assembly.GetType(EventStorageConstants.ReflectionHelpers.GrainIdClassName);

            MethodInfo methodInfo = grainIdType.GetMethod(
                EventStorageConstants.ReflectionHelpers.GetGrainIdMethodName,
                BindingFlags.Static | BindingFlags.NonPublic,
                Type.DefaultBinder,
                new[] { typeof(Int64), typeof(Int64), typeof(String) },
                null);
            var newGrainId = methodInfo.Invoke(null, parameters);

            grainReference.ReplaceGrainId(newGrainId);
        }

        public static void ChangeGrainId(this GrainReference grainReference, Guid key, string keyExt = null)
        {
            object[] parameters = { grainReference.GetGrainIdTypeCode(), key, keyExt };

            var assembly = Assembly.Load(EventStorageConstants.ReflectionHelpers.GrainIdAssemblyName);
            var grainIdType = assembly.GetType(EventStorageConstants.ReflectionHelpers.GrainIdClassName);

            MethodInfo methodInfo = grainIdType.GetMethod(
                EventStorageConstants.ReflectionHelpers.GetGrainIdMethodName,
                BindingFlags.Static | BindingFlags.NonPublic,
                Type.DefaultBinder,
                new[] { typeof(Int64), typeof(Guid), typeof(String) },
                null);

            var newGrainId = methodInfo.Invoke(null, parameters);

            grainReference.ReplaceGrainId(newGrainId);
        }

        public static void ChangeGrainId(this GrainReference grainReference, string key)
        {
            object[] parameters = { grainReference.GetGrainIdTypeCode(), key };

            var assembly = Assembly.Load(EventStorageConstants.ReflectionHelpers.GrainIdAssemblyName);
            var grainIdType = assembly.GetType(EventStorageConstants.ReflectionHelpers.GrainIdClassName);

            MethodInfo methodInfo = grainIdType.GetMethod(
                EventStorageConstants.ReflectionHelpers.GetGrainIdMethodName, 
                BindingFlags.Static | BindingFlags.NonPublic, 
                Type.DefaultBinder, 
                new [] { typeof(Int64), typeof(String) }, 
                null);

            var newGrainId = methodInfo.Invoke(null, parameters);

            grainReference.ReplaceGrainId(newGrainId);
        }

        public static void ReplaceGrainId(this GrainReference grainReference, object grainId)
        {
            grainReference.GetType().GetProperty(EventStorageConstants.ReflectionHelpers.GrainIdPropertyName, BindingFlags.Instance | BindingFlags.NonPublic).SetValue(grainReference, grainId);
        }

        public static long GetGrainIdTypeCode(this GrainReference grainReference)
        {
            var grainId = grainReference.GetGrainId();
            Int32 typeCode = (Int32)grainId.GetType().GetProperty(EventStorageConstants.ReflectionHelpers.TypeCodePropertyName).GetValue(grainId);

            return Convert.ToInt64(typeCode);
        }
    }
}
