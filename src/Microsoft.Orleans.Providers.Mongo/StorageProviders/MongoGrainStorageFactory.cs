using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Microsoft.Orleans.Providers.Mongo.Configuration;
using Orleans.Storage;

namespace Microsoft.Orleans.Providers.Mongo.StorageProviders
{
    public static class MongoGrainStorageFactory
    {
        public static IGrainStorage Create(IServiceProvider services, string name)
        {
            var optionsMonitor = services.GetRequiredService<IOptionsMonitor<MongoDBGrainStorageOptions>>();

            return ActivatorUtilities.CreateInstance<MongoGrainStorage>(services, optionsMonitor.Get(name));
        }
    }
}
