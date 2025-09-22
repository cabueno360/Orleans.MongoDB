using System;
using Newtonsoft.Json;

namespace Microsoft.Orleans.Providers.Mongo.Configuration
{
    public class JsonGrainStateSerializerOptions
    {
        public Action<JsonSerializerSettings> ConfigureJsonSerializerSettings { get; set; } = ConfigureDefaultSettings;

        private static void ConfigureDefaultSettings(JsonSerializerSettings settings)
        {
            settings.NullValueHandling = NullValueHandling.Include;
            settings.DefaultValueHandling = DefaultValueHandling.Populate;
        }
    }
}
