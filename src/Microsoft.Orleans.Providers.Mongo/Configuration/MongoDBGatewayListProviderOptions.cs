// ReSharper disable InheritdocConsiderUsage

namespace Microsoft.Orleans.Providers.Mongo.Configuration
{
    /// <summary>
    /// Configures MongoDB Gateway List Provider.
    /// </summary>
    public sealed class MongoDBGatewayListProviderOptions : MongoDBOptions
    {
        public MongoDBMembershipStrategy Strategy { get; set; }

        public MongoDBGatewayListProviderOptions()
        {
        }
    }
}
