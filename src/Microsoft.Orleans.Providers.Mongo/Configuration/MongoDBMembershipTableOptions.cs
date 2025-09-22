// ReSharper disable InheritdocConsiderUsage

namespace Microsoft.Orleans.Providers.Mongo.Configuration
{
    /// <summary>
    /// Configures MongoDB Membership.
    /// </summary>
    public sealed class MongoDBMembershipTableOptions : MongoDBOptions
    {
        public MongoDBMembershipStrategy Strategy { get; set; }

        public MongoDBMembershipTableOptions()
        {
        }
    }
}
