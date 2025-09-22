using MongoDB.Driver;

namespace Microsoft.Orleans.Providers.Mongo.Utils
{
    public sealed class DefaultMongoClientFactory : IMongoClientFactory
    {
        private readonly IMongoClient mongoClient;

        public DefaultMongoClientFactory(IMongoClient mongoClient)
        {
            this.mongoClient = mongoClient;
        }

        public IMongoClient Create(string name)
        {
            return mongoClient;
        }
    }
}
