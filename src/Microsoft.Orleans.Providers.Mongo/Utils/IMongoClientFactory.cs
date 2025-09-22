using MongoDB.Driver;

namespace Microsoft.Orleans.Providers.Mongo.Utils
{
    public interface IMongoClientFactory
    {
        IMongoClient Create(string name);
    }
}
