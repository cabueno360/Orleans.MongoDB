using MongoDB.Driver;
using Microsoft.Orleans.Providers.Mongo.Configuration;
using Microsoft.Orleans.Providers.Mongo.Membership.Store.Multiple;
using Microsoft.Orleans.Providers.Mongo.Membership.Store.MultipleDeprecated;
using Microsoft.Orleans.Providers.Mongo.Membership.Store.Single;
using System;

namespace Microsoft.Orleans.Providers.Mongo.Membership.Store
{
    public static class Factory
    {
        public static IMongoMembershipCollection CreateCollection(IMongoClient mongoClient, MongoDBOptions options, MongoDBMembershipStrategy strategy)
        {
            switch (strategy)
            {
                case MongoDBMembershipStrategy.SingleDocument:
                     return new SingleMembershipCollection(
                        mongoClient,
                        options.DatabaseName,
                        options.CollectionPrefix,
                        options.CreateShardKeyForCosmos,
                        options.CollectionConfigurator);
                case MongoDBMembershipStrategy.Multiple:
                    return new MultipleMembershipCollection(
                        mongoClient,
                        options.DatabaseName,
                        options.CollectionPrefix,
                        options.CollectionConfigurator,
                        options.CreateShardKeyForCosmos);
                case MongoDBMembershipStrategy.MultipleDeprecated:
                    return new MultipleDeprecatedMembershipCollection(
                        mongoClient,
                        options.DatabaseName,
                        options.CollectionPrefix,
                        options.CollectionConfigurator,
                        options.CreateShardKeyForCosmos);
            }

            throw new ArgumentException("Invalid strategy.", nameof(strategy));
        }
    }
}
