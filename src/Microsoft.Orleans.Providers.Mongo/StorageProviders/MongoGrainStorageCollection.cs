using System;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using Microsoft.Orleans.Providers.Mongo.Configuration;
using Microsoft.Orleans.Providers.Mongo.StorageProviders.Serializers;
using Microsoft.Orleans.Providers.Mongo.Utils;
using Orleans.Runtime;
using Orleans.Storage;

namespace Microsoft.Orleans.Providers.Mongo.StorageProviders
{
    internal sealed class MongoGrainStorageCollection : CollectionBase<BsonDocument>
    {
        private const string FieldId = "_id";
        private const string FieldDoc = "_doc";
        private const string FieldEtag = "_etag";
        private const string FieldGrainId = "_grainId";
        private readonly string collectionName;
        private readonly IGrainStateSerializer serializer;
        private readonly GrainStorageKeyGenerator keyGenerator;

        public MongoGrainStorageCollection(
            IMongoClient mongoClient,
            string databaseName,
            string collectionName,
            Action<MongoCollectionSettings> collectionConfigurator,
            bool createShardKey,
            IGrainStateSerializer serializer,
            GrainStorageKeyGenerator keyGenerator)
            : base(mongoClient, databaseName, collectionConfigurator, createShardKey)
        {
            this.collectionName = collectionName;
            this.serializer = serializer;
            this.keyGenerator = keyGenerator;
        }

        protected override string CollectionName()
        {
            return collectionName;
        }

        public async Task ReadAsync<T>(GrainId grainId, IGrainState<T> grainState)
        {
            var grainKey = keyGenerator(grainId);

            var existing =
                await Collection.Find(Filter.Eq(FieldId, grainKey))
                    .FirstOrDefaultAsync();

            if (existing != null)
            {
                grainState.RecordExists = true;

                if (existing.Contains(FieldDoc))
                {
                    grainState.ETag = existing[FieldEtag].AsString;

                    grainState.State = serializer.Deserialize<T>(existing[FieldDoc]);
                }
                else
                {
                    existing.Remove(FieldId);

                    grainState.State = serializer.Deserialize<T>(existing);
                }
            }
        }

        public async Task WriteAsync<T>(GrainId grainId, IGrainState<T> grainState)
        {
            var grainKey = keyGenerator(grainId);

            var newData = serializer.Serialize(grainState.State);

            var etag = grainState.ETag;

            var newETag = Guid.NewGuid().ToString();

            grainState.RecordExists = true;

            // Build filter: _id == key AND (_etag == provided OR _etag does not exist)
            FilterDefinition<BsonDocument> etagFilter;
            if (etag == null)
            {
                etagFilter = Filter.Exists(FieldEtag, false);
            }
            else
            {
                etagFilter = Filter.Or(
                    Filter.Eq(FieldEtag, etag),
                    Filter.Exists(FieldEtag, false));
            }

            var filter = Filter.And(
                Filter.Eq(FieldId, grainKey),
                etagFilter);

            var update = Update
                .Set(FieldGrainId, grainId.ToString())
                .Set(FieldEtag, newETag)
                .Set(FieldDoc, newData)
                .SetOnInsert(FieldId, grainKey);

            // Single round-trip, optimistic concurrency + insert-on-first-write
            var result = await Collection.UpdateOneAsync(filter, update, Upsert);

            // If nothing matched and no upsert happened, then ETag mismatch likely occurred.
            if (result.MatchedCount == 0 && result.UpsertedId == null)
            {
                var existingEtag = await Collection
                    .Find(Filter.Eq(FieldId, grainKey))
                    .Project<BsonDocument>(Project.Exclude(FieldDoc))
                    .FirstOrDefaultAsync();

                if (existingEtag != null && existingEtag.Contains(FieldEtag))
                {
                    throw new InconsistentStateException(existingEtag[FieldEtag].AsString, etag, "ETag mismatch");
                }
            }

            grainState.ETag = newETag;
        }

        public Task ClearAsync<T>(GrainId grainId, IGrainState<T> grainState)
        {
            var grainKey = keyGenerator(grainId);

            grainState.RecordExists = false;

            return Collection.DeleteOneAsync(Filter.Eq(FieldId, grainKey));
        }

        private async Task ThrowForOtherEtag(string key, string etag, Exception ex)
        {
            var existingEtag =
                await Collection.Find(Filter.Eq(FieldId, key))
                    .Project<BsonDocument>(Project.Exclude(FieldDoc)).FirstOrDefaultAsync();

            if (existingEtag != null && existingEtag.Contains(FieldEtag))
            {
                throw new InconsistentStateException(existingEtag[FieldEtag].AsString, etag, ex);
            }
        }
    }
}
