using System;
using System.Linq;
using System.Threading.Tasks;
using MongoDB.Driver;
using Microsoft.Orleans.Providers.Mongo.Utils;
using Orleans.Runtime;
using Orleans.Providers.MongoDB.Reminders.Store;

// ReSharper disable RedundantIfElseBlock

namespace Microsoft.Orleans.Providers.Mongo.Reminders.Store
{
    internal sealed class MongoReminderProjection
    {
        public string Etag { get; set; }
        public string GrainId { get; set; }
        public TimeSpan Period { get; set; }
        public string ReminderName { get; set; }
        public DateTime StartAt { get; set; }
    }

    public class MongoReminderCollection : CollectionBase<MongoReminderDocument>
    {
        private static readonly FindOneAndUpdateOptions<MongoReminderDocument> FindAndUpsert = new FindOneAndUpdateOptions<MongoReminderDocument> { IsUpsert = true };
        private readonly string serviceId;
        private readonly string collectionPrefix;

        public MongoReminderCollection(
            IMongoClient mongoClient,
            string databaseName,
            string collectionPrefix,
            Action<MongoCollectionSettings> collectionConfigurator,
            bool createShardKey,
            string serviceId)
            : base(mongoClient, databaseName, collectionConfigurator, createShardKey)
        {
            this.serviceId = serviceId;
            this.collectionPrefix = collectionPrefix;
        }

        protected override string CollectionName()
        {
            return collectionPrefix + "OrleansReminders";
        }

        protected override void SetupCollection(IMongoCollection<MongoReminderDocument> collection)
        {
            // Do NOT create an extra index on Id: Mongo already has a unique index on _id
            // Ensure compound indexes for common queries

            // 1) ReadRowsInRange / ReadRowsOutRange: ServiceId + GrainHash
            var byHashDefinition =
                Index
                    .Ascending(x => x.ServiceId)
                    .Ascending(x => x.GrainHash);
            collection.Indexes.CreateOne(new CreateIndexModel<MongoReminderDocument>(byHashDefinition,
                new CreateIndexOptions { Name = "ByService_GrainHash" }));

            // 2) ReadRow(grainId, reminderName): ServiceId + GrainId + ReminderName
            var byNameDefinition =
                Index
                    .Ascending(x => x.ServiceId)
                    .Ascending(x => x.GrainId)
                    .Ascending(x => x.ReminderName);
            collection.Indexes.CreateOne(new CreateIndexModel<MongoReminderDocument>(byNameDefinition,
                new CreateIndexOptions { Name = "ByService_GrainId_ReminderName" }));

            // 3) ReadReminderRowsAsync/ReadRow(grainId): ServiceId + GrainId
            var byGrainDefinition =
                Index
                    .Ascending(x => x.ServiceId)
                    .Ascending(x => x.GrainId);
            collection.Indexes.CreateOne(new CreateIndexModel<MongoReminderDocument>(byGrainDefinition,
                new CreateIndexOptions { Name = "ByService_GrainId" }));
        }

        public virtual async Task<ReminderTableData> ReadRowsInRange(uint beginHash, uint endHash)
        {
            var reminders =
                await Collection.Find(x =>
                        x.ServiceId == serviceId &&
                        x.GrainHash > beginHash &&
                        x.GrainHash <= endHash)
                    .Project(x => new MongoReminderProjection
                    {
                        Etag = x.Etag,
                        GrainId = x.GrainId,
                        Period = x.Period,
                        ReminderName = x.ReminderName,
                        StartAt = x.StartAt
                    })
                    .ToListAsync();

            return new ReminderTableData(reminders.Select(x => new ReminderEntry
            {
                ETag = x.Etag,
                GrainId = GrainId.Parse(x.GrainId),
                Period = x.Period,
                ReminderName = x.ReminderName,
                StartAt = x.StartAt
            }));
        }

        public virtual async Task<ReminderEntry> ReadRow(GrainId grainId, string reminderName)
        {
            var reminder =
                await Collection.Find(x =>
                        x.ServiceId == serviceId &&
                        x.GrainId == grainId.ToString() &&
                        x.ReminderName == reminderName)
                    .Project(x => new MongoReminderProjection
                    {
                        Etag = x.Etag,
                        GrainId = x.GrainId,
                        Period = x.Period,
                        ReminderName = x.ReminderName,
                        StartAt = x.StartAt
                    })
                    .FirstOrDefaultAsync();

            return reminder == null ? null : new ReminderEntry
            {
                ETag = reminder.Etag,
                GrainId = GrainId.Parse(reminder.GrainId),
                Period = reminder.Period,
                ReminderName = reminder.ReminderName,
                StartAt = reminder.StartAt
            };
        }

        public virtual async Task<ReminderTableData> ReadReminderRowsAsync(GrainId grainId)
        {
            var reminders =
                await Collection.Find(x =>
                        x.ServiceId == serviceId &&
                        x.GrainId == grainId.ToString())
                    .Project(x => new MongoReminderProjection
                    {
                        Etag = x.Etag,
                        GrainId = x.GrainId,
                        Period = x.Period,
                        ReminderName = x.ReminderName,
                        StartAt = x.StartAt
                    })
                    .ToListAsync();

            return new ReminderTableData(reminders.Select(x => new ReminderEntry
            {
                ETag = x.Etag,
                GrainId = GrainId.Parse(x.GrainId),
                Period = x.Period,
                ReminderName = x.ReminderName,
                StartAt = x.StartAt
            }));
        }

        public virtual async Task<ReminderTableData> ReadRowsOutRange(uint beginHash, uint endHash)
        {
            var reminders =
                await Collection.Find(x =>
                        (x.ServiceId == serviceId) &&
                        (x.GrainHash > beginHash || x.GrainHash <= endHash))
                    .Project(x => new MongoReminderProjection
                    {
                        Etag = x.Etag,
                        GrainId = x.GrainId,
                        Period = x.Period,
                        ReminderName = x.ReminderName,
                        StartAt = x.StartAt
                    })
                    .ToListAsync();

            return new ReminderTableData(reminders.Select(x => new ReminderEntry
            {
                ETag = x.Etag,
                GrainId = GrainId.Parse(x.GrainId),
                Period = x.Period,
                ReminderName = x.ReminderName,
                StartAt = x.StartAt
            }));
        }

        public virtual async Task<ReminderTableData> ReadRow(GrainId grainId)
        {
            var reminders =
                await Collection.Find(r =>
                        r.ServiceId == serviceId &&
                        r.GrainId == grainId.ToString())
                    .Project(x => new MongoReminderProjection
                    {
                        Etag = x.Etag,
                        GrainId = x.GrainId,
                        Period = x.Period,
                        ReminderName = x.ReminderName,
                        StartAt = x.StartAt
                    })
                    .ToListAsync();

            return new ReminderTableData(reminders.Select(x => new ReminderEntry
            {
                ETag = x.Etag,
                GrainId = GrainId.Parse(x.GrainId),
                Period = x.Period,
                ReminderName = x.ReminderName,
                StartAt = x.StartAt
            }));
        }

        public async Task<bool> RemoveRow(GrainId grainId, string reminderName, string eTag)
        {
            var id = ReturnId(serviceId, grainId, reminderName);

            // Atomic delete by primary key with ETag for concurrency
            var result = await Collection.DeleteOneAsync(x =>
                x.Id == id &&
                x.Etag == eTag &&
                x.ServiceId == serviceId);

            return result.DeletedCount == 1;
        }

        public virtual Task RemoveRows()
        {
            return Collection.DeleteManyAsync(r => r.ServiceId == serviceId);
        }

        public virtual async Task<string> UpsertRow(ReminderEntry entry)
        {
            var id = ReturnId(serviceId, entry.GrainId, entry.ReminderName);

            var updatedEtag = Guid.NewGuid().ToString();
            var updateDocument = MongoReminderDocument.Create(id, serviceId, entry, updatedEtag);

            try
            {
                await Collection.ReplaceOneAsync(x => x.Id == id,
                    updateDocument,
                    UpsertReplace);
            }
            catch (MongoException ex)
            {
                if (!ex.IsDuplicateKey())
                {
                    throw;
                }
            }

            entry.ETag = updatedEtag;

            return entry.ETag;
        }

        private static string ReturnId(string serviceId, GrainId grainId, string reminderName)
        {
            return $"{serviceId}_{grainId}_{reminderName}";
        }
    }
}
