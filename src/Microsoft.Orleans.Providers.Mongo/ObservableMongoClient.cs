using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Core.Clusters;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Orleans.Providers.Mongo
{
    public class ObservableMongoClient : IMongoClient, IDisposable
    {
        private readonly MongoClient _innerClient;
        private readonly ILogger<ObservableMongoClient> _logger;

        public static event Action OnCreated;
        public static event Action OnDisposed;

        public ObservableMongoClient(MongoClient innerClient, ILogger<ObservableMongoClient> logger)
        {
            _innerClient = innerClient;
            _logger = logger;
            OnCreated?.Invoke();
            _logger?.LogInformation("MongoClient created.");
        }

        public void Dispose()
        {
            _logger?.LogInformation("MongoClient disposed.");
            OnDisposed?.Invoke();
            _innerClient.Dispose();
        }

        public IMongoDatabase GetDatabase(string name, MongoDatabaseSettings settings = null) => _innerClient.GetDatabase(name, settings);

        public IAsyncCursor<BsonDocument> ListDatabases(ListDatabasesOptions options = null, CancellationToken cancellationToken = default) => _innerClient.ListDatabases(options, cancellationToken);
        public IAsyncCursor<BsonDocument> ListDatabases(CancellationToken cancellationToken = default) => _innerClient.ListDatabases(cancellationToken);
        public IAsyncCursor<BsonDocument> ListDatabases(IClientSessionHandle session, CancellationToken cancellationToken = default) => _innerClient.ListDatabases(session, cancellationToken);
        public IAsyncCursor<BsonDocument> ListDatabases(IClientSessionHandle session, ListDatabasesOptions options, CancellationToken cancellationToken = default) => _innerClient.ListDatabases(session, options, cancellationToken);
        public Task<IAsyncCursor<BsonDocument>> ListDatabasesAsync(ListDatabasesOptions options = null, CancellationToken cancellationToken = default) => _innerClient.ListDatabasesAsync(options, cancellationToken);
        public Task<IAsyncCursor<BsonDocument>> ListDatabasesAsync(CancellationToken cancellationToken) => _innerClient.ListDatabasesAsync(cancellationToken);
        public Task<IAsyncCursor<BsonDocument>> ListDatabasesAsync(IClientSessionHandle session, CancellationToken cancellationToken = default) => _innerClient.ListDatabasesAsync(session, cancellationToken);
        public Task<IAsyncCursor<BsonDocument>> ListDatabasesAsync(IClientSessionHandle session, ListDatabasesOptions options, CancellationToken cancellationToken = default) => _innerClient.ListDatabasesAsync(session, options, cancellationToken);

        public ICluster Cluster => _innerClient.Cluster;
        public MongoClientSettings Settings => _innerClient.Settings;

        public IMongoClient WithReadConcern(ReadConcern readConcern) => _innerClient.WithReadConcern(readConcern);
        public IMongoClient WithReadPreference(ReadPreference readPreference) => _innerClient.WithReadPreference(readPreference);
        public IMongoClient WithWriteConcern(WriteConcern writeConcern) => _innerClient.WithWriteConcern(writeConcern);

        public void DropDatabase(string name, CancellationToken cancellationToken = default) => _innerClient.DropDatabase(name, cancellationToken);
        public void DropDatabase(IClientSessionHandle session, string name, CancellationToken cancellationToken = default) => _innerClient.DropDatabase(session, name, cancellationToken);
        public Task DropDatabaseAsync(string name, CancellationToken cancellationToken = default) => _innerClient.DropDatabaseAsync(name, cancellationToken);
        public Task DropDatabaseAsync(IClientSessionHandle session, string name, CancellationToken cancellationToken = default) => _innerClient.DropDatabaseAsync(session, name, cancellationToken);

        public IAsyncCursor<string> ListDatabaseNames(CancellationToken cancellationToken = default) => _innerClient.ListDatabaseNames(cancellationToken);
        public IAsyncCursor<string> ListDatabaseNames(ListDatabaseNamesOptions options, CancellationToken cancellationToken = default) => _innerClient.ListDatabaseNames(options, cancellationToken);
        public IAsyncCursor<string> ListDatabaseNames(IClientSessionHandle session, CancellationToken cancellationToken = default) => _innerClient.ListDatabaseNames(session, cancellationToken);
        public IAsyncCursor<string> ListDatabaseNames(IClientSessionHandle session, ListDatabaseNamesOptions options, CancellationToken cancellationToken = default) => _innerClient.ListDatabaseNames(session, options, cancellationToken);
        public Task<IAsyncCursor<string>> ListDatabaseNamesAsync(CancellationToken cancellationToken = default) => _innerClient.ListDatabaseNamesAsync(cancellationToken);
        public Task<IAsyncCursor<string>> ListDatabaseNamesAsync(ListDatabaseNamesOptions options, CancellationToken cancellationToken = default) => _innerClient.ListDatabaseNamesAsync(options, cancellationToken);
        public Task<IAsyncCursor<string>> ListDatabaseNamesAsync(IClientSessionHandle session, CancellationToken cancellationToken = default) => _innerClient.ListDatabaseNamesAsync(session, cancellationToken);
        public Task<IAsyncCursor<string>> ListDatabaseNamesAsync(IClientSessionHandle session, ListDatabaseNamesOptions options, CancellationToken cancellationToken = default) => _innerClient.ListDatabaseNamesAsync(session, options, cancellationToken);

        // BulkWrite methods (non-generic BulkWriteModel, correct return type)
        public ClientBulkWriteResult BulkWrite(IReadOnlyList<BulkWriteModel> requests, ClientBulkWriteOptions options = null, CancellationToken cancellationToken = default) => _innerClient.BulkWrite(requests, options, cancellationToken);
        public ClientBulkWriteResult BulkWrite(IClientSessionHandle session, IReadOnlyList<BulkWriteModel> requests, ClientBulkWriteOptions options = null, CancellationToken cancellationToken = default) => _innerClient.BulkWrite(session, requests, options, cancellationToken);
        public Task<ClientBulkWriteResult> BulkWriteAsync(IReadOnlyList<BulkWriteModel> requests, ClientBulkWriteOptions options = null, CancellationToken cancellationToken = default) => _innerClient.BulkWriteAsync(requests, options, cancellationToken);
        public Task<ClientBulkWriteResult> BulkWriteAsync(IClientSessionHandle session, IReadOnlyList<BulkWriteModel> requests, ClientBulkWriteOptions options = null, CancellationToken cancellationToken = default) => _innerClient.BulkWriteAsync(session, requests, options, cancellationToken);

        // Watch methods (generic)
        public IChangeStreamCursor<TResult> Watch<TResult>(PipelineDefinition<ChangeStreamDocument<BsonDocument>, TResult> pipeline, ChangeStreamOptions options = null, CancellationToken cancellationToken = default) => _innerClient.Watch(pipeline, options, cancellationToken);
        public IChangeStreamCursor<TResult> Watch<TResult>(IClientSessionHandle session, PipelineDefinition<ChangeStreamDocument<BsonDocument>, TResult> pipeline, ChangeStreamOptions options = null, CancellationToken cancellationToken = default) => _innerClient.Watch(session, pipeline, options, cancellationToken);
        public Task<IChangeStreamCursor<TResult>> WatchAsync<TResult>(PipelineDefinition<ChangeStreamDocument<BsonDocument>, TResult> pipeline, ChangeStreamOptions options = null, CancellationToken cancellationToken = default) => _innerClient.WatchAsync(pipeline, options, cancellationToken);
        public Task<IChangeStreamCursor<TResult>> WatchAsync<TResult>(IClientSessionHandle session, PipelineDefinition<ChangeStreamDocument<BsonDocument>, TResult> pipeline, ChangeStreamOptions options = null, CancellationToken cancellationToken = default) => _innerClient.WatchAsync(session, pipeline, options, cancellationToken);

        // Session methods
        public IClientSessionHandle StartSession(ClientSessionOptions options = null, CancellationToken cancellationToken = default) => _innerClient.StartSession(options, cancellationToken);
        public Task<IClientSessionHandle> StartSessionAsync(ClientSessionOptions options = null, CancellationToken cancellationToken = default) => _innerClient.StartSessionAsync(options, cancellationToken);
    }
}
