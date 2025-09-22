using MongoDB.Driver.Core.Configuration;
using MongoDB.Driver.Core.Events;
using System.Collections.Concurrent;
using System.Linq;


namespace Microsoft.Orleans.Providers.Mongo;

public interface IMongoPoolHealth
{
    int InUse { get; }
    int Available { get; }
    double UtilizationPercent { get; } 
}

public sealed class MongoPoolHealth : IMongoPoolHealth
{
    private sealed class Pool { public int InUse; public int Avail; }
    private readonly ConcurrentDictionary<string, Pool> _pools = new();

    public void AttachTo(ClusterBuilder cb)
    {
        // pool lifecycle
        cb.Subscribe<ConnectionPoolOpenedEvent>(e =>
            _pools.TryAdd(Key(e.ServerId.EndPoint), new Pool()));

        cb.Subscribe<ConnectionPoolClosedEvent>(e =>
            _pools.TryRemove(Key(e.ServerId.EndPoint), out _));

        // physical connection lifecycle
        cb.Subscribe<ConnectionCreatedEvent>(e =>
            Get(Key(e.ConnectionId.ServerId.EndPoint)).Avail++);

        cb.Subscribe<ConnectionClosedEvent>(e =>
            Get(Key(e.ConnectionId.ServerId.EndPoint)).Avail--);

        // checkout / checkin (older names in 3.2.1)
        cb.Subscribe<ConnectionPoolCheckedOutConnectionEvent>(e =>
        {
            var p = Get(Key(e.ConnectionId.ServerId.EndPoint));
            p.InUse++; p.Avail--;
        });

        cb.Subscribe<ConnectionPoolCheckedInConnectionEvent>(e =>
        {
            var p = Get(Key(e.ConnectionId.ServerId.EndPoint));
            p.InUse--; p.Avail++;
        });
    }

    public int InUse => _pools.Values.Sum(p => p.InUse);
    public int Available => _pools.Values.Sum(p => p.Avail);

    public double UtilizationPercent
    {
        get
        {
            var inUse = InUse;
            var avail = Available;
            var total = inUse + avail;
            return total <= 0 ? 0 : (double)inUse / total * 100.0;
        }
    }

    private static string Key(System.Net.EndPoint ep) => ep.ToString() ?? "unknown";
    private Pool Get(string key) => _pools.GetOrAdd(key, _ => new Pool());
}

