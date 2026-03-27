using Medallion.Threading.Internal;
using MongoDB.Bson;
using MongoDB.Driver;
using System.Diagnostics;

namespace Medallion.Threading.MongoDB;

/// <summary>
/// Implements a <see cref="IDistributedLock" /> using MongoDB.
/// </summary>
public sealed partial class MongoDistributedLock : IInternalDistributedLock<MongoDistributedLockHandle>
{
    internal const string DefaultCollectionName = "distributed_locks";

    private static readonly DateTime EpochUtc = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
    private static readonly MongoIndexInitializer IndexInitializer = new();

    /// <summary>
    /// ActivitySource for distributed tracing and diagnostics
    /// </summary>
    internal static readonly ActivitySource ActivitySource = new("DistributedLock.MongoDB", "1.0.0");

    private readonly string _collectionName;
    private readonly MongoDistributedLockOptions _options;
    private readonly IMongoCollection<MongoLockDocument> _collection;

    // Cached immutable BsonDocument sub-expressions to reduce GC pressure on hot paths
    private static readonly BsonDocument ExpiredOrMissingExpr = new(
        "$lte",
        new BsonArray
        {
            new BsonDocument("$ifNull", new BsonArray { "$expiresAt", new BsonDateTime(EpochUtc) }),
            "$$NOW"
        }
    );

    private static readonly BsonDocument NewFencingTokenExpr = new(
        "$add",
        new BsonArray
        {
            new BsonDocument("$ifNull", new BsonArray { "$fencingToken", 0L }),
            1L
        }
    );

    private readonly BsonDocument _newExpiresAtExpr;

    /// <summary>
    /// The MongoDB key used to implement the lock
    /// </summary>
    public string Key { get; }

    /// <summary>
    /// Implements <see cref="IDistributedLock.Name" />
    /// </summary>
    public string Name => this.Key;

    /// <summary>
    /// Constructs a lock named <paramref name="key" /> using the provided <paramref name="database" /> and <paramref name="options" />.
    /// The locks will be stored in a collection named "distributed_locks" by default.
    /// </summary>
    public MongoDistributedLock(string key, IMongoDatabase database, Action<MongoDistributedSynchronizationOptionsBuilder>? options = null)
        : this(key, database, DefaultCollectionName, options) { }

    /// <summary>
    /// Constructs a lock named <paramref name="key" /> using the provided <paramref name="database" />, <paramref name="collectionName" />, and
    /// <paramref name="options" />.
    /// </summary>
    public MongoDistributedLock(string key, IMongoDatabase database, string collectionName, Action<MongoDistributedSynchronizationOptionsBuilder>? options = null)
        : this(key, database, collectionName, MongoDistributedSynchronizationOptionsBuilder.GetOptions(options)) { }

    internal MongoDistributedLock(string key, IMongoDatabase database, string collectionName, MongoDistributedLockOptions options)
    {
        var database1 = database ?? throw new ArgumentNullException(nameof(database));
        this._collectionName = collectionName ?? throw new ArgumentNullException(nameof(collectionName));
        // From what I can tell, modern (and all supported) MongoDB versions have no limits on index keys or
        // _id lengths other than the 16MB document limit. This is so high that providing "safe name" functionality as a fallback doesn't
        // see worth it.
        this.Key = key ?? throw new ArgumentNullException(nameof(key));
        this._options = options;
        this._collection = database1.GetCollection<MongoLockDocument>(this._collectionName);
        this._newExpiresAtExpr = new BsonDocument(
            "$dateAdd",
            new BsonDocument
            {
                { "startDate", "$$NOW" },
                { "unit", "millisecond" },
                { "amount", this._options.Expiry.InMilliseconds }
            }
        );
    }

    ValueTask<MongoDistributedLockHandle?> IInternalDistributedLock<MongoDistributedLockHandle>.InternalTryAcquireAsync(TimeoutValue timeout, CancellationToken cancellationToken) =>
        BusyWaitHelper.WaitAsync(this,
            (@this, ct) => @this.TryAcquireAsync(ct),
            timeout,
            minSleepTime: this._options.MinBusyWaitSleepTime,
            maxSleepTime: this._options.MaxBusyWaitSleepTime,
            cancellationToken);

    private async ValueTask<MongoDistributedLockHandle?> TryAcquireAsync(CancellationToken cancellationToken)
    {
        using var activity = ActivitySource.StartActivity(nameof(MongoDistributedLock) + ".TryAcquire");
        activity?.SetTag("lock.key", this.Key);
        activity?.SetTag("lock.collection", this._collectionName);

        // Ensure TTL index exists (fire-and-forget, idempotent). Triggered on every attempt
        // so that cleanup is set up even when all acquisition attempts fail.
        _ = IndexInitializer.InitializeTtlIndex(this._collection);

        // Use a unique token per acquisition attempt (like Redis' value token)
        var lockId = Guid.NewGuid().ToString("N");

        // We avoid exception-driven contention (DuplicateKey) by using a single upsert on {_id == Key}
        // and an update pipeline that only overwrites fields when the existing lock is expired.
        // This is conceptually similar to Redis: SET key value NX PX <expiry>.
        var filter = Builders<MongoLockDocument>.Filter.Eq(d => d.Id, this.Key);
        var update = this.CreateAcquireUpdate(lockId);
        var options = new FindOneAndUpdateOptions<MongoLockDocument>
        {
            IsUpsert = true,
            ReturnDocument = ReturnDocument.After
        };

        var result = SyncViaAsync.IsSynchronous
            ? this._collection.FindOneAndUpdate(filter, update, options, cancellationToken)
            : await this._collection.FindOneAndUpdateAsync(filter, update, options, cancellationToken).ConfigureAwait(false);

        // Verify we actually got the lock
        if (result?.LockId == lockId)
        {
            activity?.SetTag("lock.acquired", true);
            activity?.SetTag("lock.fencing_token", result.FencingToken);
            return new(new(this, lockId, this._collection), result.FencingToken);
        }
        activity?.SetTag("lock.acquired", false);
        return null;
    }

    private UpdateDefinition<MongoLockDocument> CreateAcquireUpdate(string lockId)
    {
        // ExpiredOrMissingExpr, _newExpiresAtExpr, and NewFencingTokenExpr are pre-cached
        // immutable BsonDocuments to reduce allocations on the busy-wait hot path.
        var setStage = new BsonDocument(
            "$set",
            new BsonDocument
            {
                // Only overwrite lock fields when the previous lock is expired/missing
                { nameof(lockId), new BsonDocument("$cond", new BsonArray { ExpiredOrMissingExpr, lockId, "$lockId" }) },
                { "expiresAt", new BsonDocument("$cond", new BsonArray { ExpiredOrMissingExpr, this._newExpiresAtExpr, "$expiresAt" }) },
                { "acquiredAt", new BsonDocument("$cond", new BsonArray { ExpiredOrMissingExpr, "$$NOW", "$acquiredAt" }) },
                { "fencingToken", new BsonDocument("$cond", new BsonArray { ExpiredOrMissingExpr, NewFencingTokenExpr, "$fencingToken" }) }
            }
        );

        return new PipelineUpdateDefinition<MongoLockDocument>(new[] { setStage });
    }

    /// <summary>
    /// Inner handle that performs actual lock management and release.
    /// Separated from the outer handle so it can be registered with ManagedFinalizerQueue.
    /// </summary>
    internal sealed class InnerHandle : IAsyncDisposable, LeaseMonitor.ILeaseHandle
    {
        private readonly MongoDistributedLock _lock;
        private readonly IMongoCollection<MongoLockDocument> _collection;
        private readonly LeaseMonitor _monitor;

        // Cached filter and update definitions to avoid repeated allocations during renewal cycles
        private readonly FilterDefinition<MongoLockDocument> _ownerFilter;
        private readonly PipelineUpdateDefinition<MongoLockDocument> _renewUpdate;

        public CancellationToken HandleLostToken => this._monitor.HandleLostToken;

        TimeoutValue LeaseMonitor.ILeaseHandle.LeaseDuration => this._lock._options.Expiry;
        TimeoutValue LeaseMonitor.ILeaseHandle.MonitoringCadence => this._lock._options.ExtensionCadence;

        public InnerHandle(MongoDistributedLock @lock, string lockId, IMongoCollection<MongoLockDocument> collection)
        {
            this._lock = @lock;
            this._collection = collection;

            // Cache the filter that identifies this specific lock ownership
            this._ownerFilter = Builders<MongoLockDocument>.Filter.Eq(d => d.Id, @lock.Key)
                & Builders<MongoLockDocument>.Filter.Eq(d => d.LockId, lockId);

            // Cache the renewal update pipeline (expiry is fixed for the lock's lifetime)
            this._renewUpdate = new PipelineUpdateDefinition<MongoLockDocument>(
                new[] { new BsonDocument("$set", new BsonDocument("expiresAt", @lock._newExpiresAtExpr)) }
            );

            // important to set this last, since the monitor constructor will read other fields of this
            this._monitor = new(this);
        }

        public async ValueTask DisposeAsync()
        {
            try { await this._monitor.DisposeAsync().ConfigureAwait(false); }
            finally { await this.ReleaseLockAsync().ConfigureAwait(false); }
        }

        private async ValueTask ReleaseLockAsync()
        {
            try
            {
                if (SyncViaAsync.IsSynchronous)
                {
                    // ReSharper disable once MethodHasAsyncOverload
                    this._collection.DeleteOne(this._ownerFilter);
                }
                else
                {
                    await this._collection.DeleteOneAsync(this._ownerFilter, this.HandleLostToken).ConfigureAwait(false);
                }
            }
            catch (Exception)
            {
                // Release failure is non-fatal: the TTL index will eventually clean up expired documents.
                // Swallowing exceptions here prevents surprising callers during Dispose.
            }
        }

        async Task<LeaseMonitor.LeaseState> LeaseMonitor.ILeaseHandle.RenewOrValidateLeaseAsync(CancellationToken cancellationToken)
        {
            var result = await this._collection.UpdateOneAsync(this._ownerFilter, this._renewUpdate, cancellationToken: cancellationToken).ConfigureAwait(false);
            return result.MatchedCount > 0 ? LeaseMonitor.LeaseState.Renewed : LeaseMonitor.LeaseState.Lost;
        }
    }
}