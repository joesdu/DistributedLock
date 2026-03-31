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

    // Safe static caches: BsonDateTime and BsonValue wrappers are immutable,
    // so they can be safely shared across threads and driver calls without risk of mutation.
    private static readonly BsonDateTime EpochBsonDateTime = new(EpochUtc);
    private static readonly BsonValue ExpiresAtFieldRef = "$expiresAt";
    private static readonly BsonValue AcquiredAtFieldRef = "$acquiredAt";
    private static readonly BsonValue FencingTokenFieldRef = "$fencingToken";
    private static readonly BsonValue LockIdFieldRef = "$lockId";
    private static readonly BsonValue NowRef = "$$NOW";

    private readonly string _collectionName;
    private readonly MongoDistributedLockOptions _options;
    private readonly Lazy<IMongoCollection<MongoLockDocument>> _collection;

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
        var validatedDatabase = database ?? throw new ArgumentNullException(nameof(database));
        this._collectionName = collectionName ?? throw new ArgumentNullException(nameof(collectionName));
        // From what I can tell, modern (and all supported) MongoDB versions have no limits on index keys or
        // _id lengths other than the 16MB document limit. This is so high that providing "safe name" functionality as a fallback doesn't
        // see worth it.
        this.Key = key ?? throw new ArgumentNullException(nameof(key));
        this._options = options;
        this._collection = new(() => validatedDatabase.GetCollection<MongoLockDocument>(this._collectionName));
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

        // Use a unique token per acquisition attempt (like Redis' value token)
        var lockId = Guid.NewGuid().ToString("N");

        var collection = this._collection.Value;

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
            ? collection.FindOneAndUpdate(filter, update, options, cancellationToken)
            : await collection.FindOneAndUpdateAsync(filter, update, options, cancellationToken).ConfigureAwait(false);

        // Verify we actually got the lock
        if (result?.LockId == lockId)
        {
            // Fire-and-forget TTL index creation only on successful acquire to avoid
            // unnecessary DB calls when the lock is contended.
            _ = IndexInitializer.InitializeTtlIndex(collection);
            activity?.SetTag("lock.acquired", true);
            activity?.SetTag("lock.fencing_token", result.FencingToken);
            return new(new(this, lockId, collection), result.FencingToken);
        }
        activity?.SetTag("lock.acquired", false);
        return null;
    }

    private UpdateDefinition<MongoLockDocument> CreateAcquireUpdate(string lockId)
    {
        Invariant.Require(!this._options.Expiry.IsInfinite);

        // expired := ifNull(expiresAt, epoch) <= $$NOW
        var expiredOrMissing = new BsonDocument(
            "$lte",
            new BsonArray
            {
                new BsonDocument("$ifNull", new BsonArray { ExpiresAtFieldRef, EpochBsonDateTime }),
                NowRef
            }
        );

        var newExpiresAt = new BsonDocument(
            "$dateAdd",
            new BsonDocument
            {
                { "startDate", NowRef },
                { "unit", "millisecond" },
                { "amount", this._options.Expiry.InMilliseconds }
            }
        );

        // Increment fencing token only when acquiring a new lock
        var newFencingToken = new BsonDocument(
            "$add",
            new BsonArray
            {
                new BsonDocument("$ifNull", new BsonArray { FencingTokenFieldRef, 0L }),
                1L
            }
        );

        var setStage = new BsonDocument(
            "$set",
            new BsonDocument
            {
                // Only overwrite lock fields when the previous lock is expired/missing
                { nameof(lockId), new BsonDocument("$cond", new BsonArray { expiredOrMissing, lockId, LockIdFieldRef }) },
                { "expiresAt", new BsonDocument("$cond", new BsonArray { expiredOrMissing, newExpiresAt, ExpiresAtFieldRef }) },
                { "acquiredAt", new BsonDocument("$cond", new BsonArray { expiredOrMissing, NowRef, AcquiredAtFieldRef }) },
                { "fencingToken", new BsonDocument("$cond", new BsonArray { expiredOrMissing, newFencingToken, FencingTokenFieldRef }) }
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

        // Cached filter to avoid repeated allocations during renewal and release
        private readonly FilterDefinition<MongoLockDocument> _ownerFilter;
        // Lazily initialized: most locks are released quickly and never need renewal
        private PipelineUpdateDefinition<MongoLockDocument>? _renewUpdate;

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

            // important to set this last, since the monitor constructor will read other fields of this
            this._monitor = new(this);
        }

        public async ValueTask DisposeAsync()
        {
            try
            {
                await this._monitor.DisposeAsync().ConfigureAwait(false);
            }
            finally
            {
                await this.ReleaseLockAsync().ConfigureAwait(false);
            }
        }

        private async ValueTask ReleaseLockAsync()
        {
            if (SyncViaAsync.IsSynchronous)
            {
                // ReSharper disable once MethodHasAsyncOverload
                this._collection.DeleteOne(this._ownerFilter);
            }
            else
            {
                // Do not use HandleLostToken here: the monitor (and its CancellationTokenSource) is
                // already disposed before ReleaseLockAsync is called from DisposeAsync.
                await this._collection.DeleteOneAsync(this._ownerFilter, CancellationToken.None).ConfigureAwait(false);
            }
        }

        async Task<LeaseMonitor.LeaseState> LeaseMonitor.ILeaseHandle.RenewOrValidateLeaseAsync(CancellationToken cancellationToken)
        {
            // Lazily create the renewal update on first use to avoid allocations for short-lived locks
            this._renewUpdate ??= new PipelineUpdateDefinition<MongoLockDocument>(
                new[]
                {
                    new BsonDocument("$set", new BsonDocument("expiresAt", new BsonDocument(
                        "$dateAdd",
                        new BsonDocument
                        {
                            { "startDate", NowRef },
                            { "unit", "millisecond" },
                            { "amount", this._lock._options.Expiry.InMilliseconds }
                        }
                    )))
                }
            );

            var result = await this._collection.UpdateOneAsync(this._ownerFilter, this._renewUpdate, cancellationToken: cancellationToken).ConfigureAwait(false);
            return result.MatchedCount > 0 ? LeaseMonitor.LeaseState.Renewed : LeaseMonitor.LeaseState.Lost;
        }
    }
}
