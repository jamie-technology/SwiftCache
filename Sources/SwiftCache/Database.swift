import SwiftCacheCore

/// A handle to a SwiftCache cluster.
///
/// ```swift
/// let db = try await Database.connect(coordinator: "swiftcache-coordinator:7400")
///
/// // Transactions retry automatically on conflict.
/// try await db.transaction { tr in
///     let balance = try await tr.get("acct/alice", as: Int.self) ?? 0
///     try tr.set("acct/alice", balance - 10)
///     try tr.set("acct/bob", (try await tr.get("acct/bob", as: Int.self) ?? 0) + 10)
/// }
///
/// // Optimistic, versionstamp-based updates.
/// let entry = try await db.getVersioned("users/123")!
/// try await db.compareAndSwap(key: "users/123", expectedVersion: entry.versionstamp, newValue: updatedUser)
/// ```
public final class Database: Sendable {
    let client: ClusterClient
    public let options: DatabaseOptions

    /// Creates a database handle over a custom transport (used by the simulator and tests).
    public init(coordinator: String, transport: any ClientTransport, options: DatabaseOptions = DatabaseOptions()) {
        self.client = ClusterClient(coordinator: coordinator, transport: transport, options: options)
        self.options = options
    }

    // MARK: Transactions

    /// Runs `body` in a transaction and commits it, retrying the whole body on retryable
    /// errors (conflicts, too-old read versions, unknown commit results, recoveries) with
    /// exponential backoff. The body may run more than once, so it should only have side
    /// effects through the transaction.
    @discardableResult
    public func transaction<T>(_ body: (Transaction) async throws -> T) async throws -> T {
        try await retrying { tr in
            let result = try await body(tr)
            try await tr.commit()
            return result
        }
    }

    /// Like `transaction`, but also returns the commit's versionstamp (nil if read-only).
    public func transactionWithVersionstamp<T>(_ body: (Transaction) async throws -> T) async throws -> (result: T, versionstamp: Versionstamp?) {
        try await retrying { tr in
            let result = try await body(tr)
            let stamp = try await tr.commit()
            return (result, stamp)
        }
    }

    /// Runs a read-only transaction pinned to a historical `version` (e.g. one returned
    /// by `readVersion()` or a versionstamp's `version`). Every read sees the database
    /// exactly as it was at that version. Fails with `transactionTooOld` if the version is
    /// older than the cluster's MVCC window. Not retried on `transactionTooOld`.
    public func read<T>(atVersion version: Version, _ body: (Transaction) async throws -> T) async throws -> T {
        try await retrying(pinnedVersion: version) { tr in try await body(tr) }
    }

    /// Creates a transaction you drive manually (call `commit()` yourself; no retries).
    public func createTransaction() -> Transaction {
        Transaction(client: client)
    }

    private func retrying<T>(pinnedVersion: Version? = nil, retryUnknownResult: Bool = true,
                             _ attempt: (Transaction) async throws -> T) async throws -> T {
        var tries = 0
        var backoff = options.initialBackoff
        while true {
            let tr = Transaction(client: client, readVersion: pinnedVersion)
            do {
                return try await attempt(tr)
            } catch let error as SwiftCacheError {
                var retryable = error.isRetryable && !(pinnedVersion != nil && error == .transactionTooOld)
                if error == .commitUnknownResult && !retryUnknownResult { retryable = false }
                guard retryable else { throw error }
                tries += 1
                if tries >= options.maxRetries { throw SwiftCacheError.retryLimitExceeded(last: error.description) }
                let jitter = client.transport.random(backoff / 2 + 1)
                try await client.transport.sleep(micros: backoff / 2 + jitter)
                backoff = min(options.maxBackoff, backoff * 2)
            }
        }
    }

    // MARK: Versions

    /// The latest committed version: every commit with a smaller-or-equal version is
    /// visible to a read at this version. Use it as a consistency token: pass it to
    /// `read(atVersion:)` later to see exactly this state again (within the MVCC window).
    public func readVersion() async throws -> Version {
        try await retrying { tr in try await tr.readVersion() }
    }

    // MARK: Single-key conveniences (each is its own transaction)

    public func get(_ key: some CacheKey) async throws -> Bytes? {
        try await transaction { try await $0.get(key) }
    }

    public func get<T: Decodable>(_ key: some CacheKey, as type: T.Type) async throws -> T? {
        try await transaction { try await $0.get(key, as: type) }
    }

    /// Reads a key and the versionstamp of the commit that last wrote it.
    public func getVersioned(_ key: some CacheKey) async throws -> VersionedEntry? {
        try await transaction { try await $0.getVersioned(key) }
    }

    /// Sets a key and returns the commit's versionstamp (which becomes the key's version).
    @discardableResult
    public func set<T: Encodable>(_ key: some CacheKey, _ value: T, ttl: Double? = nil) async throws -> Versionstamp {
        let bytes = try ValueCodec.encode(value)
        let (_, stamp) = try await transactionWithVersionstamp { tr in tr.set(key, bytes: bytes, ttl: ttl) }
        return stamp ?? .zero
    }

    @discardableResult
    public func clear(_ key: some CacheKey) async throws -> Versionstamp {
        let (_, stamp) = try await transactionWithVersionstamp { tr in tr.clear(key) }
        return stamp ?? .zero
    }

    /// Atomically adds to a little-endian Int64 counter; never conflicts.
    ///
    /// Unlike the other conveniences this is not idempotent, so a commit whose outcome is
    /// unknown (e.g. the coordinator crashed mid-commit) is *not* retried: you get
    /// `SwiftCacheError.commitUnknownResult` and decide yourself.
    @discardableResult
    public func add(_ key: some CacheKey, _ delta: Int64) async throws -> Versionstamp {
        let stamp = try await retrying(retryUnknownResult: false) { tr in
            tr.add(key, delta)
            return try await tr.commit()
        }
        return stamp ?? .zero
    }

    /// Atomically replaces the value of `key` if (and only if) the key's current
    /// versionstamp is `expectedVersion` (`nil` = the key must not exist).
    ///
    /// Returns the new versionstamp of the key. Throws `SwiftCacheError.versionMismatch`
    /// (carrying the actual versionstamp) if someone else wrote the key first. Conflicts
    /// that happen *during* the swap are retried internally, so a mismatch is always a
    /// real, observed change.
    @discardableResult
    public func compareAndSwap<T: Encodable>(key: some CacheKey, expectedVersion: Versionstamp?, newValue: T, ttl: Double? = nil) async throws -> Versionstamp {
        let bytes = try ValueCodec.encode(newValue)
        let (_, stamp) = try await transactionWithVersionstamp { tr in
            try await tr.compareAndSwap(key: key, expectedVersion: expectedVersion, newValue: bytes, ttl: ttl)
        }
        return stamp ?? .zero
    }

    /// Deletes `key` only if its versionstamp is `expectedVersion`.
    @discardableResult
    public func compareAndDelete(key: some CacheKey, expectedVersion: Versionstamp) async throws -> Versionstamp {
        let (_, stamp) = try await transactionWithVersionstamp { tr in
            try await tr.compareAndDelete(key: key, expectedVersion: expectedVersion)
        }
        return stamp ?? .zero
    }

    /// Reads key/values with the given prefix.
    public func getRange(prefix: some CacheKey, limit: Int = 1000) async throws -> [VersionedEntry] {
        try await transaction { try await $0.getRange(prefix: prefix, limit: limit) }
    }

    public func getRange(from begin: some CacheKey, to end: some CacheKey, limit: Int = 1000) async throws -> [VersionedEntry] {
        try await transaction { try await $0.getRange(from: begin, to: end, limit: limit) }
    }

    // MARK: Admin

    /// Cluster status from the coordinator (topology, servers, data movement).
    public func status() async throws -> ClusterStatus {
        try await client.status()
    }

    /// Per-server statistics (key counts, memory, versions).
    public func storageStats(_ endpoint: String) async throws -> StorageStats {
        try await client.storageStats(endpoint)
    }
}
