import Synchronization
import SwiftCacheCore

/// A handle to a SwiftCache cluster.
///
/// ```swift
/// let db = try await Database.connect(coordinators: ["coord-0:4500", "coord-1:4500", "coord-2:4500"])
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
    private let counters = Counters()

    /// Client-side transaction statistics since this handle was created.
    public struct Statistics: Sendable {
        public var transactions: Int
        public var retries: Int
        public var conflicts: Int
        public var tooOld: Int
        public var unknownResults: Int
    }

    public var statistics: Statistics {
        Statistics(transactions: counters.transactions.load(ordering: .relaxed), retries: counters.retries.load(ordering: .relaxed),
                   conflicts: counters.conflicts.load(ordering: .relaxed), tooOld: counters.tooOld.load(ordering: .relaxed),
                   unknownResults: counters.unknown.load(ordering: .relaxed))
    }

    final class Counters: Sendable {
        let transactions = Atomic<Int>(0)
        let retries = Atomic<Int>(0)
        let conflicts = Atomic<Int>(0)
        let tooOld = Atomic<Int>(0)
        let unknown = Atomic<Int>(0)
    }

    /// Creates a database handle over a custom transport (used by the simulator and tests).
    /// `coordinators` are `host:port` addresses of the cluster's coordinators.
    public init(coordinators: [String], transport: any ClientTransport, options: DatabaseOptions = DatabaseOptions()) {
        self.client = ClusterClient(coordinators: coordinators, transport: transport, options: options)
        self.options = options
    }

    // MARK: Transactions

    /// Runs `body` in a transaction and commits it, retrying the whole body on retryable
    /// errors (conflicts, too-old read versions, recoveries) with exponential backoff. The
    /// body may run more than once, so it should only have side effects through the
    /// transaction.
    ///
    /// With `idempotent: true` the operation gets an id that is recorded atomically with
    /// its commit, so a retry after `commitUnknownResult` can never apply it twice: the
    /// retry finds the record and returns the result of the attempt that committed.
    @discardableResult
    public func transaction<T>(idempotent: Bool = false, _ body: (Transaction) async throws -> T) async throws -> T {
        try await run(idempotent: idempotent, body).result
    }

    /// Like `transaction`, but also returns the commit's versionstamp (nil if read-only).
    public func transactionWithVersionstamp<T>(idempotent: Bool = false, _ body: (Transaction) async throws -> T) async throws -> (result: T, versionstamp: Versionstamp?) {
        try await run(idempotent: idempotent, body)
    }

    /// Where one `transaction` call spent its time (for benchmarks and diagnostics).
    public struct TransactionReport: Sendable {
        /// Attempts made (1 = committed first time).
        public var attempts = 0
        /// Errors that caused a retry, in order.
        public var retryReasons: [SwiftCacheError] = []
        /// Time spent sleeping between attempts (µs).
        public var backoffMicros: UInt64 = 0
        /// Time spent waiting for read versions, storage reads and commits, summed over
        /// attempts (µs).
        public var readVersionMicros: UInt64 = 0
        public var readMicros: UInt64 = 0
        public var commitMicros: UInt64 = 0
    }

    /// Like `transaction`, but also reports attempts, backoff and per-phase time, whether
    /// it succeeds or fails.
    public func transactionWithReport<T>(idempotent: Bool = false, _ body: (Transaction) async throws -> T) async -> (result: Result<T, any Error>, report: TransactionReport) {
        var report = TransactionReport()
        do {
            let r = try await run(idempotent: idempotent, report: &report, body)
            return (.success(r.result), report)
        } catch {
            return (.failure(error), report)
        }
    }

    /// Runs a read-only transaction pinned to a historical `version` (e.g. one returned
    /// by `readVersion()` or a versionstamp's `version`). Every read sees the database
    /// exactly as it was at that version. Fails with `transactionTooOld` if the version is
    /// older than the cluster's MVCC window. Not retried on `transactionTooOld`.
    public func read<T>(atVersion version: Version, _ body: (Transaction) async throws -> T) async throws -> T {
        try await run(pinnedVersion: version, idempotent: false, body).result
    }

    /// Task executors, one per client I/O thread. Running work on one of them (e.g.
    /// `Task(executorPreference: db.taskExecutors[i]) { ... }`) keeps each request on a
    /// single thread from call to reply, which roughly halves client CPU per operation
    /// under load. Optional: the client works from any executor. Empty in simulation.
    public var taskExecutors: [any TaskExecutor] { client.transport.taskExecutors }

    /// Creates a transaction you drive manually (call `commit()` yourself; no retries).
    public func createTransaction() -> Transaction {
        Transaction(client: client)
    }

    private func randomID() -> Bytes {
        (0..<2).flatMap { _ in LittleEndianInt64.encode(Int64(bitPattern: client.transport.random(.max))) }
    }

    private func run<T>(pinnedVersion: Version? = nil, idempotent: Bool,
                        _ body: (Transaction) async throws -> T) async throws -> (result: T, versionstamp: Versionstamp?) {
        var report = TransactionReport()
        return try await run(pinnedVersion: pinnedVersion, idempotent: idempotent, report: &report, body)
    }

    private func run<T>(pinnedVersion: Version? = nil, idempotent: Bool, report: inout TransactionReport,
                        _ body: (Transaction) async throws -> T) async throws -> (result: T, versionstamp: Versionstamp?) {
        counters.transactions.add(1, ordering: .relaxed)
        let operationID: Bytes? = idempotent ? randomID() : nil
        // Results of attempts whose commit outcome is unknown, by attempt id.
        var unknownAttempts: [UInt64: T] = [:]
        // The lowest read version among those attempts: any of them that committed did so
        // at a version above it, so its batch record lies above it.
        var unknownSince: Version = .max
        var tries = 0
        var backoff = options.initialBackoff
        while true {
            let attemptID = client.transport.random(.max) | 1
            let tr = Transaction(client: client, readVersion: pinnedVersion)
            report.attempts += 1
            defer { report.add(tr.timings) }
            if let operationID { tr.makeIdempotent(id: operationID, attemptID: attemptID, retention: options.idempotencyRetention) }
            do {
                if !unknownAttempts.isEmpty, let record = try await tr.idempotencyRecord(since: unknownSince),
                   let result = unknownAttempts[record.attemptID] {
                    // An earlier attempt did commit.
                    return (result, record.stamp)
                }
                let result = try await body(tr)
                do {
                    let stamp = try await tr.commit()
                    return (result, stamp)
                } catch SwiftCacheError.commitUnknownResult where operationID != nil {
                    unknownAttempts[attemptID] = result
                    if let v = tr.commitReadVersion { unknownSince = min(unknownSince, v) } else { unknownSince = 0 }
                    throw SwiftCacheError.commitUnknownResult
                }
            } catch let error as SwiftCacheError {
                if error == .transactionTooOld { client.forgetRecentVersion() }
                switch error {
                case .notCommitted: counters.conflicts.add(1, ordering: .relaxed)
                case .transactionTooOld: counters.tooOld.add(1, ordering: .relaxed)
                case .commitUnknownResult: counters.unknown.add(1, ordering: .relaxed)
                default: break
                }
                let retryable = error.isRetryable && !(pinnedVersion != nil && error == .transactionTooOld)
                guard retryable else { throw error }
                tries += 1
                counters.retries.add(1, ordering: .relaxed)
                report.retryReasons.append(error)
                if tries >= options.maxRetries { throw SwiftCacheError.retryLimitExceeded(last: error.description) }
                if tries == 1 && error == .notCommitted { continue }  // retry a first conflict right away
                let jitter = client.transport.random(backoff / 2 + 1)
                let t0 = client.transport.now()
                try await client.transport.sleep(micros: backoff / 2 + jitter)
                report.backoffMicros += client.transport.now() &- t0
                backoff = min(options.maxBackoff, backoff * 2)
            }
        }
    }

    // MARK: Versions

    /// The latest committed version: every commit with a smaller-or-equal version is
    /// visible to a read at this version. Use it as a consistency token: pass it to
    /// `read(atVersion:)` later to see exactly this state again (within the MVCC window).
    public func readVersion() async throws -> Version {
        try await transaction { tr in try await tr.readVersion() }
    }

    // MARK: Single-key conveniences (each is its own, idempotent, transaction)

    public func get(_ key: some CacheKey) async throws -> Bytes? {
        try await getVersioned(key)?.bytes
    }

    public func get<T: Decodable>(_ key: some CacheKey, as type: T.Type) async throws -> T? {
        guard let bytes = try await get(key) else { return nil }
        return try ValueCodec.decode(type, from: bytes)
    }

    /// Reads a key and the versionstamp of the commit that last wrote it.
    ///
    /// A single strictly serializable read: a fresh read version (so it sees every commit
    /// acknowledged before the call) and one storage read, retried like a transaction —
    /// without building a transaction.
    public func getVersioned(_ key: some CacheKey) async throws -> VersionedEntry? {
        let k = key.cacheKeyBytes
        var tries = 0
        var backoff = options.initialBackoff
        while true {
            do {
                // One round trip: ask for a read version and, in parallel, for the latest
                // value a replica has applied (with its version A). If A ≥ the read version
                // R, the read is linearized at A — a committed state no older than any
                // commit acknowledged before this call, observed before the call returns.
                // Otherwise (a lagging replica) read at R.
                let topology = try await client.topology()
                let latest = ResultBox<ClusterClient.LatestValue?>()
                client.readLatest(k, topology: topology, into: latest)
                let version = try await client.readVersion()
                if let l = await latest.value, l.version >= version {
                    return l.value.map { VersionedEntry(key: k, bytes: $0, versionstamp: l.stamp) }
                }
                let (value, stamp) = try await client.read(k, at: version)
                return value.map { VersionedEntry(key: k, bytes: $0, versionstamp: stamp) }
            } catch let error as SwiftCacheError where error.isRetryable {
                tries += 1
                if tries >= options.maxRetries { throw SwiftCacheError.retryLimitExceeded(last: error.description) }
                let jitter = client.transport.random(backoff / 2 + 1)
                try await client.transport.sleep(micros: backoff / 2 + jitter)
                backoff = min(options.maxBackoff, backoff * 2)
            }
        }
    }

    /// Sets a key and returns the commit's versionstamp (which becomes the key's version).
    @discardableResult
    public func set<T: Encodable>(_ key: some CacheKey, _ value: T, ttl: Double? = nil) async throws -> Versionstamp {
        let bytes = try ValueCodec.encode(value)
        let (_, stamp) = try await transactionWithVersionstamp(idempotent: true) { tr in tr.set(key, bytes: bytes, ttl: ttl) }
        return stamp ?? .zero
    }

    @discardableResult
    public func clear(_ key: some CacheKey) async throws -> Versionstamp {
        let (_, stamp) = try await transactionWithVersionstamp(idempotent: true) { tr in tr.clear(key) }
        return stamp ?? .zero
    }

    /// Atomically adds to a little-endian Int64 counter. Never conflicts with other adds,
    /// and is idempotent: an add is applied exactly once even if a commit's outcome was
    /// unknown and it had to be retried.
    @discardableResult
    public func add(_ key: some CacheKey, _ delta: Int64) async throws -> Versionstamp {
        let (_, stamp) = try await transactionWithVersionstamp(idempotent: true) { tr in tr.add(key, delta) }
        return stamp ?? .zero
    }

    /// Atomically replaces the value of `key` if (and only if) the key's current
    /// versionstamp is `expectedVersion` (`nil` = the key must not exist).
    ///
    /// Returns the new versionstamp of the key. Throws `SwiftCacheError.versionMismatch`
    /// (carrying the actual versionstamp) if someone else wrote the key first. Conflicts
    /// during the swap are retried internally and the swap is idempotent, so a mismatch is
    /// always a real change made by someone else — never this call's own earlier attempt.
    @discardableResult
    public func compareAndSwap<T: Encodable>(key: some CacheKey, expectedVersion: Versionstamp?, newValue: T, ttl: Double? = nil) async throws -> Versionstamp {
        let bytes = try ValueCodec.encode(newValue)
        let (_, stamp) = try await transactionWithVersionstamp(idempotent: true) { tr in
            try await tr.compareAndSwap(key: key, expectedVersion: expectedVersion, newValue: bytes, ttl: ttl)
        }
        return stamp ?? .zero
    }

    /// Deletes `key` only if its versionstamp is `expectedVersion`.
    @discardableResult
    public func compareAndDelete(key: some CacheKey, expectedVersion: Versionstamp) async throws -> Versionstamp {
        let (_, stamp) = try await transactionWithVersionstamp(idempotent: true) { tr in
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

    /// Cluster status from the cluster controller (roles, topology, data movement).
    public func status() async throws -> ClusterStatus {
        try await client.status()
    }

    /// Per-server statistics (key counts, memory, versions).
    public func storageStats(_ endpoint: String) async throws -> StorageStats {
        try await client.storageStats(endpoint)
    }
}

extension Database.TransactionReport {
    mutating func add(_ t: Transaction.Timings) {
        readVersionMicros += t.readVersion
        readMicros += t.reads
        commitMicros += t.commit
    }
}
