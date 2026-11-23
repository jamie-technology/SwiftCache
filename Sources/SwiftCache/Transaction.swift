import Synchronization
import SwiftCacheCore

/// A serializable transaction with FoundationDB-style semantics.
///
/// * All reads see one consistent snapshot: the database as of the transaction's read
///   version.
/// * Writes are buffered locally and are visible to the transaction's own later reads
///   (read-your-writes).
/// * At commit, the transaction fails with `notCommitted` if any key or range it read was
///   written by a transaction that committed after its read version. `Database.transaction`
///   retries such failures automatically.
/// * A transaction that commits writes gets a `Versionstamp`, recorded on every key it wrote.
///
/// Transactions are safe to use from multiple tasks (e.g. issue reads concurrently with
/// `async let`), but should not be used after commit.
public final class Transaction: Sendable {
    enum LocalWrite: Sendable {
        /// The key's value is known locally (nil = cleared).
        case value(Bytes?)
        /// Only a delta is known; the base must be read from storage.
        case add(Int64)
    }

    struct State: Sendable {
        var readVersion: Version?
        var readVersionPinned = false
        var writes: [ByteKey: LocalWrite] = [:]
        var mutations: [Mutation] = []
        var readConflictKeys: Set<ByteKey> = []
        var readConflictRanges: [KeyRange] = []
        var writeConflictKeys: Set<ByteKey> = []
        var committedStamp: Versionstamp?
        var committed = false
        var idempotencyID: Bytes?
        var attemptID: UInt64 = 0
        var idempotencyRetention: UInt32 = 0
        var timings = Timings()
        var commitReadVersion: Version?
    }

    /// Time this transaction spent waiting on the cluster (µs).
    struct Timings: Sendable {
        var readVersion: UInt64 = 0
        var reads: UInt64 = 0
        var commit: UInt64 = 0
    }
    var timings: Timings { state.withLock { $0.timings } }

    let client: ClusterClient
    let state = Mutex(State())

    init(client: ClusterClient, readVersion: Version? = nil) {
        self.client = client
        if let readVersion {
            state.withLock {
                $0.readVersion = readVersion
                $0.readVersionPinned = true
            }
        }
    }

    // MARK: Versions

    /// The version this transaction reads at. Fetched from the coordinator on first use
    /// (it is the latest committed version at that moment) unless set explicitly.
    public func readVersion() async throws -> Version {
        if let v = state.withLock({ $0.readVersion }) { return v }
        // Concurrent first reads may both fetch; the first result wins so every read in
        // the transaction uses the same version.
        let t0 = client.transport.now()
        let v = try await client.readVersion()
        let elapsed = client.transport.now() &- t0
        return state.withLock { s in
            s.timings.readVersion += elapsed
            if s.readVersion == nil { s.readVersion = v }
            return s.readVersion!
        }
    }

    /// Pins the read version, e.g. to read a historical snapshot or to continue reading
    /// at the same version as an earlier transaction. Must be called before any read.
    public func setReadVersion(_ version: Version) {
        state.withLock {
            $0.readVersion = version
            $0.readVersionPinned = true
        }
    }

    var isReadVersionPinned: Bool { state.withLock { $0.readVersionPinned } }

    /// Marks this transaction as an attempt of a logical operation with id `id`.
    func makeIdempotent(id: Bytes, attemptID: UInt64, retention: UInt32) {
        state.withLock {
            $0.idempotencyID = id
            $0.attemptID = attemptID
            $0.idempotencyRetention = retention
        }
    }

    /// The operation's idempotency record at this transaction's read version, if an
    /// earlier attempt committed: searches the batch records of commits at versions above
    /// `since` (the lowest read version of the attempts whose outcome is unknown) up to
    /// this transaction's read version.
    func idempotencyRecord(since: Version) async throws -> (stamp: Versionstamp, attemptID: UInt64)? {
        guard let id = state.withLock({ $0.idempotencyID }) else { return nil }
        let version = try await readVersion()
        guard since < version else { return nil }
        var cursor = SystemKeys.idempotencyBatch(since &+ 1)
        let end = SystemKeys.idempotencyBatch(version &+ 1)
        while true {
            let (entries, more) = try await client.readRange(KeyRange(begin: cursor, end: end), at: version, limit: 1_000)
            for e in entries {
                guard let v = SystemKeys.idempotencyBatchVersion(e.key),
                      let list = SystemKeys.parseIdempotencyBatchValue(e.value) else { continue }
                if let hit = list.first(where: { $0.id == id }) {
                    return (Versionstamp(version: v, order: hit.order), hit.attemptID)
                }
            }
            guard more, let last = entries.last else { return nil }
            cursor = last.key.keyAfter
        }
    }

    /// The read version the last `commit()` sent (set even if its outcome is unknown).
    var commitReadVersion: Version? { state.withLock { $0.commitReadVersion } }

    private func checkUserKey(_ key: Bytes) {
        precondition(!SystemKeys.isSystem(key), "keys starting with 0xFF are reserved for the system")
    }

    /// The versionstamp assigned at commit (nil before commit, or for read-only transactions).
    public var committedVersionstamp: Versionstamp? { state.withLock { $0.committedStamp } }

    // MARK: Reads

    /// Reads a key's raw bytes. With `snapshot: true` the read adds no conflict.
    public func get(_ key: some CacheKey, snapshot: Bool = false) async throws -> Bytes? {
        try await getEntry(key.cacheKeyBytes, snapshot: snapshot)?.value
    }

    /// Reads a key and decodes it (JSON unless `T` is `String`, `[UInt8]` or `Data`).
    public func get<T: Decodable>(_ key: some CacheKey, as type: T.Type, snapshot: Bool = false) async throws -> T? {
        guard let bytes = try await get(key, snapshot: snapshot) else { return nil }
        return try ValueCodec.decode(type, from: bytes)
    }

    /// Reads a key together with the versionstamp of the commit that wrote it.
    /// Throws `invalidRequest` if this transaction has already written the key (its
    /// versionstamp isn't known until commit).
    public func getVersioned(_ key: some CacheKey, snapshot: Bool = false) async throws -> VersionedEntry? {
        let k = key.cacheKeyBytes
        if state.withLock({ $0.writes[ByteKey(k)] != nil }) {
            throw SwiftCacheError.invalidRequest("getVersioned on a key written in the same transaction")
        }
        guard let (value, stamp) = try await getEntry(k, snapshot: snapshot), let value else { return nil }
        return VersionedEntry(key: k, bytes: value, versionstamp: stamp ?? .zero)
    }

    /// Returns (value, stamp) honoring local writes; stamp is nil for locally-written values.
    private func getEntry(_ key: Bytes, snapshot: Bool) async throws -> (value: Bytes?, stamp: Versionstamp?)? {
        try checkUsable()
        let local = state.withLock { $0.writes[ByteKey(key)] }
        if case .value(let v) = local { return (v, nil) }
        let version = try await readVersion()
        let t0 = client.transport.now()
        let (value, stamp) = try await client.read(key, at: version)
        let elapsed = client.transport.now() &- t0
        state.withLock {
            $0.timings.reads += elapsed
            if !snapshot { _ = $0.readConflictKeys.insert(ByteKey(key)) }
        }
        if case .add(let delta) = local {
            return (LittleEndianInt64.encode(LittleEndianInt64.decode(value) &+ delta), nil)
        }
        return (value, value == nil ? nil : stamp)
    }

    /// Reads key/values in `[begin, end)` in key order (merging this transaction's writes).
    public func getRange(from begin: some CacheKey, to end: some CacheKey, limit: Int = 1000, snapshot: Bool = false) async throws -> [VersionedEntry] {
        try checkUsable()
        let range = KeyRange(begin: begin.cacheKeyBytes, end: end.cacheKeyBytes)
        guard bytesLess(range.begin, range.end), limit > 0 else { return [] }
        let version = try await readVersion()
        let local = state.withLock { s in s.writes.filter { range.contains($0.key.bytes) } }
        let clears = local.values.filter { if case .value(nil) = $0 { return true } else { return false } }.count
        let (entries, more) = try await client.readRange(range, at: version, limit: limit + clears)
        // Overlay local writes.
        var merged: [Bytes: (Bytes, Versionstamp)] = [:]
        for e in entries { merged[e.key] = (e.value, e.stamp) }
        // Local writes beyond the last server key (when truncated) can't be placed safely.
        let horizon: Bytes? = more ? entries.last?.key : nil
        for (k, w) in local {
            let key = k.bytes
            if let h = horizon, bytesLess(h, key) { continue }
            switch w {
            case .value(let v):
                if let v { merged[key] = (v, .zero) } else { merged[key] = nil }
            case .add(let delta):
                let base = merged[key]?.0
                merged[key] = (LittleEndianInt64.encode(LittleEndianInt64.decode(base) &+ delta), .zero)
            }
        }
        var result = merged.map { VersionedEntry(key: $0.key, bytes: $0.value.0, versionstamp: $0.value.1) }
        result.sort { bytesLess($0.key, $1.key) }
        let truncated = result.count > limit || more
        if result.count > limit { result = Array(result.prefix(limit)) }
        if !snapshot {
            let conflictEnd = truncated ? (result.last?.key.keyAfter ?? range.begin.keyAfter) : range.end
            state.withLock { $0.readConflictRanges.append(KeyRange(begin: range.begin, end: conflictEnd)) }
        }
        return result
    }

    /// Reads every key with the given prefix.
    public func getRange(prefix: some CacheKey, limit: Int = 1000, snapshot: Bool = false) async throws -> [VersionedEntry] {
        let p = prefix.cacheKeyBytes
        return try await getRange(from: p, to: p.prefixEnd ?? [0xFF, 0xFF, 0xFF, 0xFF], limit: limit, snapshot: snapshot)
    }

    // MARK: Writes

    /// Sets a key to raw bytes. A `ttl` (seconds) makes the value expire.
    public func set(_ key: some CacheKey, bytes value: Bytes, ttl: Double? = nil) {
        let k = key.cacheKeyBytes
        checkUserKey(k)
        let ttlMicros = ttl.map { UInt64(max(0, $0) * 1_000_000) } ?? 0
        state.withLock { s in
            s.writes[ByteKey(k)] = .value(value)
            s.mutations.append(.set(key: k, value: value, ttl: ttlMicros))
            s.writeConflictKeys.insert(ByteKey(k))
        }
    }

    /// Sets a key. `String`, `[UInt8]` and `Data` are stored raw; other values as JSON.
    public func set<T: Encodable>(_ key: some CacheKey, _ value: T, ttl: Double? = nil) throws {
        set(key, bytes: try ValueCodec.encode(value), ttl: ttl)
    }

    /// Deletes a key.
    public func clear(_ key: some CacheKey) {
        let k = key.cacheKeyBytes
        checkUserKey(k)
        state.withLock { s in
            s.writes[ByteKey(k)] = .value(nil)
            s.mutations.append(.clear(key: k))
            s.writeConflictKeys.insert(ByteKey(k))
        }
    }

    /// Atomically adds `delta` to the little-endian Int64 at `key` (missing = 0) without
    /// reading it, so concurrent adds never conflict with each other.
    public func add(_ key: some CacheKey, _ delta: Int64) {
        let k = key.cacheKeyBytes
        checkUserKey(k)
        state.withLock { s in
            switch s.writes[ByteKey(k)] {
            case .value(let v)?:
                s.writes[ByteKey(k)] = .value(LittleEndianInt64.encode(LittleEndianInt64.decode(v) &+ delta))
            case .add(let d)?:
                s.writes[ByteKey(k)] = .add(d &+ delta)
            case nil:
                s.writes[ByteKey(k)] = .add(delta)
            }
            s.mutations.append(.add(key: k, delta: delta))
            s.writeConflictKeys.insert(ByteKey(k))
        }
    }

    /// Writes `newValue` only if the key's current versionstamp equals `expectedVersion`
    /// (`nil` = the key must not exist). Throws `versionMismatch` otherwise. The check is
    /// a conflict-tracked read, so it stays valid until commit.
    public func compareAndSwap<T: Encodable>(key: some CacheKey, expectedVersion: Versionstamp?, newValue: T, ttl: Double? = nil) async throws {
        let current = try await getVersioned(key)
        guard current?.versionstamp == expectedVersion else {
            throw SwiftCacheError.versionMismatch(key: key.cacheKeyBytes, expected: expectedVersion, actual: current?.versionstamp)
        }
        try set(key, newValue, ttl: ttl)
    }

    /// Like `compareAndSwap`, but deletes the key on success.
    public func compareAndDelete(key: some CacheKey, expectedVersion: Versionstamp) async throws {
        let current = try await getVersioned(key)
        guard current?.versionstamp == expectedVersion else {
            throw SwiftCacheError.versionMismatch(key: key.cacheKeyBytes, expected: expectedVersion, actual: current?.versionstamp)
        }
        clear(key)
    }

    /// Adds a read conflict on `key` as if it had been read.
    public func addReadConflict(_ key: some CacheKey) {
        state.withLock { _ = $0.readConflictKeys.insert(ByteKey(key.cacheKeyBytes)) }
    }

    /// Adds a write conflict on `key` as if it had been written.
    public func addWriteConflict(_ key: some CacheKey) {
        state.withLock { _ = $0.writeConflictKeys.insert(ByteKey(key.cacheKeyBytes)) }
    }

    // MARK: Commit

    private func checkUsable() throws {
        if state.withLock({ $0.committed }) {
            throw SwiftCacheError.invalidRequest("transaction already committed")
        }
    }

    /// Commits the transaction. Returns its versionstamp, or nil if it made no writes
    /// (read-only transactions always succeed: their snapshot was consistent).
    @discardableResult
    public func commit() async throws -> Versionstamp? {
        try checkUsable()
        let s = state.withLock { $0 }
        if s.mutations.isEmpty && s.writeConflictKeys.isEmpty {
            state.withLock { $0.committed = true }
            return nil
        }
        // A transaction that read nothing has no read conflicts to check: its read version
        // only bounds the window in which *its own* idempotency key is checked, where an
        // older version is merely more conservative. So reuse a recent version instead of
        // a round trip (a write-only transaction is ordered by its commit version).
        let readVersion: Version
        if let v = state.withLock({ $0.readVersion }) {
            readVersion = v
        } else if s.readConflictKeys.isEmpty, s.readConflictRanges.isEmpty,
                  let recent = client.recentVersion(maxAge: 1_000_000) {
            readVersion = recent
        } else {
            readVersion = try await self.readVersion()
        }
        let request = CommitRequest(
            readVersion: readVersion,
            readConflictKeys: s.readConflictKeys.map(\.bytes).sorted(by: bytesLess),
            readConflictRanges: s.readConflictRanges,
            writeConflictKeys: s.writeConflictKeys.map(\.bytes).sorted(by: bytesLess),
            mutations: s.mutations,
            idempotencyID: s.idempotencyID,
            idempotencyRetention: s.idempotencyRetention,
            attemptID: s.attemptID)
        state.withLock { $0.commitReadVersion = readVersion }
        let t0 = client.transport.now()
        defer {
            let elapsed = client.transport.now() &- t0
            state.withLock { $0.timings.commit += elapsed }
        }
        let stamp = try await client.commit(request)
        state.withLock {
            $0.committed = true
            $0.committedStamp = stamp
        }
        return stamp
    }
}
