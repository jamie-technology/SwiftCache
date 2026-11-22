/// Addresses. A process listens on `host:port`; every role it hosts has an endpoint of
/// the form `host:port#role-id`. The network routes on the part before `#`, the process
/// dispatches on the full endpoint. Client endpoints start with `client:` and aren't
/// dialable (replies travel back over the client's own connection).
public typealias Endpoint = String

extension String {
    /// The process (network) address of an endpoint: everything before `#`.
    public var processAddress: String {
        if let i = firstIndex(of: "#") { return String(self[..<i]) }
        return self
    }
}

/// Error codes that travel over the wire.
public enum ErrorCode: UInt16, Sendable, CustomStringConvertible {
    /// The transaction conflicted with another committed transaction. Retryable.
    case notCommitted = 1
    /// The read version is older than the MVCC window or the transaction system's
    /// generation. Retryable.
    case transactionTooOld = 2
    /// The storage server has not caught up to the requested version yet. Retryable.
    case futureVersion = 3
    /// The key's slot is not served by this storage server (stale topology). Retryable.
    case wrongShard = 4
    /// The cluster is recovering, bootstrapping or a shard has no live replicas. Retryable.
    case unavailable = 5
    /// The request was malformed.
    case invalidRequest = 6
    /// The transaction or value exceeded a size limit.
    case tooLarge = 7
    /// Generic internal failure.
    case internalError = 8
    /// The role that received the request belongs to an ended generation / lost authority.
    case staleGeneration = 9

    public var isRetryable: Bool {
        switch self {
        case .notCommitted, .transactionTooOld, .futureVersion, .wrongShard, .unavailable, .staleGeneration: true
        case .invalidRequest, .tooLarge, .internalError: false
        }
    }

    public var description: String {
        switch self {
        case .notCommitted: "not_committed"
        case .transactionTooOld: "transaction_too_old"
        case .futureVersion: "future_version"
        case .wrongShard: "wrong_shard"
        case .unavailable: "unavailable"
        case .invalidRequest: "invalid_request"
        case .tooLarge: "too_large"
        case .internalError: "internal_error"
        case .staleGeneration: "stale_generation"
        }
    }
}

/// A write within a transaction.
public enum Mutation: Hashable, Sendable, WireCodable {
    /// Set `key` to `value`. A non-zero `ttl` (in versions ≈ microseconds) makes the value
    /// invisible to reads at versions ≥ commitVersion + ttl.
    case set(key: Bytes, value: Bytes, ttl: UInt64)
    /// Delete `key`.
    case clear(key: Bytes)
    /// Atomically add `delta` to the little-endian Int64 stored at `key` (missing = 0).
    case add(key: Bytes, delta: Int64)

    public var key: Bytes {
        switch self {
        case .set(let k, _, _), .clear(let k), .add(let k, _): k
        }
    }

    public var approximateSize: Int {
        switch self {
        case .set(let k, let v, _): k.count + v.count + 16
        case .clear(let k): k.count + 8
        case .add(let k, _): k.count + 16
        }
    }

    public func encode(to w: inout WireWriter) {
        switch self {
        case .set(let k, let v, let ttl): w.u8(1); w.blob(k); w.blob(v); w.u64(ttl)
        case .clear(let k): w.u8(2); w.blob(k)
        case .add(let k, let d): w.u8(3); w.blob(k); w.i64(d)
        }
    }

    public init(from r: inout WireReader) throws {
        switch try r.u8() {
        case 1: self = .set(key: try r.blob(), value: try r.blob(), ttl: try r.u64())
        case 2: self = .clear(key: try r.blob())
        case 3: self = .add(key: try r.blob(), delta: try r.i64())
        case let t: throw WireError("bad mutation tag \(t)")
        }
    }
}

/// A key range `[begin, end)`.
public struct KeyRange: Hashable, Sendable, WireCodable {
    public var begin: Bytes
    public var end: Bytes
    public init(begin: Bytes, end: Bytes) { self.begin = begin; self.end = end }
    public func contains(_ key: Bytes) -> Bool { !bytesLess(key, begin) && bytesLess(key, end) }
    public func encode(to w: inout WireWriter) { w.blob(begin); w.blob(end) }
    public init(from r: inout WireReader) throws { begin = try r.blob(); end = try r.blob() }
}

/// One MVCC version of a key as stored by a storage server.
public struct VersionedValue: Hashable, Sendable, WireCodable {
    /// Versionstamp of the commit that wrote this version.
    public var stamp: Versionstamp
    /// The value, or nil for a deletion.
    public var value: Bytes?
    /// Version at which the value expires (0 = never).
    public var expiresAt: Version

    public init(stamp: Versionstamp, value: Bytes?, expiresAt: Version = 0) {
        self.stamp = stamp
        self.value = value
        self.expiresAt = expiresAt
    }

    /// The value visible to a read at `version`, honoring deletion and expiry.
    @inlinable public func visibleValue(at version: Version) -> Bytes? {
        guard let value else { return nil }
        if expiresAt != 0 && version >= expiresAt { return nil }
        return value
    }

    public func encode(to w: inout WireWriter) { w.stamp(stamp); w.optionalBlob(value); w.u64(expiresAt) }
    public init(from r: inout WireReader) throws {
        stamp = try r.stamp(); value = try r.optionalBlob(); expiresAt = try r.u64()
    }
}

/// A key/value returned from a read.
public struct KeyValueEntry: Hashable, Sendable, WireCodable {
    public var key: Bytes
    public var value: Bytes
    public var stamp: Versionstamp
    public init(key: Bytes, value: Bytes, stamp: Versionstamp) {
        self.key = key; self.value = value; self.stamp = stamp
    }
    public func encode(to w: inout WireWriter) { w.blob(key); w.blob(value); w.stamp(stamp) }
    public init(from r: inout WireReader) throws { key = try r.blob(); value = try r.blob(); stamp = try r.stamp() }
}

/// Keys starting with 0xFF are reserved for the system.
public enum SystemKeys {
    public static let prefix: UInt8 = 0xFF
    /// `\xff/idmp/<operation id>` → versionstamp of the commit that carried that operation.
    public static func idempotency(_ id: Bytes) -> Bytes { [0xFF] + Bytes("/idmp/".utf8) + id }
    public static func isSystem(_ key: Bytes) -> Bool { key.first == prefix }

    /// The value of an idempotency record: the commit's versionstamp and attempt id.
    public static func idempotencyValue(stamp: Versionstamp, attemptID: UInt64) -> Bytes {
        stamp.bytes + LittleEndianInt64.encode(Int64(bitPattern: attemptID))
    }

    public static func parseIdempotencyValue(_ value: Bytes) -> (stamp: Versionstamp, attemptID: UInt64)? {
        guard value.count == 18, let stamp = Versionstamp(bytes: Array(value[0..<10])) else { return nil }
        return (stamp, UInt64(bitPattern: LittleEndianInt64.decode(Array(value[10...]))))
    }
}

public struct CommitRequest: Hashable, Sendable, WireCodable {
    public var readVersion: Version
    public var readConflictKeys: [Bytes]
    public var readConflictRanges: [KeyRange]
    public var writeConflictKeys: [Bytes]
    public var mutations: [Mutation]
    /// Logical operation id (16 bytes), stable across retries. When set, the proxy records
    /// `SystemKeys.idempotency(id)` = this commit's versionstamp atomically with the
    /// commit, and the client adds a read conflict on that key to every attempt.
    public var idempotencyID: Bytes?
    /// Seconds to retain the idempotency record.
    public var idempotencyRetention: UInt32
    /// Which attempt of the operation this is (recorded so a retry can tell which
    /// attempt's result to return).
    public var attemptID: UInt64
    /// Cluster metadata changes (only accepted from the cluster controller).
    public var metaOps: [MetaOp]

    public init(readVersion: Version, readConflictKeys: [Bytes] = [], readConflictRanges: [KeyRange] = [],
                writeConflictKeys: [Bytes] = [], mutations: [Mutation] = [], idempotencyID: Bytes? = nil,
                idempotencyRetention: UInt32 = 0, attemptID: UInt64 = 0, metaOps: [MetaOp] = []) {
        self.readVersion = readVersion
        self.readConflictKeys = readConflictKeys
        self.readConflictRanges = readConflictRanges
        self.writeConflictKeys = writeConflictKeys
        self.mutations = mutations
        self.idempotencyID = idempotencyID
        self.idempotencyRetention = idempotencyRetention
        self.attemptID = attemptID
        self.metaOps = metaOps
    }

    public func encode(to w: inout WireWriter) {
        w.u64(readVersion); w.blobs(readConflictKeys); w.put(readConflictRanges); w.blobs(writeConflictKeys)
        w.put(mutations); w.optionalBlob(idempotencyID); w.u32(idempotencyRetention); w.u64(attemptID); w.put(metaOps)
    }

    public init(from r: inout WireReader) throws {
        readVersion = try r.u64(); readConflictKeys = try r.blobs(); readConflictRanges = try r.getArray()
        writeConflictKeys = try r.blobs(); mutations = try r.getArray(); idempotencyID = try r.optionalBlob()
        idempotencyRetention = try r.u32(); attemptID = try r.u64(); metaOps = try r.getArray()
    }
}

public enum CommitResult: Hashable, Sendable, WireCodable {
    case committed(Versionstamp)
    case failed(ErrorCode)

    public func encode(to w: inout WireWriter) {
        switch self {
        case .committed(let s): w.u8(1); w.stamp(s)
        case .failed(let e): w.u8(2); w.u16(e.rawValue)
        }
    }
    public init(from r: inout WireReader) throws {
        switch try r.u8() {
        case 1: self = .committed(try r.stamp())
        case 2: self = .failed(ErrorCode(rawValue: try r.u16()) ?? .internalError)
        case let t: throw WireError("bad commit result \(t)")
        }
    }
}

/// The routing table clients use: which shard owns each slot and which replicas serve it.
public struct Topology: Hashable, Sendable, WireCodable {
    public struct Shard: Hashable, Sendable {
        public var id: Int32
        public var replicas: [Endpoint]
        public init(id: Int32, replicas: [Endpoint]) { self.id = id; self.replicas = replicas }
    }

    public var epoch: UInt64
    /// Slot -> shard id (-1 = unassigned). Always `Slots.count` long.
    public var slotOwner: [Int32]
    public var shards: [Shard]

    public init(epoch: UInt64, slotOwner: [Int32], shards: [Shard]) {
        self.epoch = epoch
        self.slotOwner = slotOwner
        self.shards = shards
    }

    public static let empty = Topology(epoch: 0, slotOwner: Array(repeating: -1, count: Slots.count), shards: [])

    public func shard(id: Int32) -> Shard? { shards.first { $0.id == id } }

    public func shard(forKey key: Bytes) -> Shard? {
        let owner = slotOwner[Slots.slot(for: key)]
        return owner < 0 ? nil : shard(id: owner)
    }

    public func encode(to w: inout WireWriter) {
        w.u64(epoch)
        w.slotOwners(slotOwner)
        w.array(shards) { w, s in w.i32(s.id); w.strings(s.replicas) }
    }

    public init(from r: inout WireReader) throws {
        epoch = try r.u64()
        slotOwner = try r.slotOwners()
        shards = try r.array { r in Shard(id: try r.i32(), replicas: try r.strings()) }
    }
}

extension WireWriter {
    /// Run-length encoded slot -> owner map.
    public mutating func slotOwners(_ owners: [Int32]) {
        var runs = [(start: Int, length: Int, owner: Int32)]()
        for (i, o) in owners.enumerated() {
            if let last = runs.last, last.owner == o, last.start + last.length == i {
                runs[runs.count - 1].length += 1
            } else {
                runs.append((i, 1, o))
            }
        }
        array(runs) { w, r in w.u16(UInt16(r.start)); w.u16(UInt16(r.length)); w.i32(r.owner) }
    }
}

extension WireReader {
    public mutating func slotOwners() throws -> [Int32] {
        var owner = [Int32](repeating: -1, count: Slots.count)
        let runs = try array { r in (Int(try r.u16()), Int(try r.u16()), try r.i32()) }
        for (start, length, o) in runs {
            guard start + length <= Slots.count else { throw WireError("bad slot run") }
            for i in start..<(start + length) { owner[i] = o }
        }
        return owner
    }
}

public struct SnapshotChunk: Hashable, Sendable, WireCodable {
    public struct Item: Hashable, Sendable, WireCodable {
        public var key: Bytes
        public var versions: [VersionedValue]
        public init(key: Bytes, versions: [VersionedValue]) { self.key = key; self.versions = versions }
        public func encode(to w: inout WireWriter) { w.blob(key); w.put(versions) }
        public init(from r: inout WireReader) throws { key = try r.blob(); versions = try r.getArray() }
    }
    /// The source's applied version at the moment the snapshot was taken (0 = refused).
    public var version: Version
    public var items: [Item]
    public var done: Bool
    public init(version: Version, items: [Item], done: Bool) { self.version = version; self.items = items; self.done = done }
    public func encode(to w: inout WireWriter) { w.u64(version); w.put(items); w.bool(done) }
    public init(from r: inout WireReader) throws { version = try r.u64(); items = try r.getArray(); done = try r.bool() }
}

public struct StorageStats: Hashable, Sendable, WireCodable {
    public var endpoint: Endpoint
    public var tag: UInt32
    public var shardID: Int32
    public var isActive: Bool
    public var appliedVersion: Version
    public var keyCount: UInt64
    public var versionCount: UInt64
    public var approximateBytes: UInt64
    public var ownedSlots: UInt32

    public init(endpoint: Endpoint, tag: UInt32, shardID: Int32, isActive: Bool, appliedVersion: Version,
                keyCount: UInt64, versionCount: UInt64, approximateBytes: UInt64, ownedSlots: UInt32) {
        self.endpoint = endpoint; self.tag = tag; self.shardID = shardID; self.isActive = isActive
        self.appliedVersion = appliedVersion; self.keyCount = keyCount; self.versionCount = versionCount
        self.approximateBytes = approximateBytes; self.ownedSlots = ownedSlots
    }

    public func encode(to w: inout WireWriter) {
        w.string(endpoint); w.u32(tag); w.i32(shardID); w.bool(isActive); w.u64(appliedVersion)
        w.u64(keyCount); w.u64(versionCount); w.u64(approximateBytes); w.u32(ownedSlots)
    }
    public init(from r: inout WireReader) throws {
        endpoint = try r.string(); tag = try r.u32(); shardID = try r.i32(); isActive = try r.bool()
        appliedVersion = try r.u64(); keyCount = try r.u64(); versionCount = try r.u64()
        approximateBytes = try r.u64(); ownedSlots = try r.u32()
    }
}

/// A human-oriented snapshot of the whole cluster, served by the cluster controller.
public struct ClusterStatus: Hashable, Sendable, WireCodable {
    public struct Process: Hashable, Sendable, WireCodable {
        public var address: Endpoint
        public var processClass: String
        public var roles: [Endpoint]
        public var alive: Bool
        public init(address: Endpoint, processClass: String, roles: [Endpoint], alive: Bool) {
            self.address = address; self.processClass = processClass; self.roles = roles; self.alive = alive
        }
        public func encode(to w: inout WireWriter) { w.string(address); w.string(processClass); w.strings(roles); w.bool(alive) }
        public init(from r: inout WireReader) throws {
            address = try r.string(); processClass = try r.string(); roles = try r.strings(); alive = try r.bool()
        }
    }

    public var clusterController: Endpoint
    public var generation: UInt64
    public var phase: String
    public var master: Endpoint
    public var proxies: [Endpoint]
    public var resolvers: [Endpoint]
    public var tlogs: [Endpoint]
    public var replicationFactor: UInt32
    public var topology: Topology
    /// One line per storage role: "endpoint shard=N tag=T state".
    public var storage: [String]
    public var processes: [Process]
    public var movesInProgress: [String]
    public var recoveries: UInt64

    public init(clusterController: Endpoint, generation: UInt64, phase: String, master: Endpoint, proxies: [Endpoint],
                resolvers: [Endpoint], tlogs: [Endpoint], replicationFactor: UInt32, topology: Topology,
                storage: [String], processes: [Process], movesInProgress: [String], recoveries: UInt64) {
        self.clusterController = clusterController; self.generation = generation; self.phase = phase
        self.master = master; self.proxies = proxies; self.resolvers = resolvers; self.tlogs = tlogs
        self.replicationFactor = replicationFactor; self.topology = topology; self.storage = storage
        self.processes = processes; self.movesInProgress = movesInProgress; self.recoveries = recoveries
    }

    public func encode(to w: inout WireWriter) {
        w.string(clusterController); w.u64(generation); w.string(phase); w.string(master); w.strings(proxies)
        w.strings(resolvers); w.strings(tlogs); w.u32(replicationFactor); w.put(topology); w.strings(storage)
        w.put(processes); w.strings(movesInProgress); w.u64(recoveries)
    }

    public init(from r: inout WireReader) throws {
        clusterController = try r.string(); generation = try r.u64(); phase = try r.string(); master = try r.string()
        proxies = try r.strings(); resolvers = try r.strings(); tlogs = try r.strings(); replicationFactor = try r.u32()
        topology = try r.get(); storage = try r.strings(); processes = try r.getArray()
        movesInProgress = try r.strings(); recoveries = try r.u64()
    }
}
