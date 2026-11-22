/// Node addresses. In production an endpoint is `host:port`; in simulation it is any
/// unique name. Clients also have endpoints (they are not dialable, replies travel back
/// over the connection the client opened).
public typealias Endpoint = String

/// Error codes that travel over the wire.
public enum ErrorCode: UInt16, Sendable, CustomStringConvertible {
    /// The transaction conflicted with another committed transaction. Retryable.
    case notCommitted = 1
    /// The read version is older than the MVCC window (or predates a coordinator recovery). Retryable.
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

    public var isRetryable: Bool {
        switch self {
        case .notCommitted, .transactionTooOld, .futureVersion, .wrongShard, .unavailable: true
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
        }
    }
}

/// A write within a transaction.
public enum Mutation: Hashable, Sendable {
    /// Set `key` to `value`. A non-zero `ttl` (in versions ≈ microseconds) makes the value
    /// invisible to reads at versions ≥ commitVersion + ttl.
    case set(key: Bytes, value: Bytes, ttl: UInt64)
    /// Delete `key`.
    case clear(key: Bytes)
    /// Atomically add `delta` to the little-endian Int64 stored at `key` (missing = 0).
    /// Does not create a read conflict.
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
}

/// A key range `[begin, end)`.
public struct KeyRange: Hashable, Sendable {
    public var begin: Bytes
    public var end: Bytes
    public init(begin: Bytes, end: Bytes) { self.begin = begin; self.end = end }
    public func contains(_ key: Bytes) -> Bool { !bytesLess(key, begin) && bytesLess(key, end) }
}

/// One MVCC version of a key as stored by a storage server.
public struct VersionedValue: Hashable, Sendable {
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
}

/// A key/value returned from a read.
public struct KeyValueEntry: Hashable, Sendable {
    public var key: Bytes
    public var value: Bytes
    public var stamp: Versionstamp
    public init(key: Bytes, value: Bytes, stamp: Versionstamp) {
        self.key = key; self.value = value; self.stamp = stamp
    }
}

public struct CommitRequest: Hashable, Sendable {
    public var readVersion: Version
    public var readConflictKeys: [Bytes]
    public var readConflictRanges: [KeyRange]
    public var writeConflictKeys: [Bytes]
    public var mutations: [Mutation]

    public init(readVersion: Version, readConflictKeys: [Bytes] = [], readConflictRanges: [KeyRange] = [],
                writeConflictKeys: [Bytes] = [], mutations: [Mutation] = []) {
        self.readVersion = readVersion
        self.readConflictKeys = readConflictKeys
        self.readConflictRanges = readConflictRanges
        self.writeConflictKeys = writeConflictKeys
        self.mutations = mutations
    }
}

public enum CommitResult: Hashable, Sendable {
    case committed(Versionstamp)
    case failed(ErrorCode)
}

/// The routing table clients use: which shard owns each slot and which replicas serve it.
public struct Topology: Hashable, Sendable {
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
}

/// What a storage server reports to the coordinator when (re)joining or during recovery.
public struct StorageReport: Hashable, Sendable {
    public var endpoint: Endpoint
    /// Shard the server believes it belongs to (-1 = none).
    public var shardID: Int32
    /// Highest coordinator epoch the server has seen.
    public var coordinatorEpoch: UInt64
    /// Last version applied (0 = none).
    public var appliedVersion: Version
    /// Highest version the server knows to be committed cluster-wide.
    public var committedVersion: Version
    /// Whether the server was a fully caught-up member of its shard.
    public var isActive: Bool
    public var ownedSlots: SlotSet

    public init(endpoint: Endpoint, shardID: Int32, coordinatorEpoch: UInt64, appliedVersion: Version,
                committedVersion: Version = 0, isActive: Bool, ownedSlots: SlotSet) {
        self.endpoint = endpoint
        self.shardID = shardID
        self.coordinatorEpoch = coordinatorEpoch
        self.appliedVersion = appliedVersion
        self.committedVersion = committedVersion
        self.isActive = isActive
        self.ownedSlots = ownedSlots
    }
}

public struct Assignment: Hashable, Sendable {
    public enum Mode: UInt8, Sendable {
        /// Become an active member immediately, owning `slots`, with no data.
        case activeEmpty = 1
        /// Wipe and wait for a `beginReceive` in the batch stream to copy data from peers.
        case catchUp = 2
    }
    public var coordinatorEpoch: UInt64
    public var shardID: Int32
    public var mode: Mode
    public var slots: SlotSet

    public init(coordinatorEpoch: UInt64, shardID: Int32, mode: Mode, slots: SlotSet) {
        self.coordinatorEpoch = coordinatorEpoch
        self.shardID = shardID
        self.mode = mode
        self.slots = slots
    }
}

/// Metadata operations carried in the batch stream, applied at the batch's version
/// (after that batch's mutations).
public enum MetaOp: Hashable, Sendable {
    /// Destination shard members start buffering mutations for `slots` and copy existing
    /// data from one of `sources`.
    case beginReceive(moveID: UInt64, shardID: Int32, slots: SlotSet, sources: [Endpoint])
    /// Abandon a receive (drop buffered/copied data for those slots).
    case abortReceive(moveID: UInt64, shardID: Int32)
    /// Ownership of `slots` moves `from` -> `to`. Source members drop the data; destination
    /// members start serving it.
    case transfer(moveID: UInt64, slots: SlotSet, from: Int32, to: Int32)
    /// `shardID` starts owning `slots` with no data (used when a shard is lost).
    case gainEmpty(shardID: Int32, slots: SlotSet)
}

/// A group of transactions committed at one version, sent from the coordinator to every
/// storage server (filtered to the mutations it stores). Empty batches advance versions.
public struct Batch: Hashable, Sendable {
    public struct Entry: Hashable, Sendable {
        public var order: UInt16
        public var mutations: [Mutation]
        public init(order: UInt16, mutations: [Mutation]) { self.order = order; self.mutations = mutations }
    }
    public var coordinatorEpoch: UInt64
    public var version: Version
    public var prevVersion: Version
    /// Highest version known to be fully replicated (safe to serve reads at).
    public var committedVersion: Version
    /// Oldest version the cluster still guarantees to be readable (for GC).
    public var oldestVersion: Version
    public var entries: [Entry]
    public var meta: [MetaOp]

    public init(coordinatorEpoch: UInt64, version: Version, prevVersion: Version, committedVersion: Version,
                oldestVersion: Version, entries: [Entry], meta: [MetaOp]) {
        self.coordinatorEpoch = coordinatorEpoch
        self.version = version
        self.prevVersion = prevVersion
        self.committedVersion = committedVersion
        self.oldestVersion = oldestVersion
        self.entries = entries
        self.meta = meta
    }
}

public struct SnapshotChunk: Hashable, Sendable {
    public struct Item: Hashable, Sendable {
        public var key: Bytes
        public var versions: [VersionedValue]
        public init(key: Bytes, versions: [VersionedValue]) { self.key = key; self.versions = versions }
    }
    /// The source's applied version at the moment the snapshot was taken.
    public var version: Version
    public var items: [Item]
    public var done: Bool
    public init(version: Version, items: [Item], done: Bool) { self.version = version; self.items = items; self.done = done }
}

public struct StorageStats: Hashable, Sendable {
    public var endpoint: Endpoint
    public var shardID: Int32
    public var isActive: Bool
    public var appliedVersion: Version
    public var committedVersion: Version
    public var keyCount: UInt64
    public var versionCount: UInt64
    public var approximateBytes: UInt64
    public var ownedSlots: UInt32

    public init(endpoint: Endpoint, shardID: Int32, isActive: Bool, appliedVersion: Version, committedVersion: Version,
                keyCount: UInt64, versionCount: UInt64, approximateBytes: UInt64, ownedSlots: UInt32) {
        self.endpoint = endpoint; self.shardID = shardID; self.isActive = isActive
        self.appliedVersion = appliedVersion; self.committedVersion = committedVersion
        self.keyCount = keyCount; self.versionCount = versionCount
        self.approximateBytes = approximateBytes; self.ownedSlots = ownedSlots
    }
}

public struct ClusterStatus: Hashable, Sendable {
    public struct Server: Hashable, Sendable {
        public var endpoint: Endpoint
        public var shardID: Int32
        public var state: String
        public init(endpoint: Endpoint, shardID: Int32, state: String) {
            self.endpoint = endpoint; self.shardID = shardID; self.state = state
        }
    }
    public var coordinatorEpoch: UInt64
    public var phase: String
    public var latestVersion: Version
    public var committedVersion: Version
    public var replicationFactor: UInt32
    public var topology: Topology
    public var servers: [Server]
    public var movesInProgress: [String]
    public var commitsTotal: UInt64
    public var conflictsTotal: UInt64

    public init(coordinatorEpoch: UInt64, phase: String, latestVersion: Version, committedVersion: Version,
                replicationFactor: UInt32, topology: Topology, servers: [Server], movesInProgress: [String],
                commitsTotal: UInt64, conflictsTotal: UInt64) {
        self.coordinatorEpoch = coordinatorEpoch; self.phase = phase
        self.latestVersion = latestVersion; self.committedVersion = committedVersion
        self.replicationFactor = replicationFactor; self.topology = topology
        self.servers = servers; self.movesInProgress = movesInProgress
        self.commitsTotal = commitsTotal; self.conflictsTotal = conflictsTotal
    }
}
