// Types used by the distributed transaction system: coordination, recruitment,
// transaction logs, resolution.

/// A Paxos ballot for the coordinators' generation register. Totally ordered; `id`
/// breaks ties between masters that pick the same round.
public struct Ballot: Hashable, Comparable, Sendable, WireCodable, CustomStringConvertible {
    public var round: UInt64
    public var id: UInt64
    public init(round: UInt64, id: UInt64) { self.round = round; self.id = id }
    public static let zero = Ballot(round: 0, id: 0)
    public static func < (a: Ballot, b: Ballot) -> Bool { (a.round, a.id) < (b.round, b.id) }
    public func encode(to w: inout WireWriter) { w.u64(round); w.u64(id) }
    public init(from r: inout WireReader) throws { round = try r.u64(); id = try r.u64() }
    public var description: String { "\(round).\(id % 10_000)" }
}

/// Identity of a cluster-controller candidate / leader. `changeID` is fresh for every
/// candidacy, so a restarted or re-elected process is a different leader.
public struct LeaderInfo: Hashable, Sendable, WireCodable {
    public var endpoint: Endpoint
    public var changeID: UInt64
    public init(endpoint: Endpoint, changeID: UInt64) { self.endpoint = endpoint; self.changeID = changeID }
    public func encode(to w: inout WireWriter) { w.string(endpoint); w.u64(changeID) }
    public init(from r: inout WireReader) throws { endpoint = try r.string(); changeID = try r.u64() }
}

/// The durable root of the transaction system, stored in the coordinators' generation
/// register: which generation is current and where its logs are.
public struct CoreState: Hashable, Sendable, WireCodable {
    public var generation: UInt64
    public var tlogs: [Endpoint]
    /// The version the generation started from (the previous generation's end).
    public var startVersion: Version

    public init(generation: UInt64, tlogs: [Endpoint], startVersion: Version) {
        self.generation = generation; self.tlogs = tlogs; self.startVersion = startVersion
    }
    public func encode(to w: inout WireWriter) { w.u8(1); w.u64(generation); w.strings(tlogs); w.u64(startVersion) }
    public init(from r: inout WireReader) throws {
        guard try r.u8() == 1 else { throw WireError("unknown CoreState format") }
        generation = try r.u64(); tlogs = try r.strings(); startVersion = try r.u64()
    }
    public var bytes: Bytes { var w = WireWriter(); w.put(self); return w.bytes }
    public init(bytes: Bytes) throws { self = try WireReader.read(bytes) { try $0.get() } }
}

public enum ProcessClass: UInt8, Sendable, CustomStringConvertible {
    /// Hosts storage servers.
    case storage = 1
    /// Hosts transaction logs.
    case log = 2
    /// Hosts cluster controller, master, commit proxies and resolvers.
    case stateless = 3

    public var description: String {
        switch self {
        case .storage: "storage"
        case .log: "log"
        case .stateless: "stateless"
        }
    }

    public init?(name: String) {
        switch name {
        case "storage": self = .storage
        case "log": self = .log
        case "stateless": self = .stateless
        default: return nil
        }
    }
}

/// What the cluster controller tells every process about the current transaction system.
public struct ServerDBInfo: Hashable, Sendable, WireCodable {
    public var clusterController: Endpoint
    /// Increases with every change made by one controller (`ccChangeID` disambiguates controllers).
    public var infoVersion: UInt64
    public var ccChangeID: UInt64
    public var generation: UInt64
    /// True once the generation finished recovery and accepts commits.
    public var recovered: Bool
    public var master: Endpoint
    public var proxies: [Endpoint]
    public var resolvers: [Endpoint]
    public var tlogs: [Endpoint]

    public init(clusterController: Endpoint, infoVersion: UInt64, ccChangeID: UInt64, generation: UInt64, recovered: Bool,
                master: Endpoint, proxies: [Endpoint], resolvers: [Endpoint], tlogs: [Endpoint]) {
        self.clusterController = clusterController; self.infoVersion = infoVersion; self.ccChangeID = ccChangeID
        self.generation = generation; self.recovered = recovered; self.master = master
        self.proxies = proxies; self.resolvers = resolvers; self.tlogs = tlogs
    }

    public static let empty = ServerDBInfo(clusterController: "", infoVersion: 0, ccChangeID: 0, generation: 0,
                                           recovered: false, master: "", proxies: [], resolvers: [], tlogs: [])

    public func encode(to w: inout WireWriter) {
        w.string(clusterController); w.u64(infoVersion); w.u64(ccChangeID); w.u64(generation); w.bool(recovered)
        w.string(master); w.strings(proxies); w.strings(resolvers); w.strings(tlogs)
    }
    public init(from r: inout WireReader) throws {
        clusterController = try r.string(); infoVersion = try r.u64(); ccChangeID = try r.u64(); generation = try r.u64()
        recovered = try r.bool(); master = try r.string(); proxies = try r.strings(); resolvers = try r.strings()
        tlogs = try r.strings()
    }
}

/// What clients need: where to send read versions and commits, and the shard topology.
public struct ClientInfo: Hashable, Sendable, WireCodable {
    public var generation: UInt64
    public var proxies: [Endpoint]
    public var topology: Topology
    /// Serves read versions (under log leases).
    public var master: Endpoint
    public init(generation: UInt64, proxies: [Endpoint], topology: Topology, master: Endpoint = "") {
        self.generation = generation; self.proxies = proxies; self.topology = topology; self.master = master
    }
    public func encode(to w: inout WireWriter) { w.u64(generation); w.strings(proxies); w.put(topology); w.string(master) }
    public init(from r: inout WireReader) throws {
        generation = try r.u64(); proxies = try r.strings(); topology = try r.get(); master = try r.string()
    }
}

/// A storage server's self-report, sent with its worker's registration.
public struct StorageReport: Hashable, Sendable, WireCodable {
    public var endpoint: Endpoint
    /// 0 = no tag (not a member of any shard).
    public var tag: Tag
    public var appliedVersion: Version
    public init(endpoint: Endpoint, tag: Tag, appliedVersion: Version) {
        self.endpoint = endpoint; self.tag = tag; self.appliedVersion = appliedVersion
    }
    public func encode(to w: inout WireWriter) { w.string(endpoint); w.u32(tag); w.u64(appliedVersion) }
    public init(from r: inout WireReader) throws { endpoint = try r.string(); tag = try r.u32(); appliedVersion = try r.u64() }
}

/// Sent by every worker to the cluster controller periodically (it doubles as a heartbeat).
public struct WorkerRegistration: Hashable, Sendable, WireCodable {
    public var worker: Endpoint
    public var processClass: ProcessClass
    /// Every role endpoint currently hosted by this process.
    public var roles: [Endpoint]
    public var storage: StorageReport?
    public init(worker: Endpoint, processClass: ProcessClass, roles: [Endpoint], storage: StorageReport?) {
        self.worker = worker; self.processClass = processClass; self.roles = roles; self.storage = storage
    }
    public func encode(to w: inout WireWriter) { w.string(worker); w.u8(processClass.rawValue); w.strings(roles); w.put(storage) }
    public init(from r: inout WireReader) throws {
        worker = try r.string()
        processClass = ProcessClass(rawValue: try r.u8()) ?? .stateless
        roles = try r.strings(); storage = try r.getOptional()
    }
}

// MARK: - Recruitment

public struct MasterSpec: Hashable, Sendable, WireCodable {
    public var clusterController: Endpoint
    public var ccChangeID: UInt64
    /// Unique per recovery attempt; also the master's ballot id.
    public var recoveryID: UInt64
    public var logWorkers: [Endpoint]
    public var statelessWorkers: [Endpoint]
    public var logReplicas: UInt32
    public var proxies: UInt32
    public var resolvers: UInt32
    /// The controller's shard map and the highest storage version it has seen; used only
    /// if every log of the previous generation is gone (data-losing forced recovery).
    public var fallbackMap: ShardMap
    public var fallbackVersion: Version

    public init(clusterController: Endpoint, ccChangeID: UInt64, recoveryID: UInt64, logWorkers: [Endpoint], statelessWorkers: [Endpoint],
                logReplicas: UInt32, proxies: UInt32, resolvers: UInt32, fallbackMap: ShardMap, fallbackVersion: Version) {
        self.clusterController = clusterController; self.ccChangeID = ccChangeID; self.recoveryID = recoveryID
        self.logWorkers = logWorkers; self.statelessWorkers = statelessWorkers; self.logReplicas = logReplicas
        self.proxies = proxies; self.resolvers = resolvers; self.fallbackMap = fallbackMap; self.fallbackVersion = fallbackVersion
    }
    public func encode(to w: inout WireWriter) {
        w.string(clusterController); w.u64(ccChangeID); w.u64(recoveryID); w.strings(logWorkers); w.strings(statelessWorkers)
        w.u32(logReplicas); w.u32(proxies); w.u32(resolvers); w.put(fallbackMap); w.u64(fallbackVersion)
    }
    public init(from r: inout WireReader) throws {
        clusterController = try r.string(); ccChangeID = try r.u64(); recoveryID = try r.u64(); logWorkers = try r.strings()
        statelessWorkers = try r.strings(); logReplicas = try r.u32(); proxies = try r.u32(); resolvers = try r.u32()
        fallbackMap = try r.get(); fallbackVersion = try r.u64()
    }
}

public struct TLogSpec: Hashable, Sendable, WireCodable {
    public var generation: UInt64
    public var startVersion: Version
    /// Locked logs of the previous generation to copy unpopped data (≤ startVersion) from,
    /// in order of preference. Empty for a fresh cluster or a forced recovery.
    public var seedFrom: [Endpoint]
    /// Forced recovery only: storage tags that continue from `startVersion`.
    public var initialTags: [Tag]
    /// The master of the generation (logs report durability to it).
    public var master: Endpoint
    public init(generation: UInt64, startVersion: Version, seedFrom: [Endpoint], initialTags: [Tag] = [], master: Endpoint = "") {
        self.generation = generation; self.startVersion = startVersion; self.seedFrom = seedFrom
        self.initialTags = initialTags; self.master = master
    }
    public func encode(to w: inout WireWriter) { w.u64(generation); w.u64(startVersion); w.strings(seedFrom); w.u32s(initialTags); w.string(master) }
    public init(from r: inout WireReader) throws {
        generation = try r.u64(); startVersion = try r.u64(); seedFrom = try r.strings(); initialTags = try r.u32s()
        master = try r.string()
    }
}

public struct ResolverSpec: Hashable, Sendable, WireCodable {
    public var generation: UInt64
    public var startVersion: Version
    public var proxyCount: UInt32
    public init(generation: UInt64, startVersion: Version, proxyCount: UInt32) {
        self.generation = generation; self.startVersion = startVersion; self.proxyCount = proxyCount
    }
    public func encode(to w: inout WireWriter) { w.u64(generation); w.u64(startVersion); w.u32(proxyCount) }
    public init(from r: inout WireReader) throws { generation = try r.u64(); startVersion = try r.u64(); proxyCount = try r.u32() }
}

public struct ProxySpec: Hashable, Sendable, WireCodable {
    public var generation: UInt64
    public var index: UInt32
    public var startVersion: Version
    public var master: Endpoint
    public var resolvers: [Endpoint]
    public var tlogs: [Endpoint]
    public var clusterController: Endpoint
    public var shardMap: ShardMap
    /// Write a full shard-map snapshot to the metadata tag in the first batch.
    public var writeSnapshot: Bool

    public init(generation: UInt64, index: UInt32, startVersion: Version, master: Endpoint, resolvers: [Endpoint],
                tlogs: [Endpoint], clusterController: Endpoint, shardMap: ShardMap, writeSnapshot: Bool) {
        self.generation = generation; self.index = index; self.startVersion = startVersion; self.master = master
        self.resolvers = resolvers; self.tlogs = tlogs; self.clusterController = clusterController
        self.shardMap = shardMap; self.writeSnapshot = writeSnapshot
    }
    public func encode(to w: inout WireWriter) {
        w.u64(generation); w.u32(index); w.u64(startVersion); w.string(master); w.strings(resolvers); w.strings(tlogs)
        w.string(clusterController); w.put(shardMap); w.bool(writeSnapshot)
    }
    public init(from r: inout WireReader) throws {
        generation = try r.u64(); index = try r.u32(); startVersion = try r.u64(); master = try r.string()
        resolvers = try r.strings(); tlogs = try r.strings(); clusterController = try r.string()
        shardMap = try r.get(); writeSnapshot = try r.bool()
    }
}

public enum RoleSpec: Hashable, Sendable, WireCodable {
    case master(MasterSpec)
    case tlog(TLogSpec)
    case resolver(ResolverSpec)
    case proxy(ProxySpec)

    public var name: String {
        switch self {
        case .master: "master"
        case .tlog: "tlog"
        case .resolver: "resolver"
        case .proxy: "proxy"
        }
    }

    public func encode(to w: inout WireWriter) {
        switch self {
        case .master(let s): w.u8(1); w.put(s)
        case .tlog(let s): w.u8(2); w.put(s)
        case .resolver(let s): w.u8(3); w.put(s)
        case .proxy(let s): w.u8(4); w.put(s)
        }
    }
    public init(from r: inout WireReader) throws {
        switch try r.u8() {
        case 1: self = .master(try r.get())
        case 2: self = .tlog(try r.get())
        case 3: self = .resolver(try r.get())
        case 4: self = .proxy(try r.get())
        case let t: throw WireError("bad role spec \(t)")
        }
    }
}

/// Sent by a master to the controller once its generation is live.
public struct RecoveryResult: Hashable, Sendable, WireCodable {
    public var recoveryID: UInt64
    public var generation: UInt64
    public var master: Endpoint
    public var proxies: [Endpoint]
    public var resolvers: [Endpoint]
    public var tlogs: [Endpoint]
    public var shardMap: ShardMap
    public var startVersion: Version
    public init(recoveryID: UInt64, generation: UInt64, master: Endpoint, proxies: [Endpoint], resolvers: [Endpoint],
                tlogs: [Endpoint], shardMap: ShardMap, startVersion: Version) {
        self.recoveryID = recoveryID; self.generation = generation; self.master = master; self.proxies = proxies
        self.resolvers = resolvers; self.tlogs = tlogs; self.shardMap = shardMap; self.startVersion = startVersion
    }
    public func encode(to w: inout WireWriter) {
        w.u64(recoveryID); w.u64(generation); w.string(master); w.strings(proxies); w.strings(resolvers)
        w.strings(tlogs); w.put(shardMap); w.u64(startVersion)
    }
    public init(from r: inout WireReader) throws {
        recoveryID = try r.u64(); generation = try r.u64(); master = try r.string(); proxies = try r.strings()
        resolvers = try r.strings(); tlogs = try r.strings(); shardMap = try r.get(); startVersion = try r.u64()
    }
}

// MARK: - Transaction logs

/// A mutation as stored in the log: once, with every tag that must apply it.
public struct LogMessage: Hashable, Sendable, WireCodable {
    public var order: UInt16
    public var mutation: Mutation
    public var tags: [Tag]
    public init(order: UInt16, mutation: Mutation, tags: [Tag]) { self.order = order; self.mutation = mutation; self.tags = tags }
    public func encode(to w: inout WireWriter) { w.u16(order); w.put(mutation); w.u32s(tags) }
    public init(from r: inout WireReader) throws { order = try r.u16(); mutation = try r.get(); tags = try r.u32s() }
}

public struct TaggedMeta: Hashable, Sendable, WireCodable {
    public var op: MetaOp
    public var tags: [Tag]
    public init(op: MetaOp, tags: [Tag]) { self.op = op; self.tags = tags }
    public func encode(to w: inout WireWriter) { w.put(op); w.u32s(tags) }
    public init(from r: inout WireReader) throws { op = try r.get(); tags = try r.u32s() }
}

/// Everything committed at one version.
public struct LogEntry: Hashable, Sendable, WireCodable {
    public var version: Version
    public var messages: [LogMessage]
    public var meta: [TaggedMeta]
    public init(version: Version, messages: [LogMessage], meta: [TaggedMeta]) {
        self.version = version; self.messages = messages; self.meta = meta
    }
    public func encode(to w: inout WireWriter) { w.u64(version); w.put(messages); w.put(meta) }
    public init(from r: inout WireReader) throws { version = try r.u64(); messages = try r.getArray(); meta = try r.getArray() }
}

public struct TLogPush: Hashable, Sendable, WireCodable {
    public var generation: UInt64
    public var prevVersion: Version
    public var entry: LogEntry
    /// Highest version the proxy knows every log has (piggybacked so logs can expose it).
    public var knownCommitted: Version
    public init(generation: UInt64, prevVersion: Version, entry: LogEntry, knownCommitted: Version) {
        self.generation = generation; self.prevVersion = prevVersion; self.entry = entry; self.knownCommitted = knownCommitted
    }
    public func encode(to w: inout WireWriter) { w.u64(generation); w.u64(prevVersion); w.put(entry); w.u64(knownCommitted) }
    public init(from r: inout WireReader) throws {
        generation = try r.u64(); prevVersion = try r.u64(); entry = try r.get(); knownCommitted = try r.u64()
    }
}

/// One version of a single tag's stream.
public struct PeekEntry: Hashable, Sendable, WireCodable {
    public var version: Version
    public var mutations: [(order: UInt16, mutation: Mutation)]
    public var meta: [MetaOp]
    public init(version: Version, mutations: [(order: UInt16, mutation: Mutation)], meta: [MetaOp]) {
        self.version = version; self.mutations = mutations; self.meta = meta
    }
    public static func == (a: PeekEntry, b: PeekEntry) -> Bool {
        a.version == b.version && a.meta == b.meta && a.mutations.count == b.mutations.count
            && zip(a.mutations, b.mutations).allSatisfy { $0.order == $1.order && $0.mutation == $1.mutation }
    }
    public func hash(into h: inout Hasher) { h.combine(version); h.combine(mutations.count) }
    public func encode(to w: inout WireWriter) {
        w.u64(version)
        w.array(mutations) { w, m in w.u16(m.order); w.put(m.mutation) }
        w.put(meta)
    }
    public init(from r: inout WireReader) throws {
        version = try r.u64()
        mutations = try r.array { r in (try r.u16(), try r.get(Mutation.self)) }
        meta = try r.getArray()
    }
}

public struct PeekReply: Hashable, Sendable, WireCodable {
    public enum Status: UInt8, Sendable {
        case ok = 0
        /// The requested start is older than this log retains for the tag: re-sync.
        case tooOld = 1
        /// The tag was retired (its replica was removed).
        case tagRemoved = 2
        /// This log is locked and has nothing more to give.
        case stopped = 3
    }
    public var status: Status
    public var entries: [PeekEntry]
    /// The stream is complete up to here (entries may be empty).
    public var end: Version
    /// This reply continues the stream from here: it holds everything in (begin, end]. A
    /// consumer applies it only if it has applied through `begin` (peeks stream several
    /// replies per request, so a lost one must show up as a gap, not be skipped).
    public var begin: Version
    public init(status: Status, entries: [PeekEntry], end: Version, begin: Version = 0) {
        self.status = status; self.entries = entries; self.end = end; self.begin = begin
    }
    public func encode(to w: inout WireWriter) { w.u8(status.rawValue); w.put(entries); w.u64(end); w.u64(begin) }
    public init(from r: inout WireReader) throws {
        status = Status(rawValue: try r.u8()) ?? .stopped; entries = try r.getArray(); end = try r.u64(); begin = try r.u64()
    }
}

public struct LockReply: Hashable, Sendable, WireCodable {
    /// False if this log doesn't know the requested generation (e.g. it restarted empty).
    public var known: Bool
    public var generation: UInt64
    public var durableVersion: Version
    public var knownCommitted: Version
    public init(known: Bool, generation: UInt64, durableVersion: Version, knownCommitted: Version) {
        self.known = known; self.generation = generation; self.durableVersion = durableVersion; self.knownCommitted = knownCommitted
    }
    public func encode(to w: inout WireWriter) { w.bool(known); w.u64(generation); w.u64(durableVersion); w.u64(knownCommitted) }
    public init(from r: inout WireReader) throws {
        known = try r.bool(); generation = try r.u64(); durableVersion = try r.u64(); knownCommitted = try r.u64()
    }
}

public struct FetchReply: Hashable, Sendable, WireCodable {
    public var ok: Bool
    public var entries: [LogEntry]
    /// Per-tag pop points (data at or below is not retained). Covers every live tag.
    public var popped: [(tag: Tag, version: Version)]
    public init(ok: Bool, entries: [LogEntry], popped: [(tag: Tag, version: Version)]) {
        self.ok = ok; self.entries = entries; self.popped = popped
    }
    public static func == (a: FetchReply, b: FetchReply) -> Bool {
        a.ok == b.ok && a.entries == b.entries && a.popped.map(\.tag) == b.popped.map(\.tag) && a.popped.map(\.version) == b.popped.map(\.version)
    }
    public func hash(into h: inout Hasher) { h.combine(ok); h.combine(entries) }
    public func encode(to w: inout WireWriter) {
        w.bool(ok); w.put(entries)
        w.array(popped) { w, p in w.u32(p.tag); w.u64(p.version) }
    }
    public init(from r: inout WireReader) throws {
        ok = try r.bool(); entries = try r.getArray()
        popped = try r.array { r in (try r.u32(), try r.u64()) }
    }
}

// MARK: - Resolution

public struct ResolveTxn: Hashable, Sendable, WireCodable {
    public var readVersion: Version
    public var readKeys: [Bytes]
    public var readRanges: [KeyRange]
    public var writeKeys: [Bytes]
    public init(readVersion: Version, readKeys: [Bytes], readRanges: [KeyRange], writeKeys: [Bytes]) {
        self.readVersion = readVersion; self.readKeys = readKeys; self.readRanges = readRanges; self.writeKeys = writeKeys
    }
    public func encode(to w: inout WireWriter) { w.u64(readVersion); w.blobs(readKeys); w.put(readRanges); w.blobs(writeKeys) }
    public init(from r: inout WireReader) throws {
        readVersion = try r.u64(); readKeys = try r.blobs(); readRanges = try r.getArray(); writeKeys = try r.blobs()
    }
}

public struct ResolveRequest: Hashable, Sendable, WireCodable {
    public var generation: UInt64
    public var proxyIndex: UInt32
    public var version: Version
    public var prevVersion: Version
    /// The proxy has applied metadata up to here; the reply carries anything newer.
    public var lastSeenVersion: Version
    public var txns: [ResolveTxn]
    /// Metadata ops committed (unconditionally) in this batch.
    public var metaOps: [MetaOp]
    /// Where the resolver sends its reply (the proxy, when the master forwarded the
    /// request; empty = reply to the sender).
    public var replyTo: Endpoint
    public init(generation: UInt64, proxyIndex: UInt32, version: Version, prevVersion: Version, lastSeenVersion: Version,
                txns: [ResolveTxn], metaOps: [MetaOp], replyTo: Endpoint = "") {
        self.generation = generation; self.proxyIndex = proxyIndex; self.version = version; self.prevVersion = prevVersion
        self.lastSeenVersion = lastSeenVersion; self.txns = txns; self.metaOps = metaOps; self.replyTo = replyTo
    }
    public func encode(to w: inout WireWriter) {
        w.u64(generation); w.u32(proxyIndex); w.u64(version); w.u64(prevVersion); w.u64(lastSeenVersion); w.put(txns); w.put(metaOps)
        w.string(replyTo)
    }
    public init(from r: inout WireReader) throws {
        generation = try r.u64(); proxyIndex = try r.u32(); version = try r.u64(); prevVersion = try r.u64()
        lastSeenVersion = try r.u64(); txns = try r.getArray(); metaOps = try r.getArray(); replyTo = try r.string()
    }
}

public struct StateOps: Hashable, Sendable, WireCodable {
    public var version: Version
    public var ops: [MetaOp]
    public init(version: Version, ops: [MetaOp]) { self.version = version; self.ops = ops }
    public func encode(to w: inout WireWriter) { w.u64(version); w.put(ops) }
    public init(from r: inout WireReader) throws { version = try r.u64(); ops = try r.getArray() }
}

public struct ResolveReply: Hashable, Sendable, WireCodable {
    /// The batch's commit version and the version before it.
    public var version: Version
    public var prevVersion: Version
    /// Per transaction: 0 commit, 1 conflict, 2 too old.
    public var verdicts: [UInt8]
    /// Metadata committed by other proxies at versions in (lastSeenVersion, version).
    public var stateOps: [StateOps]
    public init(version: Version, prevVersion: Version, verdicts: [UInt8], stateOps: [StateOps]) {
        self.version = version; self.prevVersion = prevVersion; self.verdicts = verdicts; self.stateOps = stateOps
    }
    public func encode(to w: inout WireWriter) { w.u64(version); w.u64(prevVersion); w.array(verdicts) { $0.u8($1) }; w.put(stateOps) }
    public init(from r: inout WireReader) throws {
        version = try r.u64(); prevVersion = try r.u64(); verdicts = try r.array { try $0.u8() }; stateOps = try r.getArray()
    }
}
