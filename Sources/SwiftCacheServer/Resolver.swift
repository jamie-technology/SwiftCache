import SwiftCacheCore

/// Optimistic concurrency control, FoundationDB style.
///
/// A transaction reads at `readVersion` and asks to commit at a later version. It
/// conflicts if any transaction that committed *after* its read version wrote a key in its
/// read set (point keys or ranges). Transactions in one batch are resolved in order, so a
/// transaction also conflicts with earlier transactions of the same batch.
struct ConflictSet {
    /// Last committed write version of each key written within the MVCC window.
    private var lastWrite: [ByteKey: Version] = [:]
    /// Recent commits (version, sorted written keys), oldest first; used for range checks.
    private var recent = Deque<(version: Version, keys: [Bytes])>()
    /// Reads older than this are rejected (window edge or recovery point).
    private(set) var oldestReadable: Version = 0
    private var lastSweep: Version = 0

    enum Outcome: Equatable {
        case commit
        case conflict
        case tooOld
    }

    /// Decides one transaction and, if it commits, records its writes at `commitVersion`.
    mutating func resolve(_ req: CommitRequest, commitVersion: Version) -> Outcome {
        resolve(ResolveTxn(readVersion: req.readVersion, readKeys: req.readConflictKeys,
                           readRanges: req.readConflictRanges, writeKeys: req.writeConflictKeys),
                commitVersion: commitVersion)
    }

    mutating func resolve(_ req: ResolveTxn, commitVersion: Version) -> Outcome {
        if req.readVersion < oldestReadable { return .tooOld }
        for key in req.readKeys {
            if let w = lastWrite[ByteKey(key)], w > req.readVersion { return .conflict }
        }
        if !req.readRanges.isEmpty {
            // Walk commits newer than the read version (newest first).
            for entry in recent.elements.reversed() {
                if entry.version <= req.readVersion { break }
                for range in req.readRanges where Self.intersects(entry.keys, range) {
                    return .conflict
                }
            }
        }
        record(req.writeKeys, at: commitVersion)
        return .commit
    }

    private mutating func record(_ keys: [Bytes], at version: Version) {
        guard !keys.isEmpty else { return }
        for key in keys { lastWrite[ByteKey(key)] = version }
        recent.append((version, keys.sorted(by: bytesLess)))
    }

    /// Whether any key of the sorted array falls in `range`.
    private static func intersects(_ sorted: [Bytes], _ range: KeyRange) -> Bool {
        var lo = 0, hi = sorted.count
        while lo < hi {
            let mid = (lo + hi) / 2
            if bytesLess(sorted[mid], range.begin) { lo = mid + 1 } else { hi = mid }
        }
        return lo < sorted.count && bytesLess(sorted[lo], range.end)
    }

    /// Moves the window forward: reads older than `oldest` are rejected from now on.
    mutating func advance(oldest: Version) {
        guard oldest > oldestReadable else { return }
        oldestReadable = oldest
        // Expire incrementally (no stop-the-world sweeps): a key's entry goes when the
        // write that last set it leaves the window.
        while let first = recent.first, first.version < oldest {
            recent.removeFirst()
            for key in first.keys {
                let k = ByteKey(key)
                if let i = lastWrite.index(forKey: k), lastWrite.values[i] == first.version { lastWrite.remove(at: i) }
            }
        }
    }

    /// Forgets all history; reads older than `version` are rejected (coordinator recovery).
    mutating func reset(rejectingReadsBefore version: Version) {
        lastWrite = [:]
        recent = Deque()
        oldestReadable = version
        lastSweep = version
    }

    var trackedKeys: Int { lastWrite.count }
}

/// The resolver of one generation: decides conflicts for every commit batch, in version
/// order, and relays metadata changes between commit proxies so they all route the same.
public final class ResolverRole: Role {
    public let context: RoleContext
    let config: ClusterConfig
    let spec: ResolverSpec

    private var conflicts = ConflictSet()
    private var lastVersion: Version
    private var pending: [Version: (from: Endpoint, rid: UInt64, req: ResolveRequest)] = [:]
    private var stateOps: [StateOps] = []
    private var lastSeen: [UInt32: Version] = [:]
    /// Recent replies to each proxy by version, re-sent if the proxy retries.
    private var recentReplies: [UInt32: [(version: Version, reply: ResolveReply)]] = [:]

    init(context: RoleContext, config: ClusterConfig, spec: ResolverSpec) {
        self.context = context
        self.config = config
        self.spec = spec
        self.lastVersion = spec.startVersion
    }

    public func start() {
        // History before this generation is unknown: older read versions can't be checked.
        conflicts.reset(rejectingReadsBefore: spec.startVersion)
        context.ready()
    }

    public func receive(_ message: Message, from: Endpoint) {
        guard case .resolve(let rid, let req) = message else {
            if let rid = message.rid, !message.isReply { context.send(.errorReply(rid: rid, .invalidRequest), to: from) }
            return
        }
        guard req.generation == spec.generation else {
            context.send(.errorReply(rid: rid, .staleGeneration), to: from)
            return
        }
        if req.version <= lastVersion {
            // A retry of something already resolved.
            if let cached = recentReplies[req.proxyIndex]?.first(where: { $0.version == req.version }) {
                context.send(.resolveReply(rid: rid, cached.reply), to: req.replyTo.isEmpty ? from : req.replyTo)
            }
            return
        }
        pending[req.prevVersion] = (from, rid, req)
        while let next = pending.removeValue(forKey: lastVersion) {
            resolve(next.req, rid: next.rid, from: next.from)
        }
    }

    private func resolve(_ req: ResolveRequest, rid: UInt64, from: Endpoint) {
        let window = req.version > config.mvccWindow ? req.version - config.mvccWindow : 0
        conflicts.advance(oldest: max(window, spec.startVersion))
        var verdicts: [UInt8] = []
        verdicts.reserveCapacity(req.txns.count)
        for txn in req.txns {
            switch conflicts.resolve(txn, commitVersion: req.version) {
            case .commit: verdicts.append(0)
            case .conflict: verdicts.append(1)
            case .tooOld: verdicts.append(2)
            }
        }
        let relayed = stateOps.filter { $0.version > req.lastSeenVersion && $0.version < req.version }
        if !req.metaOps.isEmpty { stateOps.append(StateOps(version: req.version, ops: req.metaOps)) }
        lastSeen[req.proxyIndex] = req.version
        lastVersion = req.version
        // Every proxy has seen everything at or below the minimum of their positions.
        if lastSeen.count >= Int(spec.proxyCount), let floor = lastSeen.values.min() {
            stateOps.removeAll { $0.version <= floor }
        }
        let reply = ResolveReply(version: req.version, prevVersion: req.prevVersion, verdicts: verdicts, stateOps: relayed)
        recentReplies[req.proxyIndex, default: []].append((req.version, reply))
        if recentReplies[req.proxyIndex]!.count > 64 { recentReplies[req.proxyIndex]!.removeFirst() }
        context.send(.resolveReply(rid: rid, reply), to: req.replyTo.isEmpty ? from : req.replyTo)
    }
}
