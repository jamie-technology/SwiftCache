import Synchronization
import SwiftCacheCore

/// Low-level request routing: talks to the coordinator for versions/commits/topology and to
/// storage replicas for reads, refreshing the cached topology when it goes stale.
final class ClusterClient: Sendable {
    let coordinator: Endpoint
    let transport: any ClientTransport
    let options: DatabaseOptions
    private let cachedTopology = Mutex<Topology?>(nil)

    init(coordinator: Endpoint, transport: any ClientTransport, options: DatabaseOptions) {
        self.coordinator = coordinator
        self.transport = transport
        self.options = options
    }

    // MARK: Coordinator

    func readVersion() async throws -> Version {
        let reply: Message
        do {
            reply = try await transport.request(to: coordinator, timeout: options.requestTimeout) { .getReadVersion(rid: $0) }
        } catch let e as TransportError {
            throw SwiftCacheError.unavailable("coordinator: \(e)")
        }
        switch reply {
        case .readVersionReply(_, let v): return v
        case .errorReply(_, let code): throw SwiftCacheError(code: code)
        default: throw SwiftCacheError.internalError("unexpected reply \(reply.name)")
        }
    }

    func commit(_ request: CommitRequest) async throws -> Versionstamp {
        let reply: Message
        do {
            reply = try await transport.request(to: coordinator, timeout: options.requestTimeout) { .commit(rid: $0, request) }
        } catch is TransportError {
            throw SwiftCacheError.commitUnknownResult
        }
        switch reply {
        case .commitReply(_, .committed(let stamp)): return stamp
        case .commitReply(_, .failed(let code)): throw SwiftCacheError(code: code)
        case .errorReply(_, let code): throw SwiftCacheError(code: code)
        default: throw SwiftCacheError.internalError("unexpected reply \(reply.name)")
        }
    }

    func status() async throws -> ClusterStatus {
        let reply: Message
        do {
            reply = try await transport.request(to: coordinator, timeout: options.requestTimeout) { .getStatus(rid: $0) }
        } catch let e as TransportError {
            throw SwiftCacheError.unavailable("coordinator: \(e)")
        }
        guard case .statusReply(_, let s) = reply else { throw SwiftCacheError.internalError("unexpected reply \(reply.name)") }
        return s
    }

    func storageStats(_ endpoint: Endpoint) async throws -> StorageStats {
        let reply = try await transport.request(to: endpoint, timeout: options.requestTimeout) { .getStorageStats(rid: $0) }
        guard case .storageStatsReply(_, let s) = reply else { throw SwiftCacheError.internalError("unexpected reply \(reply.name)") }
        return s
    }

    // MARK: Topology

    func topology(refresh: Bool = false) async throws -> Topology {
        if !refresh, let t = cachedTopology.withLock({ $0 }) { return t }
        let reply: Message
        do {
            reply = try await transport.request(to: coordinator, timeout: options.requestTimeout) { .getTopology(rid: $0) }
        } catch let e as TransportError {
            throw SwiftCacheError.unavailable("coordinator: \(e)")
        }
        guard case .topologyReply(_, let t) = reply else {
            if case .errorReply(_, let code) = reply { throw SwiftCacheError(code: code) }
            throw SwiftCacheError.internalError("unexpected reply \(reply.name)")
        }
        cachedTopology.withLock { $0 = t }
        return t
    }

    private func replicaOrder(_ replicas: [Endpoint]) -> [Endpoint] {
        guard replicas.count > 1 else { return replicas }
        let start = Int(transport.random(UInt64(replicas.count)))
        return Array(replicas[start...] + replicas[..<start])
    }

    // MARK: Storage reads

    /// Reads one key at `version`. Returns (value, stamp) where value is nil if absent.
    func read(_ key: Bytes, at version: Version) async throws -> (Bytes?, Versionstamp) {
        var lastError = SwiftCacheError.unavailable("no replica for key")
        for attempt in 0..<4 {
            let topo = try await topology(refresh: attempt > 0)
            guard let shard = topo.shard(forKey: key), !shard.replicas.isEmpty else {
                lastError = .unavailable("no live replica for slot \(Slots.slot(for: key))")
                try await transport.sleep(micros: 20_000 << UInt64(attempt))
                continue
            }
            var stale = false
            for replica in replicaOrder(shard.replicas) {
                let reply: Message
                do {
                    reply = try await transport.request(to: replica, timeout: options.requestTimeout) {
                        .read(rid: $0, key: key, version: version)
                    }
                } catch {
                    lastError = .futureVersion
                    continue
                }
                switch reply {
                case .readReply(_, let value, let stamp):
                    return (value, stamp)
                case .errorReply(_, .wrongShard):
                    stale = true
                    lastError = .unavailable("stale topology")
                case .errorReply(_, .futureVersion):
                    lastError = .futureVersion
                case .errorReply(_, let code):
                    throw SwiftCacheError(code: code)
                default:
                    throw SwiftCacheError.internalError("unexpected reply \(reply.name)")
                }
                if stale { break }
            }
            if !stale { try await transport.sleep(micros: 10_000 << UInt64(attempt)) }
        }
        throw lastError
    }

    /// Reads `[begin, end)` at `version` across all shards, merged in key order.
    /// Returns up to `limit` entries and whether more exist beyond them.
    func readRange(_ range: KeyRange, at version: Version, limit: Int) async throws -> (entries: [KeyValueEntry], more: Bool) {
        var out: [KeyValueEntry] = []
        var cursor = range.begin
        while out.count < limit {
            let remaining = limit - out.count
            let page = try await readRangePage(KeyRange(begin: cursor, end: range.end), at: version, limit: remaining)
            out.append(contentsOf: page.entries)
            guard let resume = page.resumeFrom else { return (out, false) }
            if out.count >= limit { return (out, true) }
            cursor = resume
        }
        return (out, true)
    }

    /// One scatter-gather round. `resumeFrom` is set when some shard had more data than
    /// it returned: entries are complete up to (excluding) that key.
    private func readRangePage(_ range: KeyRange, at version: Version, limit: Int) async throws -> (entries: [KeyValueEntry], resumeFrom: Bytes?) {
        var lastError = SwiftCacheError.unavailable("range read failed")
        for attempt in 0..<5 {
            let topo = try await topology(refresh: attempt > 0)
            let shards = topo.shards.filter { s in topo.slotOwner.contains(s.id) }
            if shards.isEmpty || topo.slotOwner.contains(-1) {
                lastError = .unavailable("not all slots are assigned")
                try await transport.sleep(micros: 20_000 << UInt64(attempt))
                continue
            }
            do {
                let results = try await withThrowingTaskGroup(of: (entries: [KeyValueEntry], more: Bool, served: SlotSet).self) { group in
                    for shard in shards {
                        group.addTask {
                            try await self.readRangeFromShard(shard, range: range, version: version, limit: limit)
                        }
                    }
                    var all: [(entries: [KeyValueEntry], more: Bool, served: SlotSet)] = []
                    for try await r in group { all.append(r) }
                    return all
                }
                // Every slot must have been served exactly once.
                var union = SlotSet()
                var overlap = false
                for r in results {
                    if !union.isDisjoint(with: r.served) { overlap = true }
                    union.formUnion(r.served)
                }
                if overlap || union.count != Slots.count {
                    lastError = .unavailable("slot ownership changed during range read")
                    continue
                }
                var merged = results.flatMap(\.entries)
                merged.sort { bytesLess($0.key, $1.key) }
                // If a shard truncated its results, we only know the full picture up to
                // the smallest "last key" among truncated shards.
                var boundary: Bytes? = nil
                for r in results where r.more {
                    if let last = r.entries.last?.key, boundary == nil || bytesLess(last, boundary!) { boundary = last }
                }
                if let b = boundary {
                    merged = merged.filter { compareBytes($0.key, b) <= 0 }
                    let resume = b.keyAfter
                    if merged.count > limit { merged = Array(merged.prefix(limit)) }
                    return (merged, merged.count >= limit ? (merged.last!.key.keyAfter) : resume)
                }
                if merged.count > limit {
                    let page = Array(merged.prefix(limit))
                    return (page, page.last!.key.keyAfter)
                }
                return (merged, nil)
            } catch let e as SwiftCacheError where e == .transactionTooOld {
                throw e
            } catch let e as SwiftCacheError {
                lastError = e
                try await transport.sleep(micros: 10_000 << UInt64(attempt))
            }
        }
        throw lastError
    }

    private func readRangeFromShard(_ shard: Topology.Shard, range: KeyRange, version: Version, limit: Int) async throws -> (entries: [KeyValueEntry], more: Bool, served: SlotSet) {
        var lastError = SwiftCacheError.unavailable("no live replica for shard \(shard.id)")
        for replica in replicaOrder(shard.replicas) {
            let reply: Message
            do {
                reply = try await transport.request(to: replica, timeout: options.requestTimeout) {
                    .readRange(rid: $0, range: range, version: version, limit: UInt32(limit))
                }
            } catch {
                lastError = .futureVersion
                continue
            }
            switch reply {
            case .rangeReply(_, let entries, let more, let served):
                return (entries, more, served)
            case .errorReply(_, .wrongShard):
                throw SwiftCacheError.unavailable("stale topology")
            case .errorReply(_, .futureVersion):
                lastError = .futureVersion
            case .errorReply(_, let code):
                throw SwiftCacheError(code: code)
            default:
                throw SwiftCacheError.internalError("unexpected reply \(reply.name)")
            }
        }
        throw lastError
    }
}
