import Synchronization
import SwiftCacheCore

/// Low-level request routing. Finds the cluster controller through the coordinators,
/// gets `ClientInfo` (proxies + topology) from it, sends read versions and commits to the
/// current generation's commit proxies, and reads directly from storage replicas,
/// refreshing its cached information whenever something looks stale.
final class ClusterClient: Sendable {
    let coordinators: [Endpoint]
    let transport: any ClientTransport
    let options: DatabaseOptions
    private let cached = Mutex<ClientInfo?>(nil)
    /// The highest committed version this client has seen, and when (µs).
    private let latestSeen = Mutex<(version: Version, at: UInt64)>((0, 0))

    func observe(_ version: Version) {
        let now = transport.now()
        latestSeen.withLock { if version >= $0.version { $0 = (version, now) } }
    }

    /// Forget the remembered version (e.g. it predates a new generation).
    func forgetRecentVersion() {
        latestSeen.withLock { $0 = (0, 0) }
    }

    /// A recent committed version, if one was seen within `maxAge` µs.
    func recentVersion(maxAge: UInt64) -> Version? {
        let (v, at) = latestSeen.withLock { $0 }
        guard v > 0, transport.now() &- at <= maxAge else { return nil }
        return v
    }
    private let controller = Mutex<Endpoint?>(nil)

    init(coordinators: [Endpoint], transport: any ClientTransport, options: DatabaseOptions) {
        self.coordinators = coordinators.map { $0.contains("#") ? $0 : $0 + "#coord" }
        self.transport = transport
        self.options = options
        let me = WeakRef<ClusterClient>()
        let lanes = max(1, transport.laneCount)
        self.lanes = (0..<lanes).map { _ in
            Lane(grv: Coalescer<Int, Version>(maxBatch: .max) { items, done in
                guard let client = me.value else {
                    return transport.spawn { done(Array(repeating: .failure(SwiftCacheError.unavailable("client closed")), count: items.count)) }
                }
                client.fetchReadVersion { r in done(Array(repeating: r, count: items.count)) }
            })
        }
        me.value = self
    }

    // MARK: Discovery

    /// Asks coordinators for the controller, starting at a random one and skipping any in
    /// `skip` (coordinators whose answer turned out not to host a controller).
    private func findController(skip: Set<Endpoint> = []) async throws -> (controller: Endpoint, via: Endpoint?) {
        if skip.isEmpty, let c = controller.withLock({ $0 }) { return (c, nil) }
        let start = Int(transport.random(UInt64(max(1, coordinators.count))))
        for i in 0..<coordinators.count {
            let c = coordinators[(start + i) % coordinators.count]
            if skip.contains(c) { continue }
            if case .leaderReply(_, let leader?)? = try? await transport.request(to: c, timeout: options.requestTimeout, { .getLeader(rid: $0) }) {
                controller.withLock { $0 = leader.endpoint }
                return (leader.endpoint, c)
            }
        }
        throw SwiftCacheError.unavailable("no coordinator knows a cluster controller")
    }

    func clientInfo(refresh: Bool = false) async throws -> ClientInfo {
        if !refresh, let info = cached.withLock({ $0 }) { return info }
        // A coordinator outside the electing majority may name a candidate that isn't the
        // controller: then try the others.
        var skip = Set<Endpoint>()
        for _ in 0...coordinators.count {
            let (cc, via) = try await findController(skip: skip)
            if let via { skip.insert(via) }
            let reply = try? await transport.request(to: cc, timeout: options.requestTimeout) { .getClientInfo(rid: $0) }
            switch reply {
            case .clientInfoReply(_, let info)?:
                cached.withLock { $0 = info }
                return info
            case .errorReply(_, .unavailable)?:
                throw SwiftCacheError.unavailable("cluster is recovering")
            default:
                controller.withLock { $0 = nil }  // that controller is gone; ask the coordinators again
            }
        }
        throw SwiftCacheError.unavailable("cluster controller unreachable")
    }

    /// Forget cached routing information.
    func invalidate() {
        cached.withLock { $0 = nil }
    }

    private func proxy() async throws -> Endpoint {
        let info = try await clientInfo()
        guard !info.proxies.isEmpty else { throw SwiftCacheError.unavailable("no commit proxies") }
        return info.proxies[Int(transport.random(UInt64(info.proxies.count)))]
    }

    // MARK: Proxies

    // Batching state is per transport lane, so a request issued on a lane's thread is
    // batched, sent and completed on that thread.

    final class Lane: Sendable {
        /// Concurrent read-version requests on a lane share one request to the master.
        let grv: Coalescer<Int, Version>
        /// Concurrent commits to the same proxy share one message (each is still resolved
        /// and stamped individually by the proxy).
        let commits = Mutex<[Endpoint: Coalescer<CommitRequest, CommitResult>]>([:])
        /// Concurrent point reads to the same storage server share one message.
        let reads = Mutex<[Endpoint: Coalescer<ReadItem, ReadResult>]>([:])
        init(grv: Coalescer<Int, Version>) { self.grv = grv }
    }
    private let lanes: [Lane]

    /// Commit and read batching statistics, summed over lanes and servers.
    var batchingStats: (commits: Coalescer<CommitRequest, CommitResult>.Stats, reads: Coalescer<ReadItem, ReadResult>.Stats) {
        var c = Coalescer<CommitRequest, CommitResult>.Stats(), r = Coalescer<ReadItem, ReadResult>.Stats()
        for lane in lanes {
            for x in lane.commits.withLock({ Array($0.values) }).map(\.stats) {
                c.items += x.items; c.batches += x.batches; c.queued += x.queued; c.roundTrip += x.roundTrip
            }
            for x in lane.reads.withLock({ Array($0.values) }).map(\.stats) {
                r.items += x.items; r.batches += x.batches; r.queued += x.queued; r.roundTrip += x.roundTrip
            }
        }
        return (c, r)
    }
    private var lane: Lane { lanes[transport.currentLane()] }

    private func commitCoalescer(_ endpoint: Endpoint) -> Coalescer<CommitRequest, CommitResult> {
        lane.commits.withLock { all in
            if let c = all[endpoint] { return c }
            let transport = self.transport, timeout = self.options.requestTimeout
            let c = Coalescer<CommitRequest, CommitResult>(maxBatch: 512, maxInFlight: 1, now: { transport.now() }) { reqs, done in
                transport.send(to: endpoint, timeout: timeout, { .commitBatch(rid: $0, reqs) }) { result in
                    switch result {
                    case .success(.commitBatchReply(_, let results)) where results.count == reqs.count:
                        done(results.map { .success($0) })
                    case .success(.errorReply(_, let code)):
                        // The proxy no longer exists: none of these were taken.
                        done(Array(repeating: .success(.failed(code)), count: reqs.count))
                    case .success(let reply):
                        done(Array(repeating: .failure(SwiftCacheError.internalError("unexpected reply \(reply.name)")), count: reqs.count))
                    case .failure(let error):
                        done(Array(repeating: .failure(error), count: reqs.count))
                    }
                }
            }
            all[endpoint] = c
            return c
        }
    }

    private func readCoalescer(_ endpoint: Endpoint) -> Coalescer<ReadItem, ReadResult> {
        lane.reads.withLock { all in
            if let c = all[endpoint] { return c }
            let transport = self.transport, timeout = self.options.requestTimeout
            let c = Coalescer<ReadItem, ReadResult>(maxBatch: 256, maxInFlight: 1, now: { transport.now() }) { items, done in
                transport.send(to: endpoint, timeout: timeout, { .readBatch(rid: $0, items: items) }) { result in
                    switch result {
                    case .success(.readBatchReply(_, let results)) where results.count == items.count:
                        done(results.map { .success($0) })
                    case .success(.errorReply(_, let code)):
                        done(Array(repeating: .success(.error(code)), count: items.count))
                    case .success(let reply):
                        done(Array(repeating: .failure(SwiftCacheError.internalError("unexpected reply \(reply.name)")), count: items.count))
                    case .failure(let error):
                        done(Array(repeating: .failure(error), count: items.count))
                    }
                }
            }
            all[endpoint] = c
            return c
        }
    }

    func readVersion() async throws -> Version {
        try await lane.grv.submit(0)
    }

    /// Fetches a read version, calling back on the transport's thread. Synchronous up to
    /// the send when cluster info is cached; otherwise fetches it in a task first.
    private func fetchReadVersion(_ done: @escaping @Sendable (Result<Version, any Error>) -> Void) {
        guard let info = cached.withLock({ $0 }) else {
            return transport.spawn { [self] in
                do {
                    _ = try await clientInfo()
                    fetchReadVersion(done)
                } catch {
                    done(.failure(error))
                }
            }
        }
        // The master serves read versions in one round trip (under log leases); proxies
        // can too (with per-request log confirmation) if no master is advertised.
        let p: Endpoint
        if !info.master.isEmpty {
            p = info.master
        } else if !info.proxies.isEmpty {
            p = info.proxies[Int(transport.random(UInt64(info.proxies.count)))]
        } else {
            return transport.spawn { done(.failure(SwiftCacheError.unavailable("no commit proxies"))) }
        }
        transport.send(to: p, timeout: options.requestTimeout, { .getReadVersion(rid: $0) }) { [self] result in
            switch result {
            case .success(.readVersionReply(_, let v)):
                observe(v)
                done(.success(v))
            case .success(.errorReply(_, let code)):
                invalidate()
                done(.failure(SwiftCacheError(code: code)))
            case .success(let reply):
                done(.failure(SwiftCacheError.internalError("unexpected reply \(reply.name)")))
            case .failure(let error):
                invalidate()
                done(.failure(SwiftCacheError.unavailable("proxy \(p): \(error)")))
            }
        }
    }

    func commit(_ request: CommitRequest) async throws -> Versionstamp {
        let p = try await proxy()
        let result: CommitResult
        do {
            result = try await commitCoalescer(p).submit(request)
        } catch let e as SwiftCacheError {
            throw e
        } catch {
            // Sent, but no reply: it may or may not have committed.
            invalidate()
            throw SwiftCacheError.commitUnknownResult
        }
        switch result {
        case .committed(let stamp):
            observe(stamp.version)
            return stamp
        case .failed(let code):
            if code == .unavailable || code == .staleGeneration { invalidate() }
            throw SwiftCacheError(code: code)
        }
    }

    // MARK: Admin

    func status() async throws -> ClusterStatus {
        var skip = Set<Endpoint>()
        for _ in 0...coordinators.count {
            let (cc, via) = try await findController(skip: skip)
            if let via { skip.insert(via) }
            if case .statusReply(_, let s)? = try? await transport.request(to: cc, timeout: options.requestTimeout, { .getStatus(rid: $0) }) {
                return s
            }
            controller.withLock { $0 = nil }
        }
        throw SwiftCacheError.unavailable("cluster controller unreachable")
    }

    func storageStats(_ endpoint: Endpoint) async throws -> StorageStats {
        let reply = try await transport.request(to: endpoint, timeout: options.requestTimeout) { .getStorageStats(rid: $0) }
        guard case .storageStatsReply(_, let s) = reply else { throw SwiftCacheError.internalError("unexpected reply \(reply.name)") }
        return s
    }

    // MARK: Topology

    func topology(refresh: Bool = false) async throws -> Topology {
        try await clientInfo(refresh: refresh).topology
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
                let result: ReadResult
                do {
                    result = try await readCoalescer(replica).submit(ReadItem(key: key, version: version))
                } catch let e as SwiftCacheError {
                    throw e
                } catch {
                    lastError = .futureVersion
                    continue
                }
                switch result {
                case .value(let value, let stamp), .valueAt(let value, let stamp, _):
                    return (value, stamp)
                case .error(.wrongShard):
                    stale = true
                    lastError = .unavailable("stale topology")
                case .error(.futureVersion), .error(.staleGeneration):
                    lastError = .futureVersion
                case .error(let code):
                    throw SwiftCacheError(code: code)
                }
                if stale { break }
            }
            if !stale { try await transport.sleep(micros: 10_000 << UInt64(attempt)) }
        }
        throw lastError
    }

    typealias LatestValue = (value: Bytes?, stamp: Versionstamp, version: Version)

    /// Reads the latest value a replica of `key`'s shard has applied, with that version,
    /// into `box` (nil if the replica can't answer; the caller then falls back to a
    /// versioned read). Callback-based so it can run alongside a read-version request
    /// without a child task.
    func readLatest(_ key: Bytes, topology topo: Topology, into box: ResultBox<LatestValue?>) {
        guard let shard = topo.shard(forKey: key), !shard.replicas.isEmpty else { return box.set(nil) }
        let replica = shard.replicas[Int(transport.random(UInt64(shard.replicas.count)))]
        readCoalescer(replica).submit(ReadItem(key: key, version: 0)) { result in
            guard case .success(.valueAt(let v, let s, let ver)) = result else { return box.set(nil) }
            box.set((v, s, ver))
        }
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
