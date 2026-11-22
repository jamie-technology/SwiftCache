import SwiftCacheCore

/// The master of one transaction-system generation. Recruited by the cluster controller,
/// it first *recovers* the generation and then acts as the sequencer (hands out commit
/// versions, tracks the committed version).
///
/// Recovery, step by step (modelled on FoundationDB):
///
/// 1. **Read `CoreState`** from a coordinator majority with a fresh ballot. This promises
///    the coordinators won't accept a write from any older ballot, and returns the
///    current generation and its logs.
/// 2. **Lock the old generation's logs.** A commit needs *every* log, so locking any one
///    stops the old generation; and every acknowledged commit is on every log. The
///    recovery version is the minimum durable version among the locked logs: everything
///    at or below it is on all of them, nothing above it was acknowledged.
/// 3. **Recruit new logs** seeded from a locked old log (data ≤ recovery version that
///    storage hasn't released yet), and a resolver.
/// 4. **Write `CoreState`** (new generation, new logs) with the same ballot. If any newer
///    recovery has started, a majority has promised a higher ballot and the write fails:
///    this master is fenced and stops.
/// 5. **Recruit commit proxies** and tell the controller the generation is live.
///
/// Versions only move forward: the first new version is above every version any old log
/// ever saw, so versionstamps are never reused.
public final class MasterRole: Role {
    public let context: RoleContext
    let config: ClusterConfig
    let spec: MasterSpec
    let coordinators: [Endpoint]

    public enum Phase: String, Sendable {
        case readingCoreState, lockingLogs, recruiting, writingCoreState, recruitingProxies, running, failed
    }
    public private(set) var phase = Phase.readingCoreState
    private var ballot: Ballot
    private var oldState: CoreState?
    public private(set) var generation: UInt64 = 0
    private var recoveryVersion: Version = 0
    private var highestOldVersion: Version = 0
    private var seedFrom: [Endpoint] = []
    private var shardMap = ShardMap()
    private var tlogs: [Endpoint] = []
    private var resolvers: [Endpoint] = []
    private var proxies: [Endpoint] = []

    // Sequencer state.
    /// Last commit version handed out.
    public private(set) var lastVersion: Version = 0
    /// First commit version handed out by this master (0 = none yet).
    public private(set) var firstVersion: Version = 0
    private var minNextVersion: Version = 1
    public private(set) var committed: Version = 0
    /// Recent replies per proxy by request number: a retried request (its reply was lost)
    /// must get the *same* version, or the version chain would have a hole.
    private var replies: [UInt32: [UInt64: (prev: Version, version: Version)]] = [:]
    /// Each proxy's requests are served strictly in request-number order (retries can
    /// arrive out of order), so a proxy's batches are always in version order.
    private var nextRequest: [UInt32: UInt64] = [:]
    private var heldRequests: [UInt32: [UInt64: (rid: UInt64, from: Endpoint)]] = [:]
    private var lastProgress: UInt64 = 0
    private var lastProgressCommitted: Version = 0

    init(context: RoleContext, config: ClusterConfig, spec: MasterSpec, coordinators: [Endpoint]) {
        self.context = context
        self.config = config
        self.spec = spec
        self.coordinators = coordinators
        self.ballot = Ballot(round: 1, id: spec.recoveryID)
    }

    public func start() {
        context.ready()
        readCoreState(attempt: 0)
    }

    private func fail(_ reason: String) {
        guard phase != .failed else { return }
        context.log(.warning, "recovery \(spec.recoveryID % 10_000) abandoned in \(phase.rawValue): \(reason)")
        phase = .failed
        context.stopSelf()
    }

    // MARK: 1. Read CoreState

    private func readCoreState(attempt: Int) {
        guard attempt < 20 else { return fail("could not read CoreState from a coordinator majority") }
        let b = ballot
        var oks: [(Ballot, Bytes?)] = []
        var highest = b.round
        var answered = 0
        var done = false
        for c in coordinators {
            context.request(to: c, timeout: config.requestTimeout, { .genRead(rid: $0, b) }) { [weak self] reply in
                guard let self, self.phase == .readingCoreState, !done else { return }
                answered += 1
                if case .genReadReply(_, let ok, let rb, let wb, let value)? = reply {
                    if ok { oks.append((wb, value)) } else { highest = max(highest, rb.round) }
                }
                if oks.count >= majority(self.coordinators.count) {
                    done = true
                    let best = oks.max { $0.0 < $1.0 }!
                    self.oldState = best.1.flatMap { try? CoreState(bytes: $0) }
                    self.context.log(.info, "read CoreState at ballot \(b): \(self.oldState.map { "generation \($0.generation), \($0.tlogs.count) logs" } ?? "empty (new cluster)")")
                    self.lockOldLogs()
                } else if answered == self.coordinators.count {
                    done = true
                    // Someone promised a higher ballot (or we lack a majority): go higher.
                    self.ballot = Ballot(round: highest + 1, id: self.spec.recoveryID)
                    self.context.schedule(after: 50_000 + self.context.random(100_000)) { self.readCoreState(attempt: attempt + 1) }
                }
            }
        }
    }

    // MARK: 2. Lock the old generation

    private func lockOldLogs() {
        phase = .lockingLogs
        guard let old = oldState, !old.tlogs.isEmpty else {
            // A brand-new cluster.
            recoveryVersion = 0
            highestOldVersion = 0
            generation = 1
            shardMap = ShardMap()
            recruitLogsAndResolvers()
            return
        }
        generation = old.generation + 1
        lockAttempt(old, attempt: 0)
    }

    private func lockAttempt(_ old: CoreState, attempt: Int) {
        var known: [(Endpoint, LockReply)] = []
        var unknown = 0
        var answered = 0
        for t in old.tlogs {
            context.request(to: t, timeout: config.requestTimeout, { .tlogLock(rid: $0, generation: old.generation) }) { [weak self] reply in
                guard let self, self.phase == .lockingLogs else { return }
                answered += 1
                switch reply {
                case .tlogLockReply(_, let l)? where l.known: known.append((t, l))
                case .tlogLockReply?, .errorReply(_, .staleGeneration)?: unknown += 1  // definitively gone
                default: break  // no answer: maybe partitioned
                }
                guard answered == old.tlogs.count else { return }
                if !known.isEmpty {
                    self.finishLock(old, known: known)
                } else if unknown == old.tlogs.count {
                    self.forcedRecovery(old)
                } else {
                    // Some old logs are unreachable and none answered: we can't know what
                    // they acknowledged. Wait for them.
                    self.context.schedule(after: 200_000) { self.lockAttempt(old, attempt: attempt + 1) }
                }
            }
        }
    }

    private func finishLock(_ old: CoreState, known: [(Endpoint, LockReply)]) {
        recoveryVersion = max(old.startVersion, known.map(\.1.durableVersion).min()!)
        highestOldVersion = known.map(\.1.durableVersion).max()!
        seedFrom = known.sorted { $0.0 < $1.0 }.map(\.0)
        context.log(.info, "locked \(known.count)/\(old.tlogs.count) logs of generation \(old.generation); recovery version \(recoveryVersion)")
        // Rebuild the shard map from the metadata tag.
        fetchShardMap(from: seedFrom, attempt: 0)
    }

    private func fetchShardMap(from sources: [Endpoint], attempt: Int) {
        guard attempt < sources.count * 2 else { return fail("could not read the shard map from old logs") }
        let source = sources[attempt % sources.count]
        let upTo = recoveryVersion
        context.request(to: source, timeout: config.requestTimeout * 5, { .tlogFetch(rid: $0, upTo: upTo) }) { [weak self] reply in
            guard let self, self.phase == .lockingLogs else { return }
            guard case .tlogFetchReply(_, let f)? = reply, f.ok else {
                self.fetchShardMap(from: sources, attempt: attempt + 1)
                return
            }
            var map = ShardMap()
            for e in f.entries {
                for tm in e.meta where tm.tags.contains(metadataTag) { map.apply(tm.op, at: e.version) }
            }
            self.shardMap = map
            self.recruitLogsAndResolvers()
        }
    }

    /// Every log of the previous generation is definitively gone (all restarted): data
    /// committed but not yet applied by storage is lost. Continue from what storage has.
    private func forcedRecovery(_ old: CoreState) {
        context.log(.error, "every log of generation \(old.generation) is gone; forcing recovery (unapplied commits are lost)")
        recoveryVersion = max(old.startVersion, spec.fallbackVersion)
        highestOldVersion = recoveryVersion
        seedFrom = []
        shardMap = spec.fallbackMap
        recruitLogsAndResolvers()
    }

    // MARK: 3. Recruit logs and resolvers

    private func pick(_ workers: [Endpoint], _ n: Int, offset: Int) -> [Endpoint] {
        guard !workers.isEmpty else { return [] }
        let sorted = workers.sorted()
        return (0..<min(n, sorted.count)).map { sorted[($0 + offset) % sorted.count] }
    }

    private func recruitLogsAndResolvers() {
        phase = .recruiting
        let offset = Int(spec.recoveryID % 7)
        let logWorkers = pick(spec.logWorkers, Int(spec.logReplicas), offset: offset)
        let resolverWorkers = pick(spec.statelessWorkers, Int(spec.resolvers), offset: offset)
        guard !logWorkers.isEmpty, !resolverWorkers.isEmpty else { return fail("not enough workers") }
        let forcedTags = seedFrom.isEmpty && oldState != nil ? shardMap.allTags : []
        let tlogSpec = RoleSpec.tlog(TLogSpec(generation: generation, startVersion: recoveryVersion, seedFrom: seedFrom,
                                              initialTags: forcedTags))
        let resolverSpec = RoleSpec.resolver(ResolverSpec(generation: generation, startVersion: recoveryVersion,
                                                           proxyCount: spec.proxies))
        recruitAll(logWorkers.map { ($0, tlogSpec) } + resolverWorkers.map { ($0, resolverSpec) }) { [weak self] endpoints in
            guard let self, self.phase == .recruiting else { return }
            guard let endpoints else { return self.fail("recruiting logs/resolvers failed") }
            self.tlogs = Array(endpoints.prefix(logWorkers.count))
            self.resolvers = Array(endpoints.dropFirst(logWorkers.count))
            self.writeCoreState()
        }
    }

    private func recruitAll(_ jobs: [(Endpoint, RoleSpec)], completion: @escaping ([Endpoint]?) -> Void) {
        var results = [Endpoint?](repeating: nil, count: jobs.count)
        var answered = 0
        var failed = false
        for (i, job) in jobs.enumerated() {
            context.request(to: job.0, timeout: config.requestTimeout * 5, { .recruit(rid: $0, job.1) }) { reply in
                answered += 1
                if case .recruitReply(_, let ep)? = reply, !ep.isEmpty { results[i] = ep } else { failed = true }
                if answered == jobs.count { completion(failed ? nil : results.map { $0! }) }
            }
        }
    }

    // MARK: 4. Write CoreState (the fence)

    private func writeCoreState() {
        phase = .writingCoreState
        let state = CoreState(generation: generation, tlogs: tlogs, startVersion: recoveryVersion)
        let b = ballot
        var oks = 0
        var answered = 0
        var done = false
        for c in coordinators {
            context.request(to: c, timeout: config.requestTimeout, { .genWrite(rid: $0, b, value: state.bytes) }) { [weak self] reply in
                guard let self, self.phase == .writingCoreState, !done else { return }
                answered += 1
                if case .genWriteReply(_, true, _)? = reply { oks += 1 }
                if oks >= majority(self.coordinators.count) {
                    done = true
                    self.context.log(.info, "generation \(self.generation) written to coordinators (ballot \(b))")
                    self.recruitProxies()
                } else if answered == self.coordinators.count {
                    done = true
                    self.fail("CoreState write rejected: superseded by a newer recovery")
                }
            }
        }
    }

    // MARK: 5. Proxies, then go live

    private func recruitProxies() {
        phase = .recruitingProxies
        lastVersion = recoveryVersion
        minNextVersion = highestOldVersion + 1
        committed = recoveryVersion
        let workers = pick(spec.statelessWorkers, Int(spec.proxies), offset: Int(spec.recoveryID % 5) + 1)
        let jobs = workers.enumerated().map { i, w in
            (w, RoleSpec.proxy(ProxySpec(generation: generation, index: UInt32(i), startVersion: recoveryVersion,
                                         master: context.endpoint, resolvers: resolvers, tlogs: tlogs,
                                         clusterController: spec.clusterController, shardMap: shardMap,
                                         writeSnapshot: i == 0)))
        }
        recruitAll(jobs) { [weak self] endpoints in
            guard let self, self.phase == .recruitingProxies else { return }
            guard let endpoints else { return self.fail("recruiting proxies failed") }
            self.proxies = endpoints
            self.phase = .running
            self.lastProgress = self.context.now
            self.context.every(500_000) { [weak self] in self?.checkStall() }
            let result = RecoveryResult(recoveryID: self.spec.recoveryID, generation: self.generation, master: self.context.endpoint,
                                        proxies: endpoints, resolvers: self.resolvers, tlogs: self.tlogs,
                                        shardMap: self.shardMap, startVersion: self.recoveryVersion)
            self.context.log(.info, "generation \(self.generation) is live: \(endpoints.count) proxies, \(self.tlogs.count) logs, \(self.resolvers.count) resolvers, from version \(self.recoveryVersion)")
            self.reportRecovery(result, attempt: 0)
        }
    }

    private func reportRecovery(_ result: RecoveryResult, attempt: Int) {
        context.request(to: spec.clusterController, timeout: config.requestTimeout, { .recoveryComplete(rid: $0, result) }) { [weak self] reply in
            guard let self else { return }
            if case .ack? = reply { return }
            if attempt < 5 { self.reportRecovery(result, attempt: attempt + 1) }
        }
    }

    /// Liveness backstop: versions handed out but nothing committing for a long time
    /// means the generation is wedged (e.g. a version was lost). End it; the controller
    /// will notice the master is gone and recover a new generation.
    private func checkStall() {
        if committed != lastProgressCommitted || lastVersion <= committed {
            lastProgressCommitted = committed
            lastProgress = context.now
        } else if context.now &- lastProgress > config.recoveryTimeout {
            context.log(.error, "generation \(generation) stalled at \(committed) (allocated up to \(lastVersion)); ending it")
            context.stopSelf()
        }
    }

    // MARK: Sequencer

    public func receive(_ message: Message, from: Endpoint) {
        switch message {
        case .getCommitVersion(let rid, let proxy, let num):
            // Proxies start asking as soon as they're recruited (CoreState is written by then).
            guard phase == .running || phase == .recruitingProxies else { return context.send(.errorReply(rid: rid, .unavailable), to: from) }
            if let cached = replies[proxy]?[num] {
                context.send(.commitVersionReply(rid: rid, prevVersion: cached.prev, version: cached.version), to: from)
                return
            }
            let expected = nextRequest[proxy] ?? 0
            guard num >= expected else { return }  // ancient retry whose reply was evicted
            heldRequests[proxy, default: [:]][num] = (rid, from)
            // Serve this proxy's requests in order, as far as they're contiguous.
            var next = expected
            while let held = heldRequests[proxy]?.removeValue(forKey: next) {
                let version = max(lastVersion + 1, minNextVersion, context.versionClock)
                let prev = lastVersion
                lastVersion = version
                if firstVersion == 0 { firstVersion = version }
                replies[proxy, default: [:]][next] = (prev, version)
                if next >= 256 { replies[proxy]![next - 256] = nil }
                context.send(.commitVersionReply(rid: held.rid, prevVersion: prev, version: version), to: held.from)
                next += 1
            }
            nextRequest[proxy] = next

        case .reportCommitted(let rid, let version):
            committed = max(committed, version)
            context.send(.ack(rid: rid), to: from)

        case .getLiveCommitted(let rid):
            guard phase == .running || phase == .recruitingProxies else { return context.send(.errorReply(rid: rid, .unavailable), to: from) }
            context.send(.liveCommittedReply(rid: rid, version: committed), to: from)

        default:
            if let rid = message.rid, !message.isReply { context.send(.errorReply(rid: rid, .invalidRequest), to: from) }
        }
    }
}
