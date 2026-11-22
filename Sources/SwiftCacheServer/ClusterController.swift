import SwiftCacheCore

/// The elected cluster controller (one at a time, chosen through the coordinators).
///
/// * Tracks every process through its worker's registrations (which double as heartbeats).
/// * Starts a new transaction-system generation by recruiting a master whenever it takes
///   office or any role of the current generation (master, proxy, resolver, log) fails.
/// * Publishes `ServerDBInfo` to processes and `ClientInfo` to clients.
/// * Runs data distribution: forms shards from new storage servers, replaces failed
///   replicas, and rebalances slots. Every decision is a *metadata commit* through a
///   commit proxy, so it takes effect for every role at one exact version — and a
///   controller that has lost office can't commit anything, because the generation it
///   talks to is fenced.
public final class ClusterController: Role {
    public let context: RoleContext
    let config: ClusterConfig
    let leader: LeaderInfo
    let coordinators: [Endpoint]

    struct WorkerState {
        var reg: WorkerRegistration
        var lastSeen: UInt64
    }
    private var workers: [Endpoint: WorkerState] = [:]

    public enum Phase: Equatable, Sendable {
        case waiting(until: UInt64)
        case recovering(id: UInt64, master: Endpoint?, startedAt: UInt64)
        case running
    }
    public private(set) var phase: Phase = .waiting(until: 0)
    public private(set) var info: ServerDBInfo
    private var generationLiveAt: UInt64 = 0
    public private(set) var recoveries: UInt64 = 0
    private var abandonedMasters: [Endpoint] = []

    // Data distribution.
    private(set) var map = ShardMap()
    private var mapValid = false
    private var metaInFlight = false
    private var mapRefreshNeeded = false
    private var missingSince: [Tag: UInt64] = [:]
    private var moveStartedAt: UInt64 = 0
    private var migrationReady: Set<Tag> = []
    private var storageMaxApplied: Version = 0

    init(context: RoleContext, config: ClusterConfig, leader: LeaderInfo, coordinators: [Endpoint]) {
        self.context = context
        self.config = config
        self.leader = leader
        self.coordinators = coordinators
        self.info = ServerDBInfo(clusterController: context.endpoint, infoVersion: 1, ccChangeID: leader.changeID,
                                 generation: 0, recovered: false, master: "", proxies: [], resolvers: [], tlogs: [])
    }

    public func start() {
        phase = .waiting(until: context.now + config.controllerWarmup)
        context.log(.info, "cluster controller started; waiting for workers")
        context.every(100_000) { [weak self] in self?.tick() }
    }

    // MARK: Workers

    private func isAlive(_ w: WorkerState) -> Bool { context.now &- w.lastSeen < config.failureTimeout }

    private func aliveWorkers(_ cls: ProcessClass) -> [Endpoint] {
        workers.keys.sorted().filter { workers[$0]!.reg.processClass == cls && isAlive(workers[$0]!) }
    }

    private func workerOf(_ role: Endpoint) -> Endpoint { ServerProcess.workerEndpoint(role.processAddress) }

    private func roleAlive(_ role: Endpoint, strict: Bool) -> Bool {
        guard let w = workers[workerOf(role)], isAlive(w) else { return false }
        return !strict || w.reg.roles.contains(role)
    }

    /// Storage servers alive right now, by role endpoint.
    private func liveStorage() -> [Endpoint: StorageReport] {
        var out: [Endpoint: StorageReport] = [:]
        for ep in workers.keys.sorted() {
            let w = workers[ep]!
            if isAlive(w), let s = w.reg.storage, w.reg.roles.contains(s.endpoint) { out[s.endpoint] = s }
        }
        return out
    }

    // MARK: Main loop

    private func tick() {
        switch phase {
        case .waiting(let until):
            if context.now >= until { startRecovery("controller \(leader.endpoint) taking office") }
        case .recovering(let id, let master, let startedAt):
            if context.now &- startedAt > config.recoveryTimeout {
                if let master { abandon(master) }
                startRecovery("recovery \(id % 10_000) timed out")
            } else if let master, !roleAlive(master, strict: context.now &- startedAt > config.failureTimeout) {
                startRecovery("master of recovery \(id % 10_000) died")
            }
        case .running:
            if let reason = transactionSystemFailure() {
                startRecovery(reason)
            } else {
                haltStrayRoles()
                distributeData()
            }
        }
    }

    private func transactionSystemFailure() -> String? {
        let strict = context.now &- generationLiveAt > config.failureTimeout
        let roles = [info.master] + info.proxies + info.resolvers + info.tlogs
        for r in roles where !roleAlive(r, strict: strict) { return "\(r) failed" }
        // A generation formed while workers were missing runs degraded (e.g. 2 of 3
        // logs). Once enough workers are back and things have settled, recover again to
        // restore full redundancy.
        if context.now &- generationLiveAt > config.recoveryTimeout {
            let logs = min(config.logReplicas, aliveWorkers(.log).count)
            if info.tlogs.count < logs { return "restoring log redundancy (\(info.tlogs.count) of \(logs) logs)" }
            let proxies = min(config.commitProxies, aliveWorkers(.stateless).count)
            if info.proxies.count < proxies { return "restoring commit proxies (\(info.proxies.count) of \(proxies))" }
        }
        return nil
    }

    /// Halts transaction roles that workers report but that belong to no live generation
    /// (left over from generations a previous controller ran). They are already fenced —
    /// their logs are locked — this just frees them.
    private func haltStrayRoles() {
        guard config.haltStrayRoles, context.now &- generationLiveAt > config.failureTimeout else { return }
        let current = Set([info.master] + info.proxies + info.resolvers + info.tlogs)
        let kinds = ["#master-", "#proxy-", "#resolver-", "#tlog-"]
        for w in workers.keys.sorted() where isAlive(workers[w]!) {
            for role in workers[w]!.reg.roles where !current.contains(role) && kinds.contains(where: { role.contains($0) }) {
                context.send(.halt(role: role), to: w)
            }
        }
    }

    private func abandon(_ master: Endpoint) {
        abandonedMasters.append(master)
        context.send(.halt(role: master), to: workerOf(master))
    }

    // MARK: Recovery

    private func startRecovery(_ reason: String) {
        if case .recovering(_, let master?, _) = phase { abandon(master) }
        let stateless = aliveWorkers(.stateless), logs = aliveWorkers(.log)
        guard !stateless.isEmpty, !logs.isEmpty else {
            context.log(.warning, "can't recover (\(reason)): need stateless and log workers (have \(stateless.count), \(logs.count))")
            phase = .waiting(until: context.now + 500_000)
            return
        }
        recoveries += 1
        let id = context.randomID()
        context.log(.info, "starting recovery \(id % 10_000): \(reason)")
        let masterWorker = stateless[Int(recoveries) % stateless.count]
        let spec = MasterSpec(clusterController: context.endpoint, ccChangeID: leader.changeID, recoveryID: id,
                              logWorkers: logs, statelessWorkers: stateless, logReplicas: UInt32(config.logReplicas),
                              proxies: UInt32(config.commitProxies), resolvers: UInt32(config.resolvers),
                              fallbackMap: map, fallbackVersion: storageMaxApplied)
        phase = .recovering(id: id, master: nil, startedAt: context.now)
        context.request(to: masterWorker, timeout: config.requestTimeout, { .recruit(rid: $0, .master(spec)) }) { [weak self] reply in
            guard let self, case .recovering(let cur, nil, let started) = self.phase, cur == id else { return }
            if case .recruitReply(_, let ep)? = reply, !ep.isEmpty {
                self.phase = .recovering(id: id, master: ep, startedAt: started)
            } else {
                self.startRecovery("could not recruit a master on \(masterWorker)")
            }
        }
    }

    private func recoveryComplete(_ result: RecoveryResult, rid: UInt64, from: Endpoint) {
        context.send(.ack(rid: rid), to: from)
        guard case .recovering(let id, _, _) = phase, id == result.recoveryID else {
            abandon(result.master)
            return
        }
        let old = info
        info = ServerDBInfo(clusterController: context.endpoint, infoVersion: info.infoVersion + 1, ccChangeID: leader.changeID,
                            generation: result.generation, recovered: true, master: result.master,
                            proxies: result.proxies, resolvers: result.resolvers, tlogs: result.tlogs)
        map = result.shardMap
        mapValid = true
        metaInFlight = false
        mapRefreshNeeded = false
        migrationReady = []
        missingSince = [:]
        moveStartedAt = context.now
        generationLiveAt = context.now
        phase = .running
        context.log(.info, "generation \(result.generation) recovered at version \(result.startVersion); \(map.shards.count) shards")
        for w in workers.keys.sorted() where isAlive(workers[w]!) { context.send(.serverDBInfo(info), to: w) }
        // Halt the previous generation's roles (they are fenced already; this frees them).
        let current = Set([info.master] + info.proxies + info.resolvers + info.tlogs)
        for r in [old.master] + old.proxies + old.resolvers + old.tlogs + abandonedMasters where !r.isEmpty && !current.contains(r) {
            context.send(.halt(role: r), to: workerOf(r))
        }
        abandonedMasters = []
    }

    // MARK: Messages

    public func receive(_ message: Message, from: Endpoint) {
        switch message {
        case .registerWorker(let rid, let reg):
            workers[reg.worker] = WorkerState(reg: reg, lastSeen: context.now)
            if let s = reg.storage { storageMaxApplied = max(storageMaxApplied, s.appliedVersion) }
            context.send(.registerReply(rid: rid, info, assignedTag: assignedTag(reg.storage)), to: from)

        case .recoveryComplete(let rid, let result):
            recoveryComplete(result, rid: rid, from: from)

        case .getClientInfo(let rid):
            guard phase == .running else { return context.send(.errorReply(rid: rid, .unavailable), to: from) }
            let topo = map.topology(epoch: (info.generation << 32) | (map.version & 0xFFFF_FFFF))
            context.send(.clientInfoReply(rid: rid, ClientInfo(generation: info.generation, proxies: info.proxies, topology: topo)), to: from)

        case .getStatus(let rid):
            context.send(.statusReply(rid: rid, status), to: from)

        case .moveReady(let moveID, let tag):
            moveReady(moveID: moveID, tag: tag)

        default:
            if let rid = message.rid, !message.isReply { context.send(.errorReply(rid: rid, .invalidRequest), to: from) }
        }
    }

    private func assignedTag(_ report: StorageReport?) -> Tag {
        guard mapValid, let report else { return 0 }
        for id in map.shardIDs {
            if let m = map.shards[id]!.first(where: { $0.endpoint == report.endpoint }) { return m.tag }
        }
        return 0
    }

    // MARK: Data distribution

    private func commitMeta(_ ops: [MetaOp], _ what: String) {
        guard let proxy = info.proxies.first else { return }
        metaInFlight = true
        context.log(.info, "data distribution: \(what)")
        let request = CommitRequest(readVersion: 0, metaOps: ops)
        let generation = info.generation
        context.request(to: proxy, timeout: config.requestTimeout * 3, { .commit(rid: $0, request) }) { [weak self] reply in
            guard let self, self.info.generation == generation else { return }
            self.metaInFlight = false
            if case .commitReply(_, .committed(let stamp))? = reply {
                for op in ops { self.map.apply(op, at: stamp.version) }
            } else {
                // Unknown outcome: re-read the map from the transaction system.
                self.mapRefreshNeeded = true
            }
        }
    }

    private func refreshMap() {
        guard let proxy = info.proxies.first else { return }
        metaInFlight = true
        let generation = info.generation
        context.request(to: proxy, timeout: config.requestTimeout, { .getReadVersion(rid: $0) }) { [weak self] reply in
            guard let self, self.info.generation == generation else { return }
            guard case .readVersionReply(_, let v)? = reply else { self.metaInFlight = false; return }
            self.context.request(to: proxy, timeout: self.config.requestTimeout, { .getShardMap(rid: $0, minVersion: v) }) { reply in
                guard self.info.generation == generation else { return }
                self.metaInFlight = false
                if case .shardMapReply(_, let m)? = reply {
                    self.map = m
                    self.mapRefreshNeeded = false
                }
            }
        }
    }

    private func moveReady(moveID: UInt64, tag: Tag) {
        guard phase == .running, mapValid else { return }
        if let m = map.migration, m.moveID == moveID {
            migrationReady.insert(tag)
            return
        }
        if !metaInFlight, let shard = map.shardOf(tag: tag), let member = map.member(tag: tag), !member.active {
            commitMeta([.replicaReady(shard: shard, tag: tag)], "replica tag \(tag) of shard \(shard) caught up")
        }
    }

    private func distributeData() {
        guard mapValid, !metaInFlight, !info.proxies.isEmpty else { return }
        if mapRefreshNeeded { return refreshMap() }
        let now = context.now
        let r = config.replicationFactor
        let live = liveStorage()

        // 1. Retire members that are gone (dead, restarted, or wiped themselves).
        for shard in map.shardIDs {
            for m in map.members(of: shard) {
                // (A new member reports tag 0 until it hears its assignment; the timeout
                // below gives it time to.)
                if live[m.endpoint]?.tag == m.tag { missingSince[m.tag] = nil; continue }
                let since = missingSince[m.tag] ?? now
                missingSince[m.tag] = since
                let lastActive = m.active && map.activeMembers(of: shard).count == 1
                let limit = lastActive ? config.shardLossTimeout : config.failureTimeout
                guard now &- since > limit else { continue }
                var ops: [MetaOp] = [.removeReplica(shard: shard, tag: m.tag)]
                var scratch = map
                scratch.apply(ops[0], at: scratch.version)
                if scratch.activeMembers(of: shard).isEmpty {
                    // The shard's data is gone; hand its slots (empty) to the smallest live shard.
                    context.log(.error, "shard \(shard) lost its last replica; its data is lost")
                    let slots = scratch.slots(of: shard)
                    let others = scratch.shardIDs.filter { $0 != shard && !scratch.activeMembers(of: $0).isEmpty }
                    if let to = others.min(by: { scratch.slots(of: $0).count < scratch.slots(of: $1).count }), !slots.isEmpty {
                        ops.append(.reassignSlots(slots: slots, to: to))
                    }
                    if let mig = scratch.migration, mig.from == shard || mig.to == shard {
                        ops.append(.abortMigration(moveID: mig.moveID, to: mig.to))
                    }
                    if !others.isEmpty || slots.isEmpty { ops.append(.removeShard(shard: shard)) }
                }
                missingSince[m.tag] = nil
                return commitMeta(ops, "removing replica tag \(m.tag) (\(m.endpoint)) from shard \(shard)")
            }
        }

        // 2. Data movement in progress.
        if let mig = map.migration {
            if now &- moveStartedAt > config.moveTimeout {
                return commitMeta([.abortMigration(moveID: mig.moveID, to: mig.to)], "move \(mig.moveID) timed out")
            }
            let dest = map.activeMembers(of: mig.to)
            if !dest.isEmpty && dest.allSatisfy({ migrationReady.contains($0.tag) }) {
                return commitMeta([.transfer(moveID: mig.moveID, slots: mig.slots, from: mig.from, to: mig.to)],
                                  "transferring \(mig.slots.count) slots shard \(mig.from) -> \(mig.to)")
            }
            return
        }
        for shard in map.shardIDs {
            if let m = map.members(of: shard).first(where: { !$0.active }) {
                if now &- moveStartedAt > config.moveTimeout {
                    return commitMeta([.removeReplica(shard: shard, tag: m.tag)], "catch-up of tag \(m.tag) timed out")
                }
                return  // one movement at a time
            }
        }

        // 3. Put idle storage servers to work.
        let members = Set(map.shardIDs.flatMap { map.members(of: $0).map(\.endpoint) })
        let pool = live.keys.sorted().filter { !members.contains($0) && live[$0]!.tag == 0 }
        func newMembers(_ eps: [Endpoint]) -> [ShardMember] {
            eps.enumerated().map { ShardMember(tag: map.nextTag + Tag($0.offset), endpoint: $0.element, active: true) }
        }
        if map.shards.isEmpty {
            guard pool.count >= r else { return }
            var unowned = SlotSet()
            for slot in 0..<Slots.count where map.slotOwner[slot] < 0 { unowned.insert(slot) }
            return commitMeta([.createShard(shard: map.nextShardID, members: newMembers(Array(pool.prefix(r))), slots: unowned)],
                              "bootstrapping shard \(map.nextShardID) on \(pool.prefix(r).joined(separator: ", "))")
        }
        if let ep = pool.first {
            for shard in map.shardIDs where map.members(of: shard).count < r && !map.activeMembers(of: shard).isEmpty {
                let member = ShardMember(tag: map.nextTag, endpoint: ep, active: false)
                moveStartedAt = now
                return commitMeta([.addReplica(shard: shard, member: member, moveID: map.nextMoveID,
                                               sources: map.activeMembers(of: shard).map(\.endpoint), slots: map.slots(of: shard))],
                                  "re-replicating shard \(shard) onto \(ep)")
            }
        }
        if pool.count >= r {
            return commitMeta([.createShard(shard: map.nextShardID, members: newMembers(Array(pool.prefix(r))), slots: SlotSet())],
                              "forming shard \(map.nextShardID) on \(pool.prefix(r).joined(separator: ", "))")
        }

        // 4. Rebalance slots between fully replicated shards.
        rebalance()
    }

    private func rebalance() {
        let healthy = map.shardIDs.filter { id in
            let ms = map.members(of: id)
            return !ms.isEmpty && ms.allSatisfy(\.active)
        }
        guard healthy.count >= 2 else { return }
        var counts: [Int32: Int] = [:]
        for o in map.slotOwner where o >= 0 { counts[o, default: 0] += 1 }
        let total = healthy.reduce(0) { $0 + (counts[$1] ?? 0) }
        let target = total / healthy.count
        let ordered = healthy.sorted { ((counts[$0] ?? 0), $0) < ((counts[$1] ?? 0), $1) }
        let small = ordered.first!, big = ordered.last!
        let bigCount = counts[big] ?? 0, smallCount = counts[small] ?? 0
        guard bigCount - smallCount > 1 else { return }
        let n = min(config.migrationChunkSlots, bigCount - target, max(1, target - smallCount))
        guard n > 0 else { return }
        var slots = SlotSet()
        var taken = 0
        for slot in stride(from: Slots.count - 1, through: 0, by: -1) where map.slotOwner[slot] == big {
            slots.insert(slot)
            taken += 1
            if taken == n { break }
        }
        migrationReady = []
        moveStartedAt = context.now
        commitMeta([.beginMigration(moveID: map.nextMoveID, slots: slots, from: big, to: small,
                                    sources: map.activeMembers(of: big).map(\.endpoint))],
                   "migrating \(slots.count) slots shard \(big) -> \(small)")
    }

    // MARK: Status

    public var status: ClusterStatus {
        let phaseName: String
        switch phase {
        case .waiting: phaseName = "waiting"
        case .recovering: phaseName = "recovering"
        case .running: phaseName = map.shards.isEmpty ? "waiting_for_storage" : "running"
        }
        let live = liveStorage()
        var storage: [String] = []
        var inMap = Set<Endpoint>()
        for id in map.shardIDs {
            for m in map.members(of: id) {
                inMap.insert(m.endpoint)
                let applied = live[m.endpoint].map { "applied=\($0.appliedVersion)" } ?? "unreachable"
                storage.append("\(m.endpoint) shard=\(id) tag=\(m.tag) \(m.active ? "active" : "catching_up") \(applied)")
            }
        }
        for ep in live.keys.sorted() where !inMap.contains(ep) { storage.append("\(ep) unassigned") }
        let processes = workers.keys.sorted().map { ep -> ClusterStatus.Process in
            let w = workers[ep]!
            return ClusterStatus.Process(address: ep.processAddress, processClass: w.reg.processClass.description,
                                         roles: w.reg.roles, alive: isAlive(w))
        }
        var moves: [String] = []
        if let m = map.migration { moves.append("move \(m.moveID): \(m.slots.count) slots shard \(m.from) -> \(m.to)") }
        for id in map.shardIDs {
            for m in map.members(of: id) where !m.active { moves.append("tag \(m.tag) catching up into shard \(id)") }
        }
        return ClusterStatus(clusterController: context.endpoint, generation: info.generation, phase: phaseName,
                             master: info.master, proxies: info.proxies, resolvers: info.resolvers, tlogs: info.tlogs,
                             replicationFactor: UInt32(config.replicationFactor),
                             topology: map.topology(epoch: info.generation), storage: storage,
                             processes: processes, movesInProgress: moves, recoveries: recoveries)
    }
}
