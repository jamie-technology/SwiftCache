import SwiftCacheCore

/// The coordinator is the cluster's sequencer, conflict resolver and cluster controller:
///
/// * hands out read versions (the latest fully replicated commit version),
/// * batches commit requests, resolves conflicts and assigns versionstamps,
/// * streams every batch, in version order, to every storage server and acknowledges
///   commits once all active replicas of all shards have applied them,
/// * detects failed replicas, re-replicates shards, forms new shards from new servers and
///   rebalances slots onto them,
/// * on restart, rebuilds all of this from storage-server reports (it keeps no disk state).
public final class Coordinator: ServerNode {
    public let endpoint: Endpoint
    let config: ClusterConfig
    let env: any NodeEnvironment

    // MARK: State

    enum Phase: Equatable {
        case recovering(deadline: UInt64)
        case truncating(deadline: UInt64, truncateTo: Version)
        case running
    }
    private(set) var phase: Phase = .recovering(deadline: 0)
    private(set) var epoch: UInt64 = 0

    /// Version of the last batch sent (the next batch's `prevVersion`).
    private(set) var lastBatchVersion: Version = 0
    /// Next batch version must be at least this (versions are never reused).
    private var minNextVersion: Version = 1
    /// Highest version acknowledged by every active replica.
    private(set) var committed: Version = 0
    /// Commits with a read version below this are rejected (coordinator recovery point).
    private var recoveryFloor: Version = 0
    private var resolver = Resolver()

    enum ServerState: Equatable {
        case unassigned
        case active
        case catchingUp(moveID: UInt64)
        case failed
    }

    struct Server {
        var endpoint: Endpoint
        var shard: Int32 = -1
        var state: ServerState = .unassigned
        /// Shard this server was in before it failed (preferred when re-assigning).
        var preferredShard: Int32 = -1
        var lastAckTime: UInt64 = 0
        var assignedAt: UInt64 = 0
        /// Batches sent but not yet acknowledged, kept so they can be resent after a
        /// broken connection instead of forcing a full re-sync.
        var unacked = Deque<Batch>()
        /// Send time of each unacked batch (parallel to `unacked`).
        var unackedSentAt = Deque<UInt64>()
    }
    private(set) var servers: [Endpoint: Server] = [:]
    private(set) var slotOwner = [Int32](repeating: -1, count: Slots.count)
    private var shardIDs: Set<Int32> = []
    private var nextShardID: Int32 = 0
    private var topologyEpoch: UInt64 = 0

    struct Move {
        enum Kind: Equatable {
            case catchUp(server: Endpoint, shard: Int32)
            case migrate(slots: SlotSet, from: Int32, to: Int32)
        }
        var id: UInt64
        var kind: Kind
        var startedAt: UInt64
        /// Version of the batch carrying `beginReceive` (0 until sent).
        var startVersion: Version = 0
        var ready: Set<Endpoint> = []
        var transferring = false
    }
    private(set) var move: Move?
    private var nextMoveID: UInt64 = 1
    /// Metadata ops waiting for the next batch, with the servers that must receive them.
    private var pendingMeta: [(targets: [Endpoint], op: MetaOp)] = []

    struct PendingCommit {
        var client: Endpoint
        var rid: UInt64
        var request: CommitRequest
    }
    private var pending: [PendingCommit] = []

    struct InflightBatch {
        var version: Version
        var waiting: Set<Endpoint>
        var replies: [(client: Endpoint, rid: UInt64, result: CommitResult)]
        var hasData: Bool
    }
    private var inflight = Deque<InflightBatch>()
    private var lastBatchAt: UInt64 = 0
    private var committedDataSinceLastBatch = false
    private var grvWaiters: [(client: Endpoint, rid: UInt64)] = []

    // Recovery bookkeeping.
    private var reports: [Endpoint: StorageReport] = [:]
    private var recoverReplies: [Endpoint: StorageReport] = [:]
    private var lateJoins: [StorageReport] = []
    private var excludedShards: Set<Int32> = []
    /// Shards that must have a confirmed replica before recovery may finish.
    private var expectedShards: Set<Int32> = []
    private var lastRecoverSend: UInt64 = 0

    // Stats.
    private(set) var commitsTotal: UInt64 = 0
    private(set) var conflictsTotal: UInt64 = 0

    public init(endpoint: Endpoint, config: ClusterConfig, env: any NodeEnvironment) {
        self.endpoint = endpoint
        self.config = config
        self.env = env
    }

    // MARK: Lifecycle

    public func start() {
        phase = .recovering(deadline: env.now + config.recoveryWindow)
        env.log(.info, "coordinator: starting recovery (waiting \(config.recoveryWindow / 1000)ms for storage reports)")
        scheduleTick()
    }

    private func scheduleTick() {
        env.schedule(after: config.batchInterval) { [weak self] in
            guard let self else { return }
            self.tick()
            self.scheduleTick()
        }
    }

    private func tick() {
        switch phase {
        case .recovering(let deadline):
            if env.now >= deadline { beginTruncation() }
        case .truncating(let deadline, let truncateTo):
            if env.now >= deadline {
                finishRecovery()
            } else if env.now &- lastRecoverSend >= config.joinInterval {
                // `recover` is idempotent; resend to anyone who hasn't answered.
                lastRecoverSend = env.now
                for ep in reports.keys.sorted() where recoverReplies[ep] == nil {
                    env.send(.recover(coordinatorEpoch: epoch, truncateTo: truncateTo), to: ep)
                }
            }
        case .running:
            detectFailures()
            checkMoveTimeout()
            let idle = env.now &- lastBatchAt >= config.idleBatchInterval
            if inflight.count < config.maxInflightBatches
                && (!pending.isEmpty || !pendingMeta.isEmpty || idle || committedDataSinceLastBatch) {
                emitBatch()
            }
        }
    }

    // MARK: Messages

    public func receive(_ message: Message, from: Endpoint) {
        switch message {
        case .getReadVersion(let rid):
            handleGetReadVersion(rid: rid, from: from)
        case .commit(let rid, let req):
            handleCommit(rid: rid, request: req, from: from)
        case .getTopology(let rid):
            env.send(.topologyReply(rid: rid, topology), to: from)
        case .getStatus(let rid):
            env.send(.statusReply(rid: rid, status), to: from)
        case .join(let report):
            handleJoin(report, from: from)
        case .batchAck(let version, let ackEpoch):
            if ackEpoch == epoch { handleAck(version: version, from: from) }
        case .moveReady(let moveID, let ackEpoch):
            if ackEpoch == epoch { handleMoveReady(moveID: moveID, from: from) }
        case .recoverReply(let report):
            handleRecoverReply(report, from: from)
        default:
            if let rid = message.rid { env.send(.errorReply(rid: rid, .invalidRequest), to: from) }
        }
    }

    // MARK: Client requests

    private func handleGetReadVersion(rid: UInt64, from: Endpoint) {
        guard phase == .running else {
            env.send(.errorReply(rid: rid, .unavailable), to: from)
            return
        }
        if committed < recoveryFloor {
            grvWaiters.append((from, rid))
        } else {
            env.send(.readVersionReply(rid: rid, version: committed), to: from)
        }
    }

    private func handleCommit(rid: UInt64, request: CommitRequest, from: Endpoint) {
        guard phase == .running else {
            env.send(.commitReply(rid: rid, .failed(.unavailable)), to: from)
            return
        }
        var size = 0
        for m in request.mutations {
            switch m {
            case .set(let k, let v, _):
                if k.count > config.maxKeySize || v.count > config.maxValueSize {
                    env.send(.commitReply(rid: rid, .failed(.tooLarge)), to: from)
                    return
                }
            case .clear(let k), .add(let k, _):
                if k.count > config.maxKeySize {
                    env.send(.commitReply(rid: rid, .failed(.tooLarge)), to: from)
                    return
                }
            }
            size += m.approximateSize
        }
        if size > config.maxTransactionBytes {
            env.send(.commitReply(rid: rid, .failed(.tooLarge)), to: from)
            return
        }
        pending.append(PendingCommit(client: from, rid: rid, request: request))
    }

    // MARK: The commit pipeline

    private func activeMembers(of shard: Int32) -> [Endpoint] {
        servers.values.filter { $0.shard == shard && $0.state == .active }.map(\.endpoint).sorted()
    }

    private func members(of shard: Int32) -> [Endpoint] {
        servers.values.filter { s in
            guard s.shard == shard else { return false }
            switch s.state {
            case .active, .catchingUp: return true
            default: return false
            }
        }.map(\.endpoint).sorted()
    }

    private func shardIsAvailable(_ shard: Int32) -> Bool {
        shard >= 0 && servers.values.contains { $0.shard == shard && $0.state == .active }
    }

    private func emitBatch() {
        let version = max(lastBatchVersion + 1, minNextVersion, env.versionClock)
        let oldest = max(committed > config.mvccWindow ? committed - config.mvccWindow : 0, recoveryFloor)
        resolver.advance(oldest: oldest)

        // Resolve transactions in arrival order.
        var entries: [Batch.Entry] = []
        var replies: [(client: Endpoint, rid: UInt64, result: CommitResult)] = []
        let take = min(pending.count, config.maxTransactionsPerBatch)
        let txns = pending.prefix(take)
        pending.removeFirst(take)
        var availability: [Int32: Bool] = [:]
        for txn in txns {
            var available = true
            for m in txn.request.mutations {
                let owner = slotOwner[Slots.slot(for: m.key)]
                if availability[owner] == nil { availability[owner] = shardIsAvailable(owner) }
                if availability[owner] == false { available = false; break }
            }
            guard available else {
                replies.append((txn.client, txn.rid, .failed(.unavailable)))
                continue
            }
            switch resolver.resolve(txn.request, commitVersion: version) {
            case .commit:
                if txn.request.mutations.isEmpty && txn.request.writeConflictKeys.isEmpty {
                    replies.append((txn.client, txn.rid, .committed(Versionstamp(version: version, order: 0))))
                    continue
                }
                let order = UInt16(entries.count)
                entries.append(Batch.Entry(order: order, mutations: txn.request.mutations))
                replies.append((txn.client, txn.rid, .committed(Versionstamp(version: version, order: order))))
                commitsTotal += 1
            case .conflict:
                conflictsTotal += 1
                replies.append((txn.client, txn.rid, .failed(.notCommitted)))
            case .tooOld:
                replies.append((txn.client, txn.rid, .failed(.transactionTooOld)))
            }
        }

        // Metadata ops in this batch.
        let meta = pendingMeta
        pendingMeta = []
        for item in meta {
            if case .beginReceive(let moveID, _, _, _) = item.op, move?.id == moveID {
                move!.startVersion = version
            }
        }

        // Per-server batches: each server only gets mutations for slots it stores.
        let migrating: (slots: SlotSet, to: Int32)? = {
            guard let m = move, m.startVersion != 0, version > m.startVersion,
                  case .migrate(let slots, _, let to) = m.kind else { return nil }
            return (slots, to)
        }()
        var required = Set<Endpoint>()
        for ep in servers.keys.sorted() {
            let s = servers[ep]!
            let receivesData: Bool
            switch s.state {
            case .active:
                receivesData = true
                required.insert(ep)
            case .catchingUp(let moveID):
                receivesData = move?.id == moveID && move!.startVersion != 0 && version > move!.startVersion
            case .unassigned, .failed:
                continue
            }
            var mine: [Batch.Entry] = []
            if receivesData {
                for e in entries {
                    let ms = e.mutations.filter { m in
                        let slot = Slots.slot(for: m.key)
                        if slotOwner[slot] == s.shard { return true }
                        if let mig = migrating, mig.to == s.shard, mig.slots.contains(slot), s.state == .active { return true }
                        return false
                    }
                    if !ms.isEmpty { mine.append(Batch.Entry(order: e.order, mutations: ms)) }
                }
            }
            let myMeta = meta.filter { $0.targets.contains(ep) }.map(\.op)
            let batch = Batch(coordinatorEpoch: epoch, version: version, prevVersion: lastBatchVersion,
                              committedVersion: committed, oldestVersion: oldest, entries: mine, meta: myMeta)
            servers[ep]!.unacked.append(batch)
            servers[ep]!.unackedSentAt.append(env.now)
            env.send(.batch(batch), to: ep)
        }

        // Apply metadata effects on our own routing state after the batch is built.
        for item in meta {
            switch item.op {
            case .transfer(_, let slots, _, let to):
                for slot in slots.sorted { slotOwner[slot] = to }
                topologyEpoch += 1
                move = nil
            case .gainEmpty(let shard, let slots):
                for slot in slots.sorted { slotOwner[slot] = shard }
                topologyEpoch += 1
            case .beginReceive, .abortReceive:
                break
            }
        }

        inflight.append(InflightBatch(version: version, waiting: required, replies: replies, hasData: !entries.isEmpty))
        lastBatchVersion = version
        lastBatchAt = env.now
        committedDataSinceLastBatch = false
        advanceCommitted()
        if !meta.isEmpty { tryScheduleWork() }
    }

    private func handleAck(version: Version, from: Endpoint) {
        guard var s = servers[from] else { return }
        switch s.state {
        case .active, .catchingUp: break
        default: return
        }
        s.lastAckTime = env.now
        while let first = s.unacked.first, first.version <= version {
            s.unacked.removeFirst()
            s.unackedSentAt.removeFirst()
        }
        servers[from] = s
        inflight.modifyEach { b in
            if b.version <= version { b.waiting.remove(from) }
        }
        advanceCommitted()
    }

    private func advanceCommitted() {
        while let first = inflight.first, first.waiting.isEmpty {
            inflight.removeFirst()
            committed = first.version
            if first.hasData { committedDataSinceLastBatch = true }
            for r in first.replies { env.send(.commitReply(rid: r.rid, r.result), to: r.client) }
        }
        if committed >= recoveryFloor && !grvWaiters.isEmpty {
            let waiting = grvWaiters
            grvWaiters = []
            for w in waiting { env.send(.readVersionReply(rid: w.rid, version: committed), to: w.client) }
        }
    }

    // MARK: Failure detection

    private func detectFailures() {
        for ep in servers.keys.sorted() {
            let s = servers[ep]!
            switch s.state {
            case .active, .catchingUp:
                // Only count time spent owing us an acknowledgement: if the pipeline is
                // stalled (no batches being sent), silence says nothing about this server.
                guard let oldestUnacked = s.unackedSentAt.first else { continue }
                let silence = env.now &- max(oldestUnacked, s.assignedAt)
                // Never drop the last live replica of a shard on a short timeout: if every
                // replica of a shard looks dead it's likelier that *we* are cut off, and
                // dropping it would throw away the shard's data. Commits stall instead.
                let isLastReplica = s.state == .active && activeMembers(of: s.shard).count == 1
                if silence > (isLastReplica ? config.shardLossTimeout : config.failureTimeout) {
                    env.log(.warning, "coordinator: \(ep) (shard \(s.shard)) failed to acknowledge batches; marking failed")
                    markFailed(ep)
                }
            default:
                break
            }
        }
    }

    private func markFailed(_ ep: Endpoint) {
        guard var s = servers[ep] else { return }
        let shard = s.shard
        let wasCatchingUp: Bool = { if case .catchingUp = s.state { return true } else { return false } }()
        s.state = .failed
        s.unacked = Deque()
        s.unackedSentAt = Deque()
        s.preferredShard = shard
        servers[ep] = s
        topologyEpoch += 1
        inflight.modifyEach { $0.waiting.remove(ep) }

        if let m = move {
            switch m.kind {
            case .catchUp(let server, _) where server == ep:
                move = nil
            case .migrate(_, _, let to) where to == shard && !wasCatchingUp:
                move!.ready.remove(ep)
                if activeMembers(of: to).isEmpty { abortMove(reason: "destination shard \(to) has no live replicas") }
                else { maybeTransfer() }
            default:
                break
            }
        }
        if shard >= 0 && activeMembers(of: shard).isEmpty && shardIDs.contains(shard) {
            shardLost(shard)
        }
        advanceCommitted()
        tryScheduleWork()
    }

    private func shardLost(_ shard: Int32) {
        env.log(.error, "coordinator: shard \(shard) has no live replicas; its data is lost and its slots are reassigned")
        shardIDs.remove(shard)
        // Members still catching up can't finish without a source.
        for ep in servers.keys.sorted() where servers[ep]!.shard == shard {
            servers[ep]!.shard = -1
            if servers[ep]!.state != .failed { servers[ep]!.state = .unassigned }
            servers[ep]!.preferredShard = -1
        }
        if let m = move {
            switch m.kind {
            case .catchUp(_, let s) where s == shard: move = nil
            case .migrate(_, let from, let to) where from == shard || to == shard: abortMove(reason: "shard \(shard) lost")
            default: break
            }
        }
        var orphaned = SlotSet()
        for slot in 0..<Slots.count where slotOwner[slot] == shard { orphaned.insert(slot) }
        for slot in orphaned.sorted { slotOwner[slot] = -1 }
        assignOrphanedSlots(orphaned)
    }

    /// Gives unowned slots (empty) to the live shard with the fewest slots.
    private func assignOrphanedSlots(_ orphaned: SlotSet) {
        guard !orphaned.isEmpty else { return }
        let live = shardIDs.sorted().filter { !activeMembers(of: $0).isEmpty }
        guard let target = live.min(by: { slotCount($0) < slotCount($1) }) else {
            env.log(.error, "coordinator: no live shard to take \(orphaned.count) slots")
            return
        }
        pendingMeta.append((members(of: target), .gainEmpty(shardID: target, slots: orphaned)))
    }

    private func slotCount(_ shard: Int32) -> Int { slotOwner.lazy.filter { $0 == shard }.count }

    // MARK: Membership

    private func handleJoin(_ report: StorageReport, from: Endpoint) {
        switch phase {
        case .recovering:
            reports[from] = report
            return
        case .truncating:
            lateJoins.append(report)
            return
        case .running:
            break
        }
        if let s = servers[from] {
            switch s.state {
            case .unassigned:
                return  // already queued
            case .active, .catchingUp:
                // A join sent before our assignment arrived is stale.
                if env.now &- s.assignedAt < config.coordinatorTimeout { return }
                // The server lost some batches (broken connection): if we still hold
                // everything after its applied version, resend instead of re-syncing.
                if report.coordinatorEpoch == epoch && report.shardID == s.shard {
                    if report.appliedVersion == lastBatchVersion { return }
                    if let idx = s.unacked.elements.firstIndex(where: { $0.prevVersion == report.appliedVersion }) {
                        let resend = Array(s.unacked.elements[idx...])
                        env.log(.info, "coordinator: resending \(resend.count) batches to \(from) from \(report.appliedVersion)")
                        for b in resend { env.send(.batch(b), to: from) }
                        return
                    }
                }
                env.log(.warning, "coordinator: \(from) rejoined while \(s.state); re-syncing it")
                markFailed(from)
            case .failed:
                break
            }
        }
        var s = servers[from] ?? Server(endpoint: from)
        s.state = .unassigned
        if s.preferredShard < 0 { s.preferredShard = report.shardID }
        s.shard = -1
        servers[from] = s
        env.log(.info, "coordinator: \(from) joined (unassigned pool)")
        tryScheduleWork()
    }

    private func assign(_ ep: Endpoint, shard: Int32, mode: Assignment.Mode, slots: SlotSet, state: ServerState) {
        servers[ep]!.shard = shard
        servers[ep]!.state = state
        servers[ep]!.assignedAt = env.now
        servers[ep]!.lastAckTime = env.now
        env.send(.assign(Assignment(coordinatorEpoch: epoch, shardID: shard, mode: mode, slots: slots)), to: ep)
        topologyEpoch += 1
    }

    /// Assigns pooled servers, starts re-replication, forms shards and rebalances.
    private func tryScheduleWork() {
        guard phase == .running else { return }
        var pool = servers.values.filter { $0.state == .unassigned }.map(\.endpoint).sorted()
        let r = max(1, config.replicationFactor)

        // Bootstrap: the first shard owns every slot.
        if shardIDs.isEmpty && pool.count >= r {
            let id = nextShardID
            nextShardID += 1
            shardIDs.insert(id)
            let members = Array(pool.prefix(r))
            pool.removeFirst(r)
            var unowned = SlotSet()
            for slot in 0..<Slots.count where slotOwner[slot] < 0 { unowned.insert(slot) }
            for slot in unowned.sorted { slotOwner[slot] = id }
            for ep in members { assign(ep, shard: id, mode: .activeEmpty, slots: unowned, state: .active) }
            env.log(.info, "coordinator: bootstrapped shard \(id) with \(members)")
        }

        // Re-replicate under-replicated shards, one catch-up at a time.
        if move == nil && !pool.isEmpty {
            for shard in shardIDs.sorted() where members(of: shard).count < r && !activeMembers(of: shard).isEmpty {
                let pick = pool.first { servers[$0]!.preferredShard == shard } ?? pool.first { servers[$0]!.preferredShard < 0 || !shardIDs.contains(servers[$0]!.preferredShard) }
                guard let ep = pick else { continue }
                pool.removeAll { $0 == ep }
                startCatchUp(ep, shard: shard)
                break
            }
        }

        // Form new shards from whole groups of fresh servers. Servers that prefer an
        // existing under-replicated shard wait for it.
        let fresh = pool.filter { ep in
            let pref = servers[ep]!.preferredShard
            return pref < 0 || !shardIDs.contains(pref) || members(of: pref).count >= r
        }
        if !shardIDs.isEmpty && fresh.count >= r {
            let id = nextShardID
            nextShardID += 1
            shardIDs.insert(id)
            let groupMembers = Array(fresh.prefix(r))
            for ep in groupMembers { assign(ep, shard: id, mode: .activeEmpty, slots: SlotSet(), state: .active) }
            env.log(.info, "coordinator: formed shard \(id) with \(groupMembers)")
        }

        if move == nil { rebalance() }
    }

    private func startCatchUp(_ ep: Endpoint, shard: Int32) {
        let id = nextMoveID
        nextMoveID += 1
        let sources = activeMembers(of: shard)
        var slots = SlotSet()
        for slot in 0..<Slots.count where slotOwner[slot] == shard { slots.insert(slot) }
        move = Move(id: id, kind: .catchUp(server: ep, shard: shard), startedAt: env.now)
        assign(ep, shard: shard, mode: .catchUp, slots: SlotSet(), state: .catchingUp(moveID: id))
        pendingMeta.append(([ep], .beginReceive(moveID: id, shardID: shard, slots: slots, sources: sources)))
        env.log(.info, "coordinator: move \(id): \(ep) catching up into shard \(shard) from \(sources)")
    }

    private func rebalance() {
        let live = shardIDs.sorted().filter { !activeMembers(of: $0).isEmpty && members(of: $0).count == activeMembers(of: $0).count }
        guard live.count >= 2 else { return }
        var counts: [Int32: Int] = [:]
        for o in slotOwner where o >= 0 { counts[o, default: 0] += 1 }
        let total = counts.values.reduce(0, +)
        let target = total / live.count
        let byCount = live.sorted { (counts[$0] ?? 0, $0) < (counts[$1] ?? 0, $1) }
        let smallest = byCount.first!, largest = byCount.last!
        let big = counts[largest] ?? 0, small = counts[smallest] ?? 0
        guard big - small > 1 else { return }
        let n = min(config.migrationChunkSlots, big - target, max(1, target - small))
        guard n > 0 else { return }
        var slots = SlotSet()
        var taken = 0
        for slot in stride(from: Slots.count - 1, through: 0, by: -1) where slotOwner[slot] == largest {
            slots.insert(slot)
            taken += 1
            if taken == n { break }
        }
        let id = nextMoveID
        nextMoveID += 1
        move = Move(id: id, kind: .migrate(slots: slots, from: largest, to: smallest), startedAt: env.now)
        pendingMeta.append((activeMembers(of: smallest),
                            .beginReceive(moveID: id, shardID: smallest, slots: slots, sources: activeMembers(of: largest))))
        env.log(.info, "coordinator: move \(id): migrating \(slots.count) slots shard \(largest) -> \(smallest)")
    }

    private func handleMoveReady(moveID: UInt64, from: Endpoint) {
        guard let m = move, m.id == moveID else { return }
        switch m.kind {
        case .catchUp(let server, let shard):
            guard server == from, servers[from]?.state == .catchingUp(moveID: moveID) else { return }
            servers[from]!.state = .active
            servers[from]!.lastAckTime = env.now
            topologyEpoch += 1
            move = nil
            env.log(.info, "coordinator: move \(moveID): \(from) is now an active replica of shard \(shard)")
            tryScheduleWork()
        case .migrate:
            move!.ready.insert(from)
            maybeTransfer()
        }
    }

    private func maybeTransfer() {
        guard let m = move, !m.transferring, case .migrate(let slots, let from, let to) = m.kind else { return }
        let dest = activeMembers(of: to)
        guard !dest.isEmpty, dest.allSatisfy({ m.ready.contains($0) }) else { return }
        move!.transferring = true
        let targets = (members(of: from) + members(of: to)).sorted()
        pendingMeta.append((targets, .transfer(moveID: m.id, slots: slots, from: from, to: to)))
        env.log(.info, "coordinator: move \(m.id): transferring \(slots.count) slots \(from) -> \(to)")
    }

    private func abortMove(reason: String) {
        guard let m = move else { return }
        env.log(.warning, "coordinator: aborting move \(m.id): \(reason)")
        switch m.kind {
        case .catchUp(let server, _):
            if servers[server]?.state == .catchingUp(moveID: m.id) {
                servers[server]!.state = .unassigned
                servers[server]!.shard = -1
            }
        case .migrate(_, _, let to):
            if !m.transferring {
                pendingMeta.append((members(of: to), .abortReceive(moveID: m.id, shardID: to)))
            } else {
                return  // the transfer is already queued; let it finish
            }
        }
        move = nil
    }

    private func checkMoveTimeout() {
        if let m = move, env.now &- m.startedAt > config.moveTimeout {
            abortMove(reason: "timed out")
        }
    }

    // MARK: Recovery

    private func beginTruncation() {
        let all = reports.values.sorted { $0.endpoint < $1.endpoint }
        epoch = (all.map(\.coordinatorEpoch).max() ?? 0) + 1
        var maxApplied: [Int32: Version] = [:]
        for r in all where r.isActive && r.shardID >= 0 {
            maxApplied[r.shardID] = max(maxApplied[r.shardID] ?? 0, r.appliedVersion)
        }
        // Every version up to `knownCommitted` was applied by all live replicas of every
        // shard. A shard whose best replica is behind it only has stale (previously failed)
        // replicas left: its real replicas are gone, so treat it as lost rather than
        // rolling the whole cluster back past acknowledged commits.
        let knownCommitted = all.map(\.committedVersion).max() ?? 0
        for (shard, applied) in maxApplied where applied < knownCommitted {
            env.log(.error, "coordinator: shard \(shard)'s replicas are all stale (\(applied) < committed \(knownCommitted)); treating as lost")
            maxApplied[shard] = nil
        }
        let truncateTo = maxApplied.values.min() ?? 0
        expectedShards = Set(maxApplied.keys)
        for r in all where r.isActive && r.shardID >= 0 && maxApplied[r.shardID] == nil {
            excludedShards.insert(r.shardID)
        }
        let highest = all.map(\.appliedVersion).max() ?? 0
        minNextVersion = max(minNextVersion, highest + 1)
        env.log(.info, "coordinator: epoch \(epoch): \(all.count) storage reports, \(maxApplied.count) shards; truncating to \(truncateTo)")
        if all.isEmpty {
            phase = .truncating(deadline: env.now, truncateTo: 0)
            finishRecovery()
            return
        }
        phase = .truncating(deadline: env.now + config.recoveryTruncateTimeout, truncateTo: truncateTo)
        lastRecoverSend = env.now
        for r in all {
            env.send(.recover(coordinatorEpoch: epoch, truncateTo: truncateTo), to: r.endpoint)
        }
    }

    private func handleRecoverReply(_ report: StorageReport, from: Endpoint) {
        guard case .truncating = phase, report.coordinatorEpoch == epoch else { return }
        recoverReplies[from] = report
        reports[from] = reports[from] ?? report
        if recoverReplies.count == reports.count { finishRecovery() }
    }

    private func finishRecovery() {
        guard case .truncating(_, let truncateTo) = phase else { return }
        let replies = recoverReplies.values.sorted { $0.endpoint < $1.endpoint }

        // Every shard that had data must have at least one replica confirmed at the
        // truncation point. Otherwise (e.g. we were partitioned and lost the replies)
        // start recovery over rather than declaring data lost.
        let confirmed = Set(replies.filter { $0.isActive && $0.appliedVersion == truncateTo }.map(\.shardID))
        let missing = expectedShards.subtracting(confirmed)
        if !missing.isEmpty {
            env.log(.warning, "coordinator: no confirmed replica for shards \(missing.sorted()); restarting recovery")
            reports = [:]
            recoverReplies = [:]
            excludedShards = []
            expectedShards = []
            phase = .recovering(deadline: env.now + config.recoveryWindow)
            return
        }

        // Rebuild membership: active servers at the truncation point keep their place.
        var claimed = [Int32](repeating: -1, count: Slots.count)
        for r in replies {
            var s = Server(endpoint: r.endpoint)
            s.lastAckTime = env.now
            s.assignedAt = env.now
            if r.isActive && r.shardID >= 0 && r.appliedVersion == truncateTo && !excludedShards.contains(r.shardID) {
                s.shard = r.shardID
                s.state = .active
                shardIDs.insert(r.shardID)
                nextShardID = max(nextShardID, r.shardID + 1)
            } else {
                s.preferredShard = excludedShards.contains(r.shardID) ? -1 : r.shardID
                nextShardID = max(nextShardID, r.shardID + 1)
            }
            servers[r.endpoint] = s
        }
        // Slot ownership: from the first active member of each shard; members that
        // disagree are re-synced.
        for shard in shardIDs.sorted() {
            let ms = replies.filter { $0.shardID == shard && servers[$0.endpoint]?.state == .active }
            guard let first = ms.first else { continue }
            for slot in first.ownedSlots.sorted where claimed[slot] < 0 { claimed[slot] = shard }
            for other in ms.dropFirst() where other.ownedSlots != first.ownedSlots {
                env.log(.warning, "coordinator: \(other.endpoint) disagrees on shard \(shard) slots; re-syncing")
                servers[other.endpoint]!.state = .unassigned
                servers[other.endpoint]!.preferredShard = shard
                servers[other.endpoint]!.shard = -1
            }
        }
        slotOwner = claimed
        // Servers that never replied go back through the join path.
        for r in reports.values where recoverReplies[r.endpoint] == nil {
            servers[r.endpoint] = nil
        }

        committed = truncateTo
        lastBatchVersion = truncateTo
        recoveryFloor = max(minNextVersion, truncateTo + 1)
        resolver.reset(rejectingReadsBefore: recoveryFloor)
        topologyEpoch = epoch << 32
        phase = .running
        reports = [:]
        recoverReplies = [:]
        excludedShards = []
        expectedShards = []
        env.log(.info, "coordinator: recovery complete at epoch \(epoch); \(shardIDs.count) shards, committed=\(committed), next version ≥ \(recoveryFloor)")

        var orphaned = SlotSet()
        if !shardIDs.isEmpty {
            for slot in 0..<Slots.count where slotOwner[slot] < 0 { orphaned.insert(slot) }
        }
        if !orphaned.isEmpty {
            env.log(.warning, "coordinator: \(orphaned.count) slots have no owner after recovery (lost shard); reassigning")
            assignOrphanedSlots(orphaned)
        }
        let late = lateJoins
        lateJoins = []
        for r in late { handleJoin(r, from: r.endpoint) }
        tryScheduleWork()
        emitBatch()
    }

    // MARK: Introspection

    public var topology: Topology {
        let shards = shardIDs.sorted().map { Topology.Shard(id: $0, replicas: activeMembers(of: $0)) }
        return Topology(epoch: topologyEpoch, slotOwner: slotOwner, shards: shards)
    }

    public var status: ClusterStatus {
        let phaseName: String
        switch phase {
        case .recovering: phaseName = "recovering"
        case .truncating: phaseName = "truncating"
        case .running: phaseName = shardIDs.isEmpty ? "waiting_for_storage" : "running"
        }
        let list = servers.keys.sorted().map { ep -> ClusterStatus.Server in
            let s = servers[ep]!
            let state: String
            switch s.state {
            case .unassigned: state = "unassigned"
            case .active: state = "active"
            case .catchingUp(let id): state = "catching_up(move \(id))"
            case .failed: state = "failed"
            }
            return ClusterStatus.Server(endpoint: ep, shardID: s.shard, state: state)
        }
        var moves: [String] = []
        if let m = move {
            switch m.kind {
            case .catchUp(let server, let shard): moves.append("move \(m.id): \(server) catching up into shard \(shard)")
            case .migrate(let slots, let from, let to): moves.append("move \(m.id): \(slots.count) slots shard \(from) -> \(to)\(m.transferring ? " (transferring)" : "")")
            }
        }
        return ClusterStatus(coordinatorEpoch: epoch, phase: phaseName, latestVersion: lastBatchVersion,
                             committedVersion: committed, replicationFactor: UInt32(config.replicationFactor),
                             topology: topology, servers: list, movesInProgress: moves,
                             commitsTotal: commitsTotal, conflictsTotal: conflictsTotal)
    }
}
