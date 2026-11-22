import SwiftCacheCore

/// A storage server: one replica of one shard.
///
/// It receives an ordered stream of batches from the coordinator, applies them to its
/// multi-version store and serves reads at any version inside the MVCC window. A read
/// at version `v` is only answered once the server has applied `v` *and* knows `v` is
/// committed cluster-wide, so a lagging or partitioned replica can never return stale
/// data — at worst it makes the client wait and try another replica.
public final class StorageServer: ServerNode {
    public let endpoint: Endpoint
    public let coordinator: Endpoint
    let config: ClusterConfig
    let env: any NodeEnvironment

    let store = VersionedStore()

    // Membership.
    private(set) var shardID: Int32 = -1
    private(set) var coordinatorEpoch: UInt64 = 0
    private(set) var isActive = false
    private var awaitingFirstBatch = false

    // Versions.
    private(set) var applied: Version = 0
    private(set) var committed: Version = 0
    private(set) var oldestReadable: Version = 0

    /// Slots this server serves reads for.
    private(set) var owned = SlotSet()
    /// Ownership before each change, for rolling back during recovery.
    private var ownershipLog: [(version: Version, before: SlotSet)] = []

    private var lastCoordinatorContact: UInt64 = 0
    private var lastGapReport: UInt64 = 0
    private var started = false

    // Data movement into this server.
    struct Receive {
        var moveID: UInt64
        var slots: SlotSet
        var sources: [Endpoint]
        var startVersion: Version
        /// Mutations for `slots` received before the snapshot was installed.
        var buffer: [(stamp: Versionstamp, mutation: Mutation)] = []
        var snapshotRid: UInt64 = 0
        var snapshotAttempt = 0
        var snapshotItems: [SnapshotChunk.Item] = []
        /// Set once the snapshot is installed: mutations at or below this version for
        /// `slots` were already included in the snapshot and must be skipped.
        var installedThrough: Version? = nil
        var isCatchUp: Bool
    }
    private var receives: [UInt64: Receive] = [:]

    // Reads waiting for a version to become readable.
    private struct Waiter {
        var id: UInt64
        var version: Version
        var needsCommitted: Bool
        var run: () -> Void
        var expire: () -> Void
    }
    private var waiters: [Waiter] = []
    private var nextWaiterID: UInt64 = 0
    private var nextRid: UInt64 = 1

    public init(endpoint: Endpoint, coordinator: Endpoint, config: ClusterConfig, env: any NodeEnvironment) {
        self.endpoint = endpoint
        self.coordinator = coordinator
        self.config = config
        self.env = env
    }

    // MARK: Lifecycle

    public func start() {
        guard !started else { return }
        started = true
        lastCoordinatorContact = 0
        sendJoin()
        scheduleHousekeeping()
    }

    private func scheduleHousekeeping() {
        env.schedule(after: config.joinInterval) { [weak self] in
            guard let self else { return }
            if self.env.now &- self.lastCoordinatorContact >= self.config.coordinatorTimeout {
                self.sendJoin()
            }
            self.scheduleHousekeeping()
        }
    }

    var report: StorageReport {
        StorageReport(endpoint: endpoint, shardID: shardID, coordinatorEpoch: coordinatorEpoch,
                      appliedVersion: applied, committedVersion: committed, isActive: isActive && !awaitingFirstBatch, ownedSlots: owned)
    }

    private func sendJoin() {
        env.send(.join(report), to: coordinator)
    }

    // MARK: Message dispatch

    public func receive(_ message: Message, from: Endpoint) {
        switch message {
        case .batch(let b):
            if from == coordinator { handleBatch(b) }
        case .assign(let a):
            if from == coordinator { handleAssign(a) }
        case .recover(let epoch, let truncateTo):
            if from == coordinator { handleRecover(epoch: epoch, truncateTo: truncateTo) }
        case .read(let rid, let key, let version):
            handleRead(rid: rid, key: key, version: version, from: from)
        case .readRange(let rid, let range, let version, let limit):
            handleRange(rid: rid, range: range, version: version, limit: Int(limit), from: from)
        case .fetchSnapshot(let rid, let slots, let minVersion):
            handleFetchSnapshot(rid: rid, slots: slots, minVersion: minVersion, from: from)
        case .snapshotChunk(let rid, let chunk):
            handleSnapshotChunk(rid: rid, chunk: chunk, from: from)
        case .getStorageStats(let rid):
            env.send(.storageStatsReply(rid: rid, stats), to: from)
        default:
            if let rid = message.rid { env.send(.errorReply(rid: rid, .invalidRequest), to: from) }
        }
    }

    public var stats: StorageStats {
        StorageStats(endpoint: endpoint, shardID: shardID, isActive: isActive, appliedVersion: applied,
                     committedVersion: committed, keyCount: UInt64(store.keyCount),
                     versionCount: UInt64(store.versionCount), approximateBytes: UInt64(max(0, store.approximateBytes)),
                     ownedSlots: UInt32(owned.count))
    }

    // MARK: Membership

    private func resetState() {
        store.removeAll()
        receives = [:]
        owned = SlotSet()
        ownershipLog = []
        applied = 0
        committed = 0
        oldestReadable = 0
        isActive = false
        let old = waiters
        waiters = []
        for w in old { w.expire() }
    }

    private func handleAssign(_ a: Assignment) {
        guard a.coordinatorEpoch >= coordinatorEpoch else { return }
        coordinatorEpoch = a.coordinatorEpoch
        lastCoordinatorContact = env.now
        resetState()
        shardID = a.shardID
        awaitingFirstBatch = true
        switch a.mode {
        case .activeEmpty:
            owned = a.slots
            isActive = true
        case .catchUp:
            isActive = false
        }
        env.log(.info, "\(endpoint): assigned to shard \(a.shardID) mode=\(a.mode) slots=\(a.slots.count)")
    }

    private func handleRecover(epoch: UInt64, truncateTo: Version) {
        guard epoch >= coordinatorEpoch else { return }
        coordinatorEpoch = epoch
        lastCoordinatorContact = env.now
        // Abandon in-flight data movement: drop anything we were receiving but don't own.
        for id in receives.keys.sorted() {
            let r = receives[id]!
            store.drop(slots: r.slots.subtracting(owned))
        }
        receives = [:]
        if isActive && !awaitingFirstBatch && applied >= truncateTo {
            store.truncate(after: truncateTo)
            while let last = ownershipLog.last, last.version > truncateTo {
                owned = last.before
                ownershipLog.removeLast()
            }
            applied = truncateTo
            committed = min(committed, truncateTo)
            env.log(.info, "\(endpoint): recovered, truncated to \(truncateTo)")
        } else {
            // We can't be made consistent at `truncateTo`; report as inactive so the
            // coordinator re-syncs us from a peer.
            let shard = shardID
            resetState()
            shardID = shard
            awaitingFirstBatch = false
            env.log(.info, "\(endpoint): recovery requires resync (applied=\(applied))")
        }
        env.send(.recoverReply(report), to: coordinator)
    }

    // MARK: Batches

    private func handleBatch(_ b: Batch) {
        guard b.coordinatorEpoch == coordinatorEpoch, shardID >= 0 else { return }
        lastCoordinatorContact = env.now
        if awaitingFirstBatch {
            applied = b.prevVersion
            awaitingFirstBatch = false
        }
        if b.version <= applied { return }  // duplicate
        guard b.prevVersion == applied else {
            // We missed batches (a connection broke). Drop this one and ask the
            // coordinator to resend from our applied version; it re-syncs us from a peer
            // only if it no longer has those batches.
            if env.now &- lastGapReport >= 100_000 {
                lastGapReport = env.now
                env.log(.info, "\(endpoint): batch gap (have \(applied), got prev \(b.prevVersion)); requesting resend")
                sendJoin()
            }
            return
        }

        for entry in b.entries {
            let stamp = Versionstamp(version: b.version, order: entry.order)
            for m in entry.mutations { applyMutation(m, stamp: stamp) }
        }
        for op in b.meta { applyMeta(op, version: b.version) }

        applied = b.version
        committed = max(committed, min(b.committedVersion, applied))
        if b.oldestVersion > oldestReadable {
            oldestReadable = min(b.oldestVersion, committed)
            store.collectGarbage(oldest: oldestReadable)
            ownershipLog.removeAll { $0.version < committed }
        }
        // Once our stream has passed a snapshot's version, nothing more needs skipping.
        for id in receives.keys.sorted() {
            guard let through = receives[id]?.installedThrough, through != 0, through <= applied else { continue }
            if receives[id]!.isCatchUp {
                receives[id] = nil
            } else {
                receives[id]!.installedThrough = 0  // keep the receive (marks "ready") until transfer
            }
        }
        env.send(.batchAck(version: b.version, coordinatorEpoch: coordinatorEpoch), to: coordinator)
        wakeWaiters()
    }

    private func receiveFor(slot: Int) -> UInt64? {
        for id in receives.keys.sorted() where receives[id]!.slots.contains(slot) { return id }
        return nil
    }

    private func applyMutation(_ m: Mutation, stamp: Versionstamp) {
        let slot = Slots.slot(for: m.key)
        if !receives.isEmpty, let id = receiveFor(slot: slot) {
            if let through = receives[id]!.installedThrough {
                if stamp.version > through { store.apply(m, stamp: stamp) }
            } else {
                receives[id]!.buffer.append((stamp, m))
            }
            return
        }
        store.apply(m, stamp: stamp)
    }

    private func changeOwnership(at version: Version, _ change: (inout SlotSet) -> Void) {
        ownershipLog.append((version, owned))
        change(&owned)
    }

    private func applyMeta(_ op: MetaOp, version: Version) {
        switch op {
        case .beginReceive(let moveID, let shard, let slots, let sources):
            guard shard == shardID else { return }
            var r = Receive(moveID: moveID, slots: slots, sources: sources, startVersion: version, isCatchUp: !isActive)
            r.snapshotAttempt = 0
            receives[moveID] = r
            env.log(.info, "\(endpoint): begin receive move \(moveID) slots=\(slots.count) catchUp=\(r.isCatchUp)")
            requestSnapshot(moveID)

        case .abortReceive(let moveID, _):
            guard let r = receives.removeValue(forKey: moveID) else { return }
            store.drop(slots: r.slots.subtracting(owned))
            env.log(.info, "\(endpoint): aborted receive move \(moveID)")

        case .transfer(let moveID, let slots, let from, let to):
            if from == shardID {
                changeOwnership(at: version) { $0.subtract(slots) }
                store.drop(slots: slots)
            }
            if to == shardID {
                if let r = receives.removeValue(forKey: moveID), r.installedThrough != nil {
                    changeOwnership(at: version) { $0.formUnion(slots) }
                } else {
                    // We were not ready, so our copy is incomplete. Resync.
                    env.log(.warning, "\(endpoint): transfer \(moveID) arrived before snapshot installed; resyncing")
                    let shard = shardID
                    resetState()
                    shardID = shard
                    sendJoin()
                }
            }

        case .gainEmpty(let shard, let slots):
            guard shard == shardID else { return }
            changeOwnership(at: version) { $0.formUnion(slots) }
        }
    }

    // MARK: Snapshots (data movement)

    private func requestSnapshot(_ moveID: UInt64) {
        guard var r = receives[moveID], !r.sources.isEmpty else { return }
        let source = r.sources[r.snapshotAttempt % r.sources.count]
        let rid = nextRid
        nextRid += 1
        r.snapshotRid = rid
        r.snapshotItems = []
        receives[moveID] = r
        env.send(.fetchSnapshot(rid: rid, slots: r.slots, minVersion: r.startVersion), to: source)
        env.schedule(after: config.snapshotTimeout) { [weak self] in
            guard let self, var r = self.receives[moveID], r.snapshotRid == rid, r.installedThrough == nil else { return }
            self.env.log(.warning, "\(self.endpoint): snapshot from \(source) timed out; retrying")
            r.snapshotAttempt += 1
            self.receives[moveID] = r
            self.requestSnapshot(moveID)
        }
    }

    private func handleSnapshotChunk(rid: UInt64, chunk: SnapshotChunk, from: Endpoint) {
        guard let id = receives.keys.sorted().first(where: { receives[$0]!.snapshotRid == rid }),
              var r = receives[id], r.installedThrough == nil else { return }
        if chunk.version == 0 {
            // Source refused (doesn't own the slots); try the next one shortly.
            r.snapshotAttempt += 1
            r.snapshotRid = 0
            receives[id] = r
            env.schedule(after: 50_000) { [weak self] in self?.requestSnapshot(id) }
            return
        }
        r.snapshotItems.append(contentsOf: chunk.items)
        guard chunk.done else { receives[id] = r; return }

        // Install the snapshot, then replay buffered mutations newer than it.
        for item in r.snapshotItems { store.install(item) }
        for (stamp, m) in r.buffer where stamp.version > chunk.version {
            store.apply(m, stamp: stamp)
        }
        r.buffer = []
        r.snapshotItems = []
        r.installedThrough = chunk.version
        env.log(.info, "\(endpoint): installed snapshot for move \(id) at \(chunk.version) (catchUp=\(r.isCatchUp))")
        if r.isCatchUp {
            receives[id] = nil
            if chunk.version > applied {
                // The snapshot is ahead of our batch stream; skip the overlap.
                receives[id] = Receive(moveID: id, slots: r.slots, sources: [], startVersion: r.startVersion,
                                       installedThrough: chunk.version, isCatchUp: true)
            }
            changeOwnership(at: applied) { $0.formUnion(r.slots) }
            isActive = true
            // Snapshot data may be older than our own stream; reads below this are safe.
        } else {
            receives[id] = r
        }
        env.send(.moveReady(moveID: id, coordinatorEpoch: coordinatorEpoch), to: coordinator)
        wakeWaiters()
    }

    private func handleFetchSnapshot(rid: UInt64, slots: SlotSet, minVersion: Version, from: Endpoint) {
        if applied < minVersion {
            wait(for: minVersion, needsCommitted: false, then: { [weak self] in
                self?.handleFetchSnapshot(rid: rid, slots: slots, minVersion: minVersion, from: from)
            }, expire: { [weak self] in
                self?.env.send(.snapshotChunk(rid: rid, SnapshotChunk(version: 0, items: [], done: true)), to: from)
            })
            return
        }
        guard isActive, slots.subtracting(owned).isEmpty else {
            env.send(.snapshotChunk(rid: rid, SnapshotChunk(version: 0, items: [], done: true)), to: from)
            return
        }
        let items = store.extract(slots: slots)
        let size = max(1, config.snapshotChunkKeys)
        var i = 0
        repeat {
            let end = min(items.count, i + size)
            env.send(.snapshotChunk(rid: rid, SnapshotChunk(version: applied, items: Array(items[i..<end]), done: end == items.count)), to: from)
            i = end
        } while i < items.count
    }

    // MARK: Reads

    /// Runs `run` once `version` is applied (and, if `needsCommitted`, known committed);
    /// runs `expire` instead if that takes longer than `readWaitTimeout`.
    private func wait(for version: Version, needsCommitted: Bool, then run: @escaping () -> Void, expire: @escaping () -> Void) {
        let id = nextWaiterID
        nextWaiterID += 1
        waiters.append(Waiter(id: id, version: version, needsCommitted: needsCommitted, run: run, expire: expire))
        env.schedule(after: config.readWaitTimeout) { [weak self] in
            guard let self, let idx = self.waiters.firstIndex(where: { $0.id == id }) else { return }
            let w = self.waiters.remove(at: idx)
            w.expire()
        }
    }

    private func isReady(_ w: Waiter) -> Bool {
        w.version <= applied && (!w.needsCommitted || w.version <= committed)
    }

    private func wakeWaiters() {
        guard !waiters.isEmpty else { return }
        let ready = waiters.filter(isReady)
        guard !ready.isEmpty else { return }
        waiters.removeAll(where: isReady)
        for w in ready { w.run() }
    }

    private enum ReadCheck { case ok, error(ErrorCode), deferred }

    /// Common checks for reads: version bounds, membership, and waiting for the version.
    private func checkRead(version: Version, retry: @escaping () -> Void, onExpire: @escaping () -> Void) -> ReadCheck {
        if shardID < 0 || awaitingFirstBatch { return .error(.wrongShard) }
        if version < oldestReadable { return .error(.transactionTooOld) }
        if version > applied || version > committed {
            wait(for: version, needsCommitted: true, then: retry, expire: onExpire)
            return .deferred
        }
        return .ok
    }

    private func handleRead(rid: UInt64, key: Bytes, version: Version, from: Endpoint) {
        let check = checkRead(version: version, retry: { [weak self] in
            self?.handleRead(rid: rid, key: key, version: version, from: from)
        }, onExpire: { [weak self] in
            self?.env.send(.errorReply(rid: rid, .futureVersion), to: from)
        })
        switch check {
        case .deferred: return
        case .error(let e): env.send(.errorReply(rid: rid, e), to: from); return
        case .ok: break
        }
        guard owned.contains(Slots.slot(for: key)) else {
            env.send(.errorReply(rid: rid, .wrongShard), to: from)
            return
        }
        if let v = store.get(key, at: version), let value = v.visibleValue(at: version) {
            env.send(.readReply(rid: rid, value: value, stamp: v.stamp), to: from)
        } else {
            env.send(.readReply(rid: rid, value: nil, stamp: .zero), to: from)
        }
    }

    private func handleRange(rid: UInt64, range: KeyRange, version: Version, limit: Int, from: Endpoint) {
        let check = checkRead(version: version, retry: { [weak self] in
            self?.handleRange(rid: rid, range: range, version: version, limit: limit, from: from)
        }, onExpire: { [weak self] in
            self?.env.send(.errorReply(rid: rid, .futureVersion), to: from)
        })
        switch check {
        case .deferred: return
        case .error(let e): env.send(.errorReply(rid: rid, e), to: from); return
        case .ok: break
        }
        let owned = self.owned
        let result = store.range(range, at: version, limit: max(1, limit)) { owned.contains(Slots.slot(for: $0)) }
        env.send(.rangeReply(rid: rid, entries: result.entries, more: result.more, servedSlots: owned), to: from)
    }
}
