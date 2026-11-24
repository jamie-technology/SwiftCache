import SwiftCacheCore

/// A storage server: one replica of one shard (or idle, waiting to be given one).
///
/// It *pulls* its tag's mutations from the transaction logs (`peek`), applies them to its
/// multi-version store, and releases them (`pop`). Logs only hand out versions known to be
/// committed on every log, so everything a storage server applies is durable; reads at
/// version `v` are served once `v` has been applied. A lagging or partitioned replica can
/// never return stale data — at worst it makes the client wait and try another replica.
///
/// Membership changes arrive as metadata ops in its tag's stream, at exact versions.
public final class StorageServer: Role {
    public let context: RoleContext
    let config: ClusterConfig
    let store = VersionedStore()

    public private(set) var tag: Tag = 0
    public private(set) var shardID: Int32 = -1
    /// False while copying the shard's data after joining it.
    public private(set) var isActive = false
    public private(set) var applied: Version = 0
    private(set) var oldestReadable: Version = 0
    public private(set) var owned = SlotSet()

    private var dbInfo = ServerDBInfo.empty
    private var peekInFlight = false
    /// Identifies the current peek; replies to superseded peeks don't drive the loop.
    private var peekSeq: UInt64 = 0
    /// The version the current peek asks the log to wait for (0 = data only).
    private var peekNeed: Version = 0
    /// The log the in-flight peek went to.
    private var peekTarget: Endpoint?
    /// The current subscription's request id and tag (0 = none), and its last reply.
    private var streamRID: UInt64 = 0
    private var streamTag: Tag = 0
    private var lastStreamAt: UInt64 = 0
    private var peekCursor = 0
    private var lastPopped: Version = 0
    private var tagRetired = Set<Tag>()
    /// When this server last stopped being (or started out not being) a shard member.
    private var idleSince: UInt64 = 0

    struct Receive {
        var moveID: UInt64
        var slots: SlotSet
        var sources: [Endpoint]
        var startVersion: Version
        var buffer: [(stamp: Versionstamp, mutation: Mutation)] = []
        var snapshotRid: UInt64 = 0
        var snapshotAttempt = 0
        var snapshotItems: [SnapshotChunk.Item] = []
        /// Set once the snapshot is installed: mutations at or below this version for
        /// `slots` were already in it and must be skipped (0 once our stream has passed it).
        var installedThrough: Version?
        var isCatchUp: Bool
        /// The controller has acknowledged readiness (replicaReady / transfer seen).
        var acknowledged = false
    }
    private var receives: [UInt64: Receive] = [:]

    private struct Waiter {
        var id: UInt64
        var version: Version
        var run: () -> Void
        var expire: () -> Void
    }
    private var waiters: [Waiter] = []
    private var nextWaiterID: UInt64 = 0
    private var nextRid: UInt64 = 1
    private var snapshotRequests: [UInt64: UInt64] = [:]  // our rid -> moveID

    init(context: RoleContext, config: ClusterConfig) {
        self.context = context
        self.config = config
    }

    /// Safe to take another replica down (rolling restarts wait for this): an active,
    /// caught-up shard member, or a spare that has been idle long enough that the cluster
    /// clearly doesn't need it.
    public var isReadyForRollout: Bool {
        if tag != 0 { return isActive && receives.values.allSatisfy { !$0.isCatchUp } }
        return context.now &- idleSince > 30_000_000
    }

    public func start() {
        idleSince = context.now
        context.every(config.popInterval) { [weak self] in self?.housekeeping() }
    }

    /// Every visible key/value in owned slots at `version` (for consistency checks).
    public func debugContents(at version: Version) -> [Bytes: Bytes] {
        var out: [Bytes: Bytes] = [:]
        for (k, versions) in store.data where owned.contains(Slots.slot(for: k.bytes)) {
            let key = k.bytes
            var v: VersionedValue?
            for x in versions where x.stamp.version <= version { v = x }
            if let value = v?.visibleValue(at: version) { out[key] = value }
        }
        return out
    }

    /// The stored versions of one key (debugging).
    public func debugVersions(_ key: Bytes) -> [Versionstamp] {
        store.data[ByteKey(key)]?.map(\.stamp) ?? []
    }

    var report: StorageReport {
        StorageReport(endpoint: context.endpoint, tag: tag, appliedVersion: applied)
    }

    public var stats: StorageStats {
        StorageStats(endpoint: context.endpoint, tag: tag, shardID: shardID, isActive: isActive, appliedVersion: applied,
                     keyCount: UInt64(store.keyCount), versionCount: UInt64(store.versionCount),
                     approximateBytes: UInt64(max(0, store.approximateBytes)), ownedSlots: UInt32(owned.count))
    }

    // MARK: Cluster information

    func updateDBInfo(_ info: ServerDBInfo) {
        let changed = info.tlogs != dbInfo.tlogs
        dbInfo = info
        if changed { peekCursor = Int(tag) }
        pullIfNeeded()
    }

    /// The controller tells us our tag once it has committed our membership.
    func assignTag(_ assigned: Tag) {
        guard assigned != 0, tag == 0, !tagRetired.contains(assigned) else { return }
        tag = assigned
        peekCursor = Int(assigned)  // spread storage servers across the logs
        applied = 0
        context.log(.info, "assigned tag \(assigned)")
        pullIfNeeded()
    }

    private func wipe(_ reason: String) {
        context.log(.warning, "wiping (\(reason)); waiting for a new assignment")
        if tag != 0 { tagRetired.insert(tag) }
        tag = 0
        idleSince = context.now
        shardID = -1
        isActive = false
        applied = 0
        lastPopped = 0
        oldestReadable = 0
        owned = SlotSet()
        receives = [:]
        store.removeAll()
        let old = waiters
        waiters = []
        for w in old { w.expire() }
    }

    // MARK: Pulling from the logs

    /// The highest version a waiting read (or snapshot) needs.
    private var neededVersion: Version {
        waiters.map(\.version).max() ?? 0
    }

    /// A read needs `version` before our tag's next data may arrive: ask the log to answer
    /// as soon as it's committed (superseding a data-only peek).
    private func needVersion(_ version: Version) {
        guard version > applied, version > peekNeed || !peekInFlight else { return }
        if peekInFlight, let target = peekTarget {
            // Tell the log holding our peek, rather than superseding it (one message
            // instead of a cancelled reply plus a new round trip). If the peek was already
            // answered, the next one is built from `neededVersion`.
            peekNeed = version
            context.send(.tlogNeed(tag: tag, need: version), to: target)
            return
        }
        peekInFlight = false
        pullIfNeeded()
    }

    /// Subscribes to our tag on one log. The log streams replies (same rid) as committed
    /// data arrives, plus a heartbeat at least every `peekTimeout`, until we resubscribe,
    /// unsubscribe, or it stops (locked by a recovery). One message per delivery instead
    /// of a reply and a new peek.
    private func pullIfNeeded() {
        guard tag != 0, !peekInFlight, !dbInfo.tlogs.isEmpty else { return }
        peekInFlight = true
        peekSeq += 1
        let seq = peekSeq
        let tlog = dbInfo.tlogs[peekCursor % dbInfo.tlogs.count]
        peekTarget = tlog
        let myTag = tag
        let after = applied
        let need = neededVersion > applied ? neededVersion : 0
        peekNeed = need
        var rid: UInt64 = 0
        context.request(to: tlog, timeout: config.peekTimeout + config.requestTimeout, {
            rid = $0
            return .tlogPeek(rid: $0, tag: myTag, from: after, upTo: 0, need: need)
        }) { [weak self] reply in
            guard let self, seq == self.peekSeq else { return }  // replaced by a newer subscription
            guard let reply else { return self.streamFailed() }
            self.streamReply(reply, tag: myTag)
        }
        streamRID = rid
        streamTag = myTag
        lastStreamAt = context.now
    }

    /// A reply of the current subscription (the first arrives through the request, the
    /// rest through `receive`).
    private func streamReply(_ reply: Message, tag myTag: Tag) {
        lastStreamAt = context.now
        guard tag == myTag else {
            endStream()
            return pullIfNeeded()
        }
        guard case .tlogPeekReply(_, let p) = reply else { return streamFailed() }
        switch p.status {
        case .ok:
            // Apply only a contiguous continuation of what we have: if a reply was lost
            // (broken connection), resubscribe from where we are.
            guard p.begin <= applied else {
                endStream()
                return pullIfNeeded()
            }
            applyPeek(p)
        case .tooOld:
            endStream()
            wipe("log no longer has versions after \(p.begin) for tag \(myTag)")
        case .tagRemoved:
            endStream()
            wipe("tag \(myTag) was retired")
        case .stopped:
            // That log was locked by a recovery; wait for the new generation's logs.
            endStream()
            peekCursor += 1
            context.schedule(after: 100_000) { [weak self] in self?.pullIfNeeded() }
        }
    }

    /// Ends the current subscription (telling the log, in case it's still streaming).
    private func endStream() {
        if peekInFlight, streamRID != 0, let t = peekTarget {
            context.send(.tlogUnsubscribe(rid: streamRID, tag: streamTag), to: t)
        }
        peekInFlight = false
        streamRID = 0
        peekSeq += 1  // a pending first reply is now stale
    }

    /// The log stopped answering: try the next one.
    private func streamFailed() {
        endStream()
        peekCursor += 1
        context.schedule(after: 20_000) { [weak self] in self?.pullIfNeeded() }
    }

    private func applyPeek(_ p: PeekReply) {
        for e in p.entries where e.version > applied {
            for (order, m) in e.mutations {
                applyMutation(m, stamp: Versionstamp(version: e.version, order: order))
            }
            for op in e.meta { applyMeta(op, version: e.version) }
            if tag == 0 { return }  // removed from the shard by this entry
        }
        if p.end > applied { applied = p.end }
        // Once our stream passes a snapshot's version, nothing more needs skipping.
        for id in receives.keys.sorted() {
            guard let through = receives[id]?.installedThrough, through != 0, through <= applied else { continue }
            receives[id]!.installedThrough = 0
            if receives[id]!.isCatchUp && receives[id]!.acknowledged { receives[id] = nil }
        }
        if applied > config.mvccWindow, applied - config.mvccWindow > oldestReadable {
            oldestReadable = applied - config.mvccWindow
            store.collectGarbage(oldest: oldestReadable)
        }
        wakeWaiters()
    }

    private func housekeeping() {
        // A live subscription heartbeats every peekTimeout: silence means it's gone.
        if peekInFlight, streamRID != 0, context.now &- lastStreamAt > config.peekTimeout + config.requestTimeout {
            streamFailed()
        }
        // Release applied data on every log.
        if tag != 0, applied > lastPopped {
            lastPopped = applied
            for t in dbInfo.tlogs { context.send(.tlogPop(tag: tag, version: applied), to: t) }
        }
        // Keep telling the controller about finished copies until it acts on them.
        for id in receives.keys.sorted() {
            let r = receives[id]!
            if r.installedThrough != nil && !r.acknowledged && !dbInfo.clusterController.isEmpty {
                context.send(.moveReady(moveID: id, tag: tag), to: dbInfo.clusterController)
            }
        }
        pullIfNeeded()
    }

    private func receiveFor(slot: Int) -> UInt64? {
        for id in receives.keys.sorted() where receives[id]!.slots.contains(slot) { return id }
        return nil
    }

    private func applyMutation(_ m: Mutation, stamp: Versionstamp) {
        if !receives.isEmpty, let id = receiveFor(slot: Slots.slot(for: m.key)) {
            if let through = receives[id]!.installedThrough {
                if stamp.version > through { store.apply(m, stamp: stamp) }
            } else {
                receives[id]!.buffer.append((stamp, m))
            }
            return
        }
        store.apply(m, stamp: stamp)
    }

    private func applyMeta(_ op: MetaOp, version: Version) {
        switch op {
        case .createShard(let id, let members, let slots):
            guard members.contains(where: { $0.tag == tag }) else { return }
            shardID = id
            owned = slots
            isActive = true
            context.log(.info, "member of new shard \(id) with \(slots.count) slots")

        case .addReplica(let id, let member, let moveID, let sources, let slots):
            guard member.tag == tag else { return }
            shardID = id
            isActive = false
            receives[moveID] = Receive(moveID: moveID, slots: slots, sources: sources, startVersion: version, isCatchUp: true)
            context.log(.info, "joining shard \(id): copying \(slots.count) slots from \(sources.count) peers")
            requestSnapshot(moveID)

        case .replicaReady(_, let t):
            guard t == tag else { return }
            for id in receives.keys.sorted() where receives[id]!.isCatchUp {
                receives[id]!.acknowledged = true
                if receives[id]!.installedThrough == 0 { receives[id] = nil }
            }

        case .removeReplica(_, let t):
            if t == tag { wipe("removed from shard \(shardID)") }

        case .beginMigration(let moveID, let slots, _, let to, let sources):
            guard to == shardID else { return }
            receives[moveID] = Receive(moveID: moveID, slots: slots, sources: sources, startVersion: version, isCatchUp: false)
            context.log(.info, "receiving \(slots.count) slots (move \(moveID))")
            requestSnapshot(moveID)

        case .transfer(let moveID, let slots, let from, let to):
            if from == shardID {
                owned.subtract(slots)
                store.drop(slots: slots)
            }
            if to == shardID {
                if let r = receives.removeValue(forKey: moveID), r.installedThrough != nil {
                    owned.formUnion(slots)
                } else {
                    // We weren't ready (the controller waits for us, so we must have lost
                    // our copy): retire this tag and let the controller re-replicate.
                    wipe("transfer \(moveID) arrived before our copy was installed")
                }
            }

        case .abortMigration(let moveID, _):
            if let r = receives.removeValue(forKey: moveID) {
                store.drop(slots: r.slots.subtracting(owned))
            }

        case .reassignSlots(let slots, let to):
            if to == shardID { owned.formUnion(slots) }

        case .removeShard, .snapshot:
            break
        }
    }

    // MARK: Snapshots (data movement)

    private func requestSnapshot(_ moveID: UInt64) {
        guard var r = receives[moveID], !r.sources.isEmpty, r.installedThrough == nil else { return }
        let source = r.sources[r.snapshotAttempt % r.sources.count]
        let rid = nextRid
        nextRid += 1
        snapshotRequests[rid] = moveID
        r.snapshotRid = rid
        r.snapshotItems = []
        receives[moveID] = r
        context.send(.fetchSnapshot(rid: rid, slots: r.slots, minVersion: r.startVersion), to: source)
        context.schedule(after: config.snapshotTimeout) { [weak self] in
            guard let self, var r = self.receives[moveID], r.snapshotRid == rid, r.installedThrough == nil else { return }
            self.context.log(.warning, "snapshot from \(source) timed out; retrying")
            r.snapshotAttempt += 1
            self.receives[moveID] = r
            self.requestSnapshot(moveID)
        }
    }

    private func handleSnapshotChunk(rid: UInt64, chunk: SnapshotChunk) {
        guard let id = snapshotRequests[rid], var r = receives[id], r.snapshotRid == rid, r.installedThrough == nil else { return }
        if chunk.version == 0 {
            // The source refused (not an active owner); try the next one shortly.
            snapshotRequests[rid] = nil
            r.snapshotAttempt += 1
            r.snapshotRid = 0
            receives[id] = r
            context.schedule(after: 50_000) { [weak self] in self?.requestSnapshot(id) }
            return
        }
        r.snapshotItems.append(contentsOf: chunk.items)
        guard chunk.done else { receives[id] = r; return }
        snapshotRequests[rid] = nil

        for item in r.snapshotItems { store.install(item) }
        for (stamp, m) in r.buffer where stamp.version > chunk.version { store.apply(m, stamp: stamp) }
        r.buffer = []
        r.snapshotItems = []
        r.installedThrough = chunk.version <= applied ? 0 : chunk.version
        receives[id] = r
        context.log(.info, "installed snapshot for move \(id) at \(chunk.version)")
        if r.isCatchUp {
            owned.formUnion(r.slots)
            isActive = true
        }
        if !dbInfo.clusterController.isEmpty {
            context.send(.moveReady(moveID: id, tag: tag), to: dbInfo.clusterController)
        }
        wakeWaiters()
    }

    private func handleFetchSnapshot(rid: UInt64, slots: SlotSet, minVersion: Version, from: Endpoint) {
        if applied < minVersion {
            wait(for: minVersion, then: { [weak self] in
                self?.handleFetchSnapshot(rid: rid, slots: slots, minVersion: minVersion, from: from)
            }, expire: { [weak self] in
                self?.context.send(.snapshotChunk(rid: rid, SnapshotChunk(version: 0, items: [], done: true)), to: from)
            })
            return
        }
        guard isActive, tag != 0, slots.subtracting(owned).isEmpty else {
            context.send(.snapshotChunk(rid: rid, SnapshotChunk(version: 0, items: [], done: true)), to: from)
            return
        }
        let items = store.extract(slots: slots)
        let size = max(1, config.snapshotChunkKeys)
        var i = 0
        repeat {
            let end = min(items.count, i + size)
            context.send(.snapshotChunk(rid: rid, SnapshotChunk(version: applied, items: Array(items[i..<end]), done: end == items.count)), to: from)
            i = end
        } while i < items.count
    }

    // MARK: Messages

    public func receive(_ message: Message, from: Endpoint) {
        switch message {
        case .tlogPeekReply(let rid, _):
            if rid != 0, rid == streamRID, from == peekTarget {
                streamReply(message, tag: streamTag)
            } else {
                // A stream we no longer want (we resubscribed elsewhere): stop it.
                context.send(.tlogUnsubscribe(rid: rid, tag: streamTag), to: from)
            }
        case .errorReply(let rid, _) where rid != 0 && rid == streamRID && from == peekTarget:
            streamFailed()
        case .read(let rid, let key, let version):
            handleRead(rid: rid, key: key, version: version, from: from)
        case .readBatch(let rid, let items):
            handleReadBatch(rid: rid, items: items, from: from)
        case .readRange(let rid, let range, let version, let limit):
            handleRange(rid: rid, range: range, version: version, limit: Int(limit), from: from)
        case .fetchSnapshot(let rid, let slots, let minVersion):
            handleFetchSnapshot(rid: rid, slots: slots, minVersion: minVersion, from: from)
        case .snapshotChunk(let rid, let chunk):
            handleSnapshotChunk(rid: rid, chunk: chunk)
        case .getStorageStats(let rid):
            context.send(.storageStatsReply(rid: rid, stats), to: from)
        default:
            if let rid = message.rid, !message.isReply { context.send(.errorReply(rid: rid, .invalidRequest), to: from) }
        }
    }

    // MARK: Reads

    private func wait(for version: Version, then run: @escaping () -> Void, expire: @escaping () -> Void) {
        let id = nextWaiterID
        nextWaiterID += 1
        waiters.append(Waiter(id: id, version: version, run: run, expire: expire))
        needVersion(version)
        context.schedule(after: config.readWaitTimeout) { [weak self] in
            guard let self, let idx = self.waiters.firstIndex(where: { $0.id == id }) else { return }
            let w = self.waiters.remove(at: idx)
            w.expire()
        }
    }

    private func wakeWaiters() {
        guard !waiters.isEmpty else { return }
        let ready = waiters.filter { $0.version <= applied }
        guard !ready.isEmpty else { return }
        waiters.removeAll { $0.version <= applied }
        for w in ready { w.run() }
    }

    private enum ReadCheck { case ok, error(ErrorCode), deferred }

    private func checkRead(version: Version, retry: @escaping () -> Void, onExpire: @escaping () -> Void) -> ReadCheck {
        if tag == 0 || shardID < 0 { return .error(.wrongShard) }
        if version < oldestReadable { return .error(.transactionTooOld) }
        if version > applied {
            wait(for: version, then: retry, expire: onExpire)
            return .deferred
        }
        return .ok
    }

    private func handleRead(rid: UInt64, key: Bytes, version: Version, from: Endpoint) {
        let check = checkRead(version: version, retry: { [weak self] in
            self?.handleRead(rid: rid, key: key, version: version, from: from)
        }, onExpire: { [weak self] in
            self?.context.send(.errorReply(rid: rid, .futureVersion), to: from)
        })
        switch check {
        case .deferred: return
        case .error(let e): context.send(.errorReply(rid: rid, e), to: from); return
        case .ok: break
        }
        guard owned.contains(Slots.slot(for: key)) else {
            context.send(.errorReply(rid: rid, .wrongShard), to: from)
            return
        }
        if let v = store.get(key, at: version), let value = v.visibleValue(at: version) {
            context.send(.readReply(rid: rid, value: value, stamp: v.stamp), to: from)
        } else {
            context.send(.readReply(rid: rid, value: nil, stamp: .zero), to: from)
        }
    }

    /// Answers many reads at once: waits (once) for the highest version among them, then
    /// evaluates each exactly like a single read.
    private func handleReadBatch(rid: UInt64, items: [ReadItem], from: Endpoint) {
        let maxVersion = items.map(\.version).max() ?? 0  // "latest" items (0) never wait
        if tag != 0, shardID >= 0, maxVersion > applied {
            wait(for: maxVersion, then: { [weak self] in
                self?.handleReadBatch(rid: rid, items: items, from: from)
            }, expire: { [weak self] in
                // Serve what we can; the rest fail with future_version (retried by clients).
                self?.answerReadBatch(rid: rid, items: items, from: from)
            })
            return
        }
        answerReadBatch(rid: rid, items: items, from: from)
    }

    private func answerReadBatch(rid: UInt64, items: [ReadItem], from: Endpoint) {
        let results = items.map { item -> ReadResult in
            if tag == 0 || shardID < 0 { return .error(.wrongShard) }
            if item.version == 0 {
                // Latest applied: everything applied is committed (logs only hand out
                // committed versions), so this is a consistent, committed state.
                guard owned.contains(Slots.slot(for: item.key)), applied > 0 else { return .error(.wrongShard) }
                if let v = store.get(item.key, at: applied), let value = v.visibleValue(at: applied) {
                    return .valueAt(value, v.stamp, version: applied)
                }
                return .valueAt(nil, .zero, version: applied)
            }
            if item.version < oldestReadable { return .error(.transactionTooOld) }
            if item.version > applied { return .error(.futureVersion) }
            guard owned.contains(Slots.slot(for: item.key)) else { return .error(.wrongShard) }
            if let v = store.get(item.key, at: item.version), let value = v.visibleValue(at: item.version) {
                return .value(value, v.stamp)
            }
            return .value(nil, .zero)
        }
        context.send(.readBatchReply(rid: rid, results: results), to: from)
    }

    private func handleRange(rid: UInt64, range: KeyRange, version: Version, limit: Int, from: Endpoint) {
        let check = checkRead(version: version, retry: { [weak self] in
            self?.handleRange(rid: rid, range: range, version: version, limit: limit, from: from)
        }, onExpire: { [weak self] in
            self?.context.send(.errorReply(rid: rid, .futureVersion), to: from)
        })
        switch check {
        case .deferred: return
        case .error(let e): context.send(.errorReply(rid: rid, e), to: from); return
        case .ok: break
        }
        let owned = self.owned
        let system = SystemKeys.isSystem(range.begin)
        let result = store.range(range, at: version, limit: max(1, limit)) {
            owned.contains(Slots.slot(for: $0)) && SystemKeys.isSystem($0) == system
        }
        context.send(.rangeReply(rid: rid, entries: result.entries, more: result.more, servedSlots: owned), to: from)
    }
}
