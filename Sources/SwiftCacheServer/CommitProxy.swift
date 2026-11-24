import SwiftCacheCore

/// A commit proxy of one generation. Clients send read-version and commit requests to
/// any proxy of the current generation.
///
/// Commit path, per batch:
/// 1. get a commit version (and the previous one) from the master;
/// 2. send the batch's conflict ranges to the resolver, which decides every transaction
///    and returns metadata committed by other proxies since this proxy's last batch;
/// 3. tag each committed mutation with the storage tags that own its slot (and record
///    idempotency ids), then push the version to *every* log of the generation;
/// 4. once all logs acknowledge, report the version to the master and only then reply to
///    clients — so any read version handed out afterwards includes the commit.
///
/// Read versions: the proxy asks the master for the committed version *and* confirms
/// with every log that the generation is still unlocked; a proxy of a superseded
/// generation therefore can never hand out a stale read version.
public final class CommitProxy: Role {
    public let context: RoleContext
    let config: ClusterConfig
    let spec: ProxySpec

    private var map: ShardMap
    private var knownCommitted: Version
    private var requestNum: UInt64 = 0
    public private(set) var dead = false
    public var generation: UInt64 { spec.generation }

    struct Pending {
        var client: Endpoint
        var rid: UInt64
        var request: CommitRequest
        /// Set when the commit arrived in a `commitBatch`: the reply is collective.
        var group: CommitGroup? = nil
        var index = 0
        var arrived: UInt64 = 0
    }

    /// The commits of one `commitBatch`, answered together once all are decided.
    final class CommitGroup {
        let client: Endpoint
        let rid: UInt64
        var results: [CommitResult?]
        var remaining: Int
        init(client: Endpoint, rid: UInt64, count: Int) {
            self.client = client; self.rid = rid
            results = Array(repeating: nil, count: count); remaining = count
        }
    }

    private func reply(_ p: Pending, _ result: CommitResult) {
        guard let g = p.group else {
            context.send(.commitReply(rid: p.rid, result), to: p.client)
            return
        }
        if g.results[p.index] == nil { g.remaining -= 1 }
        g.results[p.index] = result
        if g.remaining == 0 { context.send(.commitBatchReply(rid: g.rid, g.results.map { $0! }), to: g.client) }
    }
    private var pending: [Pending] = []
    private var metaPending: [Pending] = []

    final class BatchState {
        let num: UInt64
        let txns: [Pending]
        let meta: [Pending]
        var version: Version = 0
        var prevVersion: Version = 0
        var resolution: ResolveReply?
        var finalized = false
        /// Declared committed by the master (no more re-pushes).
        var committed = false
        var push: TLogPush?
        var pushedAt: UInt64 = 0
        var startedAt: UInt64 = 0
        var resolvedAt: UInt64 = 0
        var replies: [(pending: Pending, result: CommitResult)] = []
        var hasData = false
        init(num: UInt64, txns: [Pending], meta: [Pending]) { self.num = num; self.txns = txns; self.meta = meta }
    }
    private var batches: [BatchState] = []
    private var lastBatchAt: UInt64 = 0
    private var snapshotWritten = false

    private var grvQueue: [(client: Endpoint, rid: UInt64)] = []
    /// Read-version rounds in flight (each confirms liveness independently).
    private var grvRounds = 0
    private let maxGRVRounds = 1
    private var mapWaiters: [(client: Endpoint, rid: UInt64, minVersion: Version)] = []

    init(context: RoleContext, config: ClusterConfig, spec: ProxySpec) {
        self.context = context
        self.config = config
        self.spec = spec
        self.map = spec.shardMap
        self.knownCommitted = spec.startVersion
    }

    private var statBatches = 0
    private var statTxns = 0
    /// Per-batch phase times (µs), summed and max, for the stats line.
    private var statWait = (sum: UInt64(0), max: UInt64(0))
    private var statResolve = (sum: UInt64(0), max: UInt64(0))
    private var statDurable = (sum: UInt64(0), max: UInt64(0))
    private var statDone = 0

    public func start() {
        context.ready()
        context.every(config.batchInterval) { [weak self] in self?.tick() }
        context.every(config.requestTimeout / 4) { [weak self] in self?.repushStale() }
        context.every(5_000_000) { [weak self] in
            guard let self, self.statBatches > 0 else { return }
            let n = UInt64(max(1, self.statDone)), t = UInt64(max(1, self.statTxns))
            self.context.log(.info, "stats: \(self.statBatches) batches, \(self.statTxns) txns (\(self.statTxns / max(1, self.statBatches)) per batch); "
                + "µs avg/max: queued \(self.statWait.sum / t)/\(self.statWait.max), resolve \(self.statResolve.sum / n)/\(self.statResolve.max), "
                + "logs+master \(self.statDurable.sum / n)/\(self.statDurable.max)")
            self.statBatches = 0
            self.statTxns = 0
            self.statDone = 0
            self.statWait = (0, 0); self.statResolve = (0, 0); self.statDurable = (0, 0)
        }
    }

    private func generationEnded(_ why: String) {
        guard !dead else { return }
        dead = true
        context.log(.info, "generation \(spec.generation) ended (\(why)); stopping")
        for g in grvQueue { context.send(.errorReply(rid: g.rid, .staleGeneration), to: g.client) }
        context.stopSelf()
    }

    /// A batch whose version/resolution hasn't come back yet.
    private var resolving: Bool { batches.contains { !$0.finalized } }

    /// Continuous pipelining: start the next batch as soon as nothing is awaiting
    /// resolution, taking everything that arrived meanwhile (batch size adapts to load,
    /// and no commit waits for a timer).
    private func pump() {
        guard !dead, !resolving, batches.count < config.maxInflightBatches,
              !pending.isEmpty || !metaPending.isEmpty else { return }
        startBatch()
    }

    private func tick() {
        guard !dead else { return }
        let idle = context.now &- lastBatchAt >= config.idleBatchInterval
        let needSnapshot = spec.writeSnapshot && !snapshotWritten
        // (Logs learn the committed version from the master's `tlogCommitted`, so no batch
        // is needed just to carry it.)
        if batches.count < config.maxInflightBatches && !resolving
            && (!pending.isEmpty || !metaPending.isEmpty || idle || needSnapshot) {
            startBatch()
        }
        if !grvQueue.isEmpty && grvRounds < maxGRVRounds { startGRV() }
    }

    // MARK: Requests

    public func receive(_ message: Message, from: Endpoint) {
        guard !dead else {
            if let rid = message.rid, !message.isReply { context.send(.errorReply(rid: rid, .staleGeneration), to: from) }
            return
        }
        switch message {
        case .getReadVersion(let rid):
            grvQueue.append((from, rid))
            if grvRounds < maxGRVRounds { startGRV() }
        case .commit(let rid, let req):
            enqueue(Pending(client: from, rid: rid, request: req))
        case .commitBatch(let rid, let reqs):
            let group = CommitGroup(client: from, rid: rid, count: reqs.count)
            for (i, req) in reqs.enumerated() {
                enqueue(Pending(client: from, rid: rid, request: req, group: group, index: i), startIfIdle: false)
            }
            pump()
            if reqs.isEmpty { context.send(.commitBatchReply(rid: rid, []), to: from) }
        case .committedVersion(let gen, let version):
            guard gen == spec.generation, from == spec.master else { return }
            if version > knownCommitted { knownCommitted = version }
            completeCommitted()
        case .tlogPushReply(_, false) where spec.tlogs.contains(from), .errorReply where spec.tlogs.contains(from):
            // A log is locked (a newer generation is recovering) or gone. Whether pending
            // batches survive is decided by that recovery; clients see an unknown result.
            generationEnded("log \(from) refused a push")
        case .getShardMap(let rid, let minVersion):
            if map.version >= minVersion || lastResolvedVersion >= minVersion {
                context.send(.shardMapReply(rid: rid, map), to: from)
            } else {
                mapWaiters.append((from, rid, minVersion))
            }
        default:
            if let rid = message.rid, !message.isReply { context.send(.errorReply(rid: rid, .invalidRequest), to: from) }
        }
    }

    private func enqueue(_ p: Pending, startIfIdle: Bool = true) {
        var p = p
        p.arrived = context.now
        if let error = validate(p.request, from: p.client) {
            reply(p, .failed(error))
            return
        }
        if p.request.metaOps.isEmpty { pending.append(p) } else { metaPending.append(p) }
        // Lightly loaded: don't wait for the batching tick.
        if startIfIdle { pump() }
    }

    private func validate(_ req: CommitRequest, from: Endpoint) -> ErrorCode? {
        if !req.metaOps.isEmpty && from != spec.clusterController { return .invalidRequest }
        var size = 0
        for m in req.mutations {
            if SystemKeys.isSystem(m.key) { return .invalidRequest }
            if m.key.count > config.maxKeySize { return .tooLarge }
            if case .set(_, let v, _) = m, v.count > config.maxValueSize { return .tooLarge }
            size += m.approximateSize
        }
        if size > config.maxTransactionBytes { return .tooLarge }
        if let id = req.idempotencyID, id.isEmpty || id.count > 64 { return .invalidRequest }
        return nil
    }

    // MARK: Commit pipeline

    private var lastResolvedVersion: Version = 0

    private func startBatch() {
        let take = min(pending.count, config.maxTransactionsPerBatch)
        let b = BatchState(num: requestNum, txns: Array(pending.prefix(take)), meta: metaPending)
        pending.removeFirst(take)
        metaPending = []
        requestNum += 1
        statBatches += 1
        statTxns += b.txns.count
        b.startedAt = context.now
        for t in b.txns {
            let w = b.startedAt &- t.arrived
            statWait.sum += w; statWait.max = max(statWait.max, w)
        }
        batches.append(b)
        lastBatchAt = context.now
        versionAndResolve(b)
    }

    private func idempotencyKey(_ req: CommitRequest) -> Bytes? {
        req.idempotencyID.map { SystemKeys.idempotency($0) }
    }

    /// One round trip: the master allocates the batch's version and forwards the conflict
    /// ranges to the resolver, which replies here with the version and the verdicts.
    private func versionAndResolve(_ b: BatchState) {
        let txns = b.txns.map { p -> ResolveTxn in
            var readKeys = p.request.readConflictKeys
            var writeKeys = p.request.writeConflictKeys
            if let k = idempotencyKey(p.request) {
                // Every attempt of an operation reads and writes its idempotency key, so two
                // attempts can never both commit.
                readKeys.append(k)
                writeKeys.append(k)
            }
            return ResolveTxn(readVersion: p.request.readVersion, readKeys: readKeys,
                              readRanges: p.request.readConflictRanges, writeKeys: writeKeys)
        }
        let req = ResolveRequest(generation: spec.generation, proxyIndex: spec.index, version: 0, prevVersion: 0,
                                 lastSeenVersion: map.version, txns: txns, metaOps: b.meta.flatMap(\.request.metaOps))
        sendVersionAndResolve(b, req)
    }

    private func sendVersionAndResolve(_ b: BatchState, _ req: ResolveRequest) {
        context.request(to: spec.master, timeout: config.requestTimeout * 2, {
            .versionAndResolve(rid: $0, requestNum: b.num, req)
        }) { [weak self] reply in
            guard let self, !self.dead else { return }
            switch reply {
            case .resolveReply(_, let r)?:
                b.version = r.version
                b.prevVersion = r.prevVersion
                b.resolution = r
                b.resolvedAt = self.context.now
                self.finalizeReady()
            case .errorReply(_, .staleGeneration)?:
                self.generationEnded("master or resolver gone")
            case .errorReply?:
                self.context.schedule(after: 10_000) { self.sendVersionAndResolve(b, req) }
            default:
                // Same request number: the master returns the same version.
                self.sendVersionAndResolve(b, req)
            }
        }
    }

    /// Finalizes resolved batches strictly in order (they define the routing state).
    private func finalizeReady() {
        for b in batches where !b.finalized {
            guard b.resolution != nil else { return }
            finalize(b)
        }
    }

    private func finalize(_ b: BatchState) {
        let r = b.resolution!
        b.finalized = true
        // Routing state is a function of version order: it must never go backwards.
        guard b.version > lastResolvedVersion else {
            context.log(.error, "batch \(b.num) has version \(b.version) after \(lastResolvedVersion); ending generation")
            return generationEnded("batch versions out of order")
        }
        // Metadata committed by other proxies at earlier versions.
        for s in r.stateOps.sorted(by: { $0.version < $1.version }) where s.version > map.version {
            for op in s.ops { map.apply(op, at: s.version) }
        }

        var messages: [LogMessage] = []
        var order: UInt16 = 0
        var idempotent: [SystemKeys.IdempotencyEntry] = []
        var retention: UInt32 = 0
        for (i, p) in b.txns.enumerated() {
            let verdict = i < r.verdicts.count ? r.verdicts[i] : 1
            switch verdict {
            case 0:
                let routed = p.request.mutations.map { ($0, map.tags(forSlot: Slots.slot(for: $0.key))) }
                if routed.contains(where: { $0.1.isEmpty }) {
                    b.replies.append((p, .failed(.unavailable)))
                    continue
                }
                let stamp = Versionstamp(version: b.version, order: order)
                for (m, tags) in routed { messages.append(LogMessage(order: order, mutation: m, tags: tags)) }
                if let id = p.request.idempotencyID {
                    idempotent.append(SystemKeys.IdempotencyEntry(id: id, attemptID: p.request.attemptID, order: order))
                    retention = max(retention, p.request.idempotencyRetention)
                }
                b.replies.append((p, .committed(stamp)))
                order += 1
            case 2:
                b.replies.append((p, .failed(.transactionTooOld)))
            default:
                b.replies.append((p, .failed(.notCommitted)))
            }
        }
        if !idempotent.isEmpty {
            // One record for the whole batch, atomically with its commits. If its shard
            // has no owner the batch can't record them: fail those operations instead.
            let key = SystemKeys.idempotencyBatch(b.version)
            let tags = map.tags(forSlot: Slots.slot(for: key))
            if tags.isEmpty {
                let ids = Set(idempotent.map(\.order))
                messages.removeAll { ids.contains($0.order) }
                for i in b.replies.indices {
                    if case .committed(let stamp) = b.replies[i].result, ids.contains(stamp.order), b.replies[i].pending.request.idempotencyID != nil {
                        b.replies[i].result = .failed(.unavailable)
                    }
                }
            } else {
                let m = Mutation.set(key: key, value: SystemKeys.idempotencyBatchValue(idempotent),
                                     ttl: UInt64(max(1, retention)) * 1_000_000)
                messages.append(LogMessage(order: order, mutation: m, tags: tags))
                order += 1
            }
        }
        var meta: [TaggedMeta] = []
        for p in b.meta {
            for op in p.request.metaOps {
                meta.append(TaggedMeta(op: op, tags: map.tagsAffected(by: op) + [metadataTag]))
                map.apply(op, at: b.version)
            }
            b.replies.append((p, .committed(Versionstamp(version: b.version, order: order))))
            order += 1
        }
        if spec.writeSnapshot && !snapshotWritten {
            snapshotWritten = true
            meta.append(TaggedMeta(op: .snapshot(map), tags: [metadataTag]))
        }
        map.version = max(map.version, b.version)
        lastResolvedVersion = b.version
        b.hasData = !messages.isEmpty || !meta.isEmpty
        wakeMapWaiters()

        defer { pump() }
        let push = TLogPush(generation: spec.generation, prevVersion: b.prevVersion,
                            entry: LogEntry(version: b.version, messages: messages, meta: meta),
                            knownCommitted: knownCommitted)
        sendPushes(b, push)
    }

    /// Pushes a version to every log. Logs acknowledge durability to the master (whose
    /// committed version completes the batch), and reply to us only to refuse. So a push
    /// is fire-and-forget, re-sent to every log until the master declares it committed:
    /// a log treats a repeat of a version it has as a no-op.
    private func sendPushes(_ b: BatchState, _ push: TLogPush) {
        b.push = push
        b.pushedAt = context.now
        for t in spec.tlogs { context.send(.tlogPush(rid: 0, push), to: t) }
    }

    /// Re-pushes batches the master hasn't declared committed within a request timeout
    /// (a push or durability report may have been lost on a broken connection).
    private func repushStale() {
        let now = context.now
        for b in batches where b.finalized && !b.committed {
            if let push = b.push, now &- b.pushedAt >= config.requestTimeout { sendPushes(b, push) }
        }
    }

    /// Reports the leading durable batches to the master, then replies to their clients.
    /// Replies to the leading batches the master has declared committed (on every log, and
    /// known to the master, so any read version handed out from now on includes them).
    private func completeCommitted() {
        var done = 0
        for b in batches {
            guard b.finalized, b.version <= knownCommitted else { break }
            for r in b.replies { reply(r.pending, r.result) }
            b.committed = true
            let resolve = b.resolvedAt &- b.startedAt, durable = context.now &- b.resolvedAt
            statResolve.sum += resolve; statResolve.max = max(statResolve.max, resolve)
            statDurable.sum += durable; statDurable.max = max(statDurable.max, durable)
            statDone += 1
            done += 1
        }
        if done > 0 {
            batches.removeFirst(done)
            pump()
        }
    }

    private func wakeMapWaiters() {
        guard !mapWaiters.isEmpty else { return }
        let ready = mapWaiters.filter { lastResolvedVersion >= $0.minVersion }
        mapWaiters.removeAll { lastResolvedVersion >= $0.minVersion }
        for w in ready { context.send(.shardMapReply(rid: w.rid, map), to: w.client) }
    }

    // MARK: Read versions

    private func startGRV() {
        grvRounds += 1
        let waiting = grvQueue
        grvQueue = []
        var masterVersion: Version?
        var confirmed = 0
        var failed = false
        var finished = false
        let needed = spec.tlogs.count
        func finish() {
            guard !finished, !dead else { return }
            if failed {
                finished = true
                grvRounds -= 1
                for w in waiting { context.send(.errorReply(rid: w.rid, .unavailable), to: w.client) }
                return
            }
            guard let mv = masterVersion, confirmed == needed else { return }
            finished = true
            grvRounds -= 1
            let v = max(mv, knownCommitted)
            for w in waiting { context.send(.readVersionReply(rid: w.rid, version: v), to: w.client) }
            // Requests that arrived during this round get the next one right away.
            if !grvQueue.isEmpty && grvRounds < maxGRVRounds { startGRV() }
        }
        context.request(to: spec.master, timeout: config.requestTimeout, { .getLiveCommitted(rid: $0) }) { [weak self] reply in
            guard self != nil else { return }
            if case .liveCommittedReply(_, let v)? = reply { masterVersion = v } else { failed = true }
            finish()
        }
        for t in spec.tlogs {
            context.request(to: t, timeout: config.requestTimeout, { .tlogConfirm(rid: $0, generation: self.spec.generation) }) { [weak self] reply in
                guard let self else { return }
                switch reply {
                case .tlogConfirmReply(_, true)?: confirmed += 1
                case .tlogConfirmReply(_, false)?:
                    failed = true
                    finish()
                    self.generationEnded("a log is locked")
                    return
                default: failed = true
                }
                finish()
            }
        }
    }
}
