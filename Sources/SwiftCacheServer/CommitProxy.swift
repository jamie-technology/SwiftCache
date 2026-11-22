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
        var acks: Set<Endpoint> = []
        var durable = false
        var replies: [(client: Endpoint, rid: UInt64, result: CommitResult)] = []
        var hasData = false
        init(num: UInt64, txns: [Pending], meta: [Pending]) { self.num = num; self.txns = txns; self.meta = meta }
    }
    private var batches: [BatchState] = []
    private var reporting = false
    private var lastBatchAt: UInt64 = 0
    private var committedAdvanced = false
    private var snapshotWritten = false

    private var grvQueue: [(client: Endpoint, rid: UInt64)] = []
    private var grvInFlight = false
    private var mapWaiters: [(client: Endpoint, rid: UInt64, minVersion: Version)] = []

    init(context: RoleContext, config: ClusterConfig, spec: ProxySpec) {
        self.context = context
        self.config = config
        self.spec = spec
        self.map = spec.shardMap
        self.knownCommitted = spec.startVersion
    }

    public func start() {
        context.ready()
        context.every(config.batchInterval) { [weak self] in self?.tick() }
    }

    private func generationEnded(_ why: String) {
        guard !dead else { return }
        dead = true
        context.log(.info, "generation \(spec.generation) ended (\(why)); stopping")
        for g in grvQueue { context.send(.errorReply(rid: g.rid, .staleGeneration), to: g.client) }
        context.stopSelf()
    }

    private func tick() {
        guard !dead else { return }
        let idle = context.now &- lastBatchAt >= config.idleBatchInterval
        let needSnapshot = spec.writeSnapshot && !snapshotWritten
        if batches.count < config.maxInflightBatches
            && (!pending.isEmpty || !metaPending.isEmpty || idle || committedAdvanced || needSnapshot) {
            startBatch()
        }
        if !grvQueue.isEmpty && !grvInFlight { startGRV() }
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
        case .commit(let rid, let req):
            if let error = validate(req, from: from) {
                context.send(.commitReply(rid: rid, .failed(error)), to: from)
                return
            }
            if req.metaOps.isEmpty {
                pending.append(Pending(client: from, rid: rid, request: req))
            } else {
                metaPending.append(Pending(client: from, rid: rid, request: req))
            }
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
        batches.append(b)
        lastBatchAt = context.now
        committedAdvanced = false
        requestVersion(b)
    }

    private func requestVersion(_ b: BatchState) {
        context.request(to: spec.master, timeout: config.requestTimeout, {
            .getCommitVersion(rid: $0, proxyIndex: self.spec.index, requestNum: b.num)
        }) { [weak self] reply in
            guard let self, !self.dead else { return }
            switch reply {
            case .commitVersionReply(_, let prev, let version)?:
                b.prevVersion = prev
                b.version = version
                self.resolve(b)
            case .errorReply(_, .staleGeneration)?:
                self.generationEnded("master gone")
            case .errorReply?:
                self.context.schedule(after: 10_000) { self.requestVersion(b) }
            default:
                self.requestVersion(b)  // same requestNum: the master returns the same version
            }
        }
    }

    private func idempotencyKey(_ req: CommitRequest) -> Bytes? {
        req.idempotencyID.map { SystemKeys.idempotency($0) }
    }

    private func resolve(_ b: BatchState) {
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
        let req = ResolveRequest(generation: spec.generation, proxyIndex: spec.index, version: b.version,
                                 prevVersion: b.prevVersion, lastSeenVersion: map.version,
                                 txns: txns, metaOps: b.meta.flatMap(\.request.metaOps))
        sendResolve(b, req)
    }

    private func sendResolve(_ b: BatchState, _ req: ResolveRequest) {
        guard let resolver = spec.resolvers.first else { return }
        context.request(to: resolver, timeout: config.requestTimeout * 2, { .resolve(rid: $0, req) }) { [weak self] reply in
            guard let self, !self.dead else { return }
            switch reply {
            case .resolveReply(_, let r)?:
                b.resolution = r
                self.finalizeReady()
            case .errorReply?:
                self.generationEnded("resolver refused")
            default:
                self.sendResolve(b, req)
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
        for (i, p) in b.txns.enumerated() {
            let verdict = i < r.verdicts.count ? r.verdicts[i] : 1
            switch verdict {
            case 0:
                let routed = p.request.mutations.map { ($0, map.tags(forSlot: Slots.slot(for: $0.key))) }
                if routed.contains(where: { $0.1.isEmpty }) {
                    b.replies.append((p.client, p.rid, .failed(.unavailable)))
                    continue
                }
                let stamp = Versionstamp(version: b.version, order: order)
                for (m, tags) in routed { messages.append(LogMessage(order: order, mutation: m, tags: tags)) }
                if let key = idempotencyKey(p.request) {
                    let ttl = UInt64(max(1, p.request.idempotencyRetention)) * 1_000_000
                    let value = SystemKeys.idempotencyValue(stamp: stamp, attemptID: p.request.attemptID)
                    let m = Mutation.set(key: key, value: value, ttl: ttl)
                    let tags = map.tags(forSlot: Slots.slot(for: key))
                    if !tags.isEmpty { messages.append(LogMessage(order: order, mutation: m, tags: tags)) }
                }
                b.replies.append((p.client, p.rid, .committed(stamp)))
                order += 1
            case 2:
                b.replies.append((p.client, p.rid, .failed(.transactionTooOld)))
            default:
                b.replies.append((p.client, p.rid, .failed(.notCommitted)))
            }
        }
        var meta: [TaggedMeta] = []
        for p in b.meta {
            for op in p.request.metaOps {
                meta.append(TaggedMeta(op: op, tags: map.tagsAffected(by: op) + [metadataTag]))
                map.apply(op, at: b.version)
            }
            b.replies.append((p.client, p.rid, .committed(Versionstamp(version: b.version, order: order))))
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

        let push = TLogPush(generation: spec.generation, prevVersion: b.prevVersion,
                            entry: LogEntry(version: b.version, messages: messages, meta: meta),
                            knownCommitted: knownCommitted)
        for t in spec.tlogs { sendPush(b, push, to: t) }
    }

    private func sendPush(_ b: BatchState, _ push: TLogPush, to tlog: Endpoint) {
        context.request(to: tlog, timeout: config.requestTimeout, { .tlogPush(rid: $0, push) }) { [weak self] reply in
            guard let self, !self.dead else { return }
            switch reply {
            case .tlogPushReply(_, true)?:
                b.acks.insert(tlog)
                if b.acks.count == self.spec.tlogs.count {
                    b.durable = true
                    self.completeDurable()
                }
            case .tlogPushReply(_, false)?, .errorReply?:
                // The log is locked (a newer generation is recovering) or gone. Whether this
                // batch survives is decided by that recovery; clients see an unknown result.
                self.generationEnded("log \(tlog) refused a push")
            default:
                self.sendPush(b, push, to: tlog)  // the log treats a repeat as a no-op
            }
        }
    }

    /// Reports the leading durable batches to the master, then replies to their clients.
    private func completeDurable() {
        guard !reporting else { return }
        var ready: [BatchState] = []
        for b in batches {
            guard b.durable else { break }
            ready.append(b)
        }
        guard let last = ready.last else { return }
        reporting = true
        reportCommitted(last.version) { [weak self] in
            guard let self, !self.dead else { return }
            self.reporting = false
            self.knownCommitted = max(self.knownCommitted, last.version)
            for b in ready {
                for r in b.replies { self.context.send(.commitReply(rid: r.rid, r.result), to: r.client) }
                if b.hasData { self.committedAdvanced = true }
            }
            self.batches.removeFirst(ready.count)
            self.completeDurable()
        }
    }

    private func reportCommitted(_ version: Version, then: @escaping () -> Void) {
        context.request(to: spec.master, timeout: config.requestTimeout, { .reportCommitted(rid: $0, version: version) }) { [weak self] reply in
            guard let self, !self.dead else { return }
            switch reply {
            case .ack?: then()
            case .errorReply?: self.generationEnded("master gone")
            default: self.reportCommitted(version, then: then)
            }
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
        grvInFlight = true
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
                grvInFlight = false
                for w in waiting { context.send(.errorReply(rid: w.rid, .unavailable), to: w.client) }
                return
            }
            guard let mv = masterVersion, confirmed == needed else { return }
            finished = true
            grvInFlight = false
            let v = max(mv, knownCommitted)
            knownCommitted = v
            for w in waiting { context.send(.readVersionReply(rid: w.rid, version: v), to: w.client) }
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
