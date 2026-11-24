import SwiftCacheCore

/// A transaction log of one generation.
///
/// Commit proxies push every version (in `prevVersion` order) to *every* log of the
/// generation; a commit is acknowledged only once all of them have it, so any single
/// surviving log holds every acknowledged commit. Storage servers pull their tag from
/// the log (`peek`) and release it (`pop`) once applied.
///
/// Recovery `lock`s the logs of the old generation: a locked log refuses further pushes
/// (fencing the old proxies) and confirms nothing (fencing old read versions), but keeps
/// serving its data so the new generation can copy it.
public final class TLogRole: Role {
    public let context: RoleContext
    let config: ClusterConfig
    let spec: TLogSpec

    private var ready = false
    public private(set) var locked = false
    /// The read-version lease granted to this generation's master runs until here.
    private var leaseExpiry: UInt64 = 0
    public var generation: UInt64 { spec.generation }
    private var entries = Deque<LogEntry>()
    /// Metadata-tag history from entries already released by every storage tag: kept
    /// (stripped of data) until a committed snapshot supersedes it, so recovery can still
    /// rebuild the shard map, without retaining the data those entries carried.
    private var metaHistory = Deque<LogEntry>()
    /// Last version appended.
    public private(set) var durable: Version = 0
    /// Highest version known to be on every log of the generation (safe to expose).
    public private(set) var knownCommitted: Version = 0
    /// Per live tag: data at or below this version has been released.
    private var popped: [Tag: Version] = [:]
    private var pendingPushes: [Version: (from: Endpoint, rid: UInt64, push: TLogPush)] = [:]
    /// Metadata snapshots not yet known committed. Older metadata may only be compacted
    /// once a snapshot is on *every* log: a snapshot above the eventual recovery version
    /// is discarded by recovery, and the history it replaced must still be there.
    private var pendingSnapshots: [Version] = []

    private struct PeekWaiter {
        var from: Endpoint
        var rid: UInt64
        var tag: Tag
        var after: Version
        var need: Version
        var id: UInt64
        /// When this subscription last got a reply (heartbeats keep consumers' watchdogs quiet).
        var lastSent: UInt64
    }
    /// Per tag, the versions that carry data for it (ascending, trimmed on pop): lets a
    /// held peek be answered only when its tag actually has something new.
    private var tagVersions: [Tag: Deque<Version>] = [:]
    private var peekWaiters: [PeekWaiter] = []
    private var nextWaiterID: UInt64 = 0

    init(context: RoleContext, config: ClusterConfig, spec: TLogSpec) {
        self.context = context
        self.config = config
        self.spec = spec
    }

    /// The tags of each message at `version` (debugging).
    public func debugTags(at version: Version) -> [[Tag]]? {
        entries.elements.first { $0.version == version }.map { $0.messages.map(\.tags) }
    }

    /// Versions retained (with data, and metadata-only), for leak checks.
    public var retainedVersions: (entries: Int, metaOnly: Int) { (entries.count, metaHistory.count) }

    /// Data at or below this version has been released by every storage tag.
    public var dataFloor: Version? { popped.filter { $0.key != metadataTag }.values.min() }

    public func start() {
        context.every(config.peekTimeout / 4) { [weak self] in self?.heartbeatSubscriptions() }
        durable = spec.startVersion
        knownCommitted = spec.startVersion
        popped[metadataTag] = 0
        for tag in spec.initialTags { popped[tag] = spec.startVersion }
        if spec.seedFrom.isEmpty {
            becomeReady()
        } else {
            seed(attempt: 0)
        }
    }

    private func becomeReady() {
        ready = true
        context.log(.info, "tlog ready for generation \(spec.generation) at \(spec.startVersion) (\(entries.count) seeded versions, \(popped.count - 1) tags)")
        context.ready()
    }

    /// Copies the previous generation's unreleased data (≤ startVersion) from a locked log.
    private func seed(attempt: Int) {
        guard attempt < spec.seedFrom.count * 3 else {
            context.log(.error, "could not seed from any previous log")
            return
        }
        let source = spec.seedFrom[attempt % spec.seedFrom.count]
        context.request(to: source, timeout: config.requestTimeout * 5, { .tlogFetch(rid: $0, upTo: self.spec.startVersion) }) { [weak self] reply in
            guard let self else { return }
            guard case .tlogFetchReply(_, let f)? = reply, f.ok else {
                self.seed(attempt: attempt + 1)
                return
            }
            for e in f.entries where e.version <= self.spec.startVersion {
                self.entries.append(e)
                self.indexTags(e)
            }
            for p in f.popped { self.popped[p.tag] = p.version }
            self.becomeReady()
        }
    }

    public func receive(_ message: Message, from: Endpoint) {
        switch message {
        case .tlogPush(let rid, let push):
            handlePush(rid: rid, push: push, from: from)
        case .tlogPeek(let rid, let tag, let after, let upTo, let need):
            // A peek is a subscription: replies keep streaming (same rid) until the consumer
            // peeks again, unsubscribes, or the stream ends. A consumer has one per tag: a
            // new peek replaces its old subscription.
            peekWaiters.removeAll { $0.from == from && $0.tag == tag }
            handlePeek(rid: rid, tag: tag, after: after, upTo: upTo, need: need, from: from, waiterID: nil)
        case .tlogUnsubscribe(let rid, _):
            // Request ids are unique per consumer role, so (from, rid) names one stream.
            peekWaiters.removeAll { $0.from == from && $0.rid == rid }
        case .tlogNeed(let tag, let need):
            // Raise the need of this consumer's held peek; if it was already answered the
            // note is moot (the consumer's next peek carries its needs).
            guard let i = peekWaiters.firstIndex(where: { $0.from == from && $0.tag == tag }) else { return }
            peekWaiters[i].need = max(peekWaiters[i].need, need)
            if knownCommitted >= need && need > peekWaiters[i].after {
                let w = peekWaiters.remove(at: i)
                handlePeek(rid: w.rid, tag: w.tag, after: w.after, upTo: 0, need: w.need, from: w.from, waiterID: w.id)
            }
        case .tlogPop(let tag, let version):
            if let p = popped[tag], version > p {
                popped[tag] = version
                while let v = tagVersions[tag]?.first, v <= version { tagVersions[tag]!.removeFirst() }
                collectGarbage()
            }
        case .tlogLease(let rid, let generation):
            // A locked log grants nothing: the old master can't renew past a recovery.
            guard ready, !locked, generation == spec.generation else { return }
            leaseExpiry = context.now + config.readVersionLease
            context.send(.ack(rid: rid), to: from)
        case .tlogLock(let rid, let generation):
            if generation == spec.generation {
                if !locked { context.log(.info, "locked by recovery (durable \(durable), committed \(knownCommitted))") }
                locked = true
                wakePeekWaiters()
                // Stop accepting pushes now, but don't let the recovery proceed until the
                // read-version lease we granted has expired.
                let reply = Message.tlogLockReply(rid: rid, LockReply(known: true, generation: spec.generation,
                                                                     durableVersion: durable, knownCommitted: knownCommitted))
                let wait = leaseExpiry > context.now ? leaseExpiry - context.now : 0
                if wait == 0 {
                    context.send(reply, to: from)
                } else {
                    context.schedule(after: wait) { [weak self] in self?.context.send(reply, to: from) }
                }
            } else {
                context.send(.tlogLockReply(rid: rid, LockReply(known: false, generation: spec.generation, durableVersion: 0, knownCommitted: 0)), to: from)
            }
        case .tlogCommitted(let generation, let version):
            guard ready, generation == spec.generation else { return }
            advanceKnownCommitted(version)
        case .tlogConfirm(let rid, let generation):
            context.send(.tlogConfirmReply(rid: rid, ok: ready && !locked && generation == spec.generation), to: from)
        case .tlogFetch(let rid, let upTo):
            // Released entries survive only as metadata history; both are in version order
            // and every history entry precedes every retained entry.
            let es = metaHistory.elements.filter { $0.version <= upTo } + entries.elements.filter { $0.version <= upTo }
            let pops = popped.keys.sorted().map { (tag: $0, version: popped[$0]!) }
            context.send(.tlogFetchReply(rid: rid, FetchReply(ok: ready, entries: es, popped: pops)), to: from)
        default:
            if let rid = message.rid, !message.isReply { context.send(.errorReply(rid: rid, .invalidRequest), to: from) }
        }
    }

    // MARK: Pushes

    private func handlePush(rid: UInt64, push: TLogPush, from: Endpoint) {
        guard ready, !locked, push.generation == spec.generation else {
            context.send(.tlogPushReply(rid: rid, ok: false), to: from)
            return
        }
        // Success isn't acknowledged to the proxy: durability goes to the master, whose
        // committed version completes the commit. Only refusals are answered.
        if push.entry.version <= durable {
            // A retransmission of something we have; its committed version may be news.
            return advanceKnownCommitted(push.knownCommitted)
        }
        pendingPushes[push.prevVersion] = (from, rid, push)
        var appended = false
        while let next = pendingPushes.removeValue(forKey: durable) {
            append(next.push)
            appended = true
        }
        if appended {
            // Commit acknowledgement path: the master learns durability directly from
            // every log (no extra proxy -> master round trip).
            if !spec.master.isEmpty {
                context.send(.tlogDurable(generation: spec.generation, version: durable), to: spec.master)
            }
            collectGarbage()
            wakePeekWaiters()
        }
    }

    private func append(_ push: TLogPush) {
        let e = push.entry
        for tm in e.meta {
            switch tm.op {
            case .createShard(_, let members, _):
                for m in members where popped[m.tag] == nil { popped[m.tag] = e.version - 1 }
            case .addReplica(_, let m, _, _, _):
                if popped[m.tag] == nil { popped[m.tag] = e.version - 1 }
            case .removeReplica(_, let tag):
                popped[tag] = nil
            case .snapshot:
                pendingSnapshots.append(e.version)
            default:
                break
            }
        }
        indexTags(e)
        entries.append(e)
        durable = e.version
        knownCommitted = min(durable, max(knownCommitted, push.knownCommitted))
        compactSnapshots()
    }

    /// Learns that everything up to `version` is on every log of the generation.
    private func advanceKnownCommitted(_ version: Version) {
        let v = min(durable, version)
        guard v > knownCommitted else { return }
        knownCommitted = v
        compactSnapshots()
        collectGarbage()
        wakePeekWaiters()
    }

    /// A committed snapshot supersedes everything older on the metadata tag.
    private func compactSnapshots() {
        var compacted = false
        while let s = pendingSnapshots.first, s <= knownCommitted {
            pendingSnapshots.removeFirst()
            popped[metadataTag] = max(popped[metadataTag] ?? 0, s - 1)
            compacted = true
        }
        guard compacted, let floor = popped[metadataTag] else { return }
        while let first = metaHistory.first, first.version <= floor { metaHistory.removeFirst() }
        while let v = tagVersions[metadataTag]?.first, v <= floor { tagVersions[metadataTag]!.removeFirst() }
    }

    /// Releases entries every storage tag has popped. The metadata tag doesn't hold data
    /// back: its unreleased part of a released entry moves to `metaHistory`. (It used to
    /// count towards the floor, and only advances when a snapshot commits, i.e. once per
    /// generation, so logs retained every entry they ever received.)
    private func collectGarbage() {
        let metaFloor = popped[metadataTag] ?? 0
        guard let floor = dataFloor ?? (popped.isEmpty ? nil : knownCommitted) else { return }
        while let first = entries.first, first.version <= floor {
            entries.removeFirst()
            guard first.version > metaFloor else { continue }
            let meta = first.meta.filter { $0.tags.contains(metadataTag) }
            if !meta.isEmpty { metaHistory.append(LogEntry(version: first.version, messages: [], meta: meta)) }
        }
    }

    // MARK: Peeks

    private func indexTags(_ e: LogEntry) {
        var touched = Set<Tag>()
        for m in e.messages { touched.formUnion(m.tags) }
        for tm in e.meta { touched.formUnion(tm.tags) }
        for t in touched.sorted() { tagVersions[t, default: Deque()].append(e.version) }
    }

    /// Whether `tag` has data at a version in (after, limit].
    private func hasData(_ tag: Tag, after: Version, through limit: Version) -> Bool {
        guard let vs = tagVersions[tag] else { return false }
        let all = vs.elements
        var lo = all.startIndex, hi = all.endIndex
        while lo < hi {
            let mid = (lo + hi) / 2
            if all[mid] <= after { lo = mid + 1 } else { hi = mid }
        }
        return lo < all.endIndex && all[lo] <= limit
    }

    private func handlePeek(rid: UInt64, tag: Tag, after requested: Version, upTo: Version, need: Version, from: Endpoint, waiterID: UInt64?) {
        guard ready else {
            context.send(.tlogPeekReply(rid: rid, PeekReply(status: .stopped, entries: [], end: requested, begin: requested)), to: from)
            return
        }
        guard let tagPopped = popped[tag] else {
            context.send(.tlogPeekReply(rid: rid, PeekReply(status: .tagRemoved, entries: [], end: requested)), to: from)
            return
        }
        // A brand-new consumer (after = 0) starts at the tag's beginning: for it, the stream
        // continues from 0 (it has everything before the tag existed, i.e. nothing).
        let after = requested == 0 ? tagPopped : requested
        let begin = requested
        if after < tagPopped {
            context.send(.tlogPeekReply(rid: rid, PeekReply(status: .tooOld, entries: [], end: requested)), to: from)
            return
        }
        let limit = upTo > 0 ? min(upTo, durable) : knownCommitted
        // Hold the request unless there's new data for the tag, the waiting read's version
        // has become available, or the caller asked for a fixed range.
        let answerable = upTo > 0 || locked || need == .max || (need > after && limit >= need)
            || limit >= after &+ config.replicaFreshness || hasData(tag, after: after, through: limit)
        let id = waiterID ?? { nextWaiterID += 1; return nextWaiterID }()
        if limit <= after || !answerable {
            if locked {
                context.send(.tlogPeekReply(rid: rid, PeekReply(status: .stopped, entries: [], end: after, begin: after)), to: from)
                return
            }
            if upTo > 0 || need == .max {
                respond(rid: rid, tag: tag, after: after, begin: begin, upTo: upTo, need: need, from: from, id: id, entries: [], end: after)
                return
            }
            // Hold: answered when something new is committed, or by the heartbeat sweep.
            peekWaiters.append(PeekWaiter(from: from, rid: rid, tag: tag, after: after, need: need, id: id, lastSent: context.now))
            return
        }
        var out: [PeekEntry] = []
        var end = limit
        var count = 0
        // Binary search for the first entry after `after`.
        let all = entries.elements
        var lo = all.startIndex, hi = all.endIndex
        while lo < hi {
            let mid = (lo + hi) / 2
            if all[mid].version <= after { lo = mid + 1 } else { hi = mid }
        }
        var i = lo
        while i < all.endIndex, all[i].version <= limit {
            let e = all[i]
            var ms: [(order: UInt16, mutation: Mutation)] = []
            for m in e.messages where m.tags.contains(tag) { ms.append((m.order, m.mutation)) }
            var meta: [MetaOp] = []
            for tm in e.meta where tm.tags.contains(tag) { meta.append(tm.op) }
            if !ms.isEmpty || !meta.isEmpty {
                out.append(PeekEntry(version: e.version, mutations: ms, meta: meta))
                count += ms.count + meta.count + 1
                if count >= 5_000 {
                    end = e.version
                    break
                }
            }
            i += 1
        }
        respond(rid: rid, tag: tag, after: after, begin: begin, upTo: upTo, need: need, from: from, id: id, entries: out, end: end)
    }

    /// Sends an ok peek reply covering (after, end] and, for a streaming peek, keeps the
    /// subscription going from `end`.
    private func respond(rid: UInt64, tag: Tag, after: Version, begin: Version, upTo: Version, need: Version, from: Endpoint, id: UInt64,
                         entries out: [PeekEntry], end: Version) {
        context.send(.tlogPeekReply(rid: rid, PeekReply(status: .ok, entries: out, end: end, begin: begin)), to: from)
        guard upTo == 0, !locked else { return }
        peekWaiters.append(PeekWaiter(from: from, rid: rid, tag: tag, after: max(after, end),
                                      need: need == .max ? 0 : need, id: id, lastSent: context.now))
    }

    /// Answers subscriptions that have had no reply for `peekTimeout` (an empty reply if
    /// nothing is committed for them), so consumers can tell a live stream from a dead one.
    private func heartbeatSubscriptions() {
        let now = context.now
        guard peekWaiters.contains(where: { now &- $0.lastSent >= config.peekTimeout }) else { return }
        let due = peekWaiters.filter { now &- $0.lastSent >= config.peekTimeout }
        peekWaiters.removeAll { now &- $0.lastSent >= config.peekTimeout }
        // Answer with whatever is committed (a full scan: the reply's content never depends
        // on the tag index, only its timing does).
        for w in due { handlePeek(rid: w.rid, tag: w.tag, after: w.after, upTo: 0, need: .max, from: w.from, waiterID: w.id) }
    }

    private func wakePeekWaiters() {
        guard !peekWaiters.isEmpty else { return }
        let waiting = peekWaiters
        peekWaiters = []
        for w in waiting {
            // Answer when the tag has new data, a waiting read needs the version, or the
            // replica would otherwise fall more than `replicaFreshness` behind committed
            // (keeps idle replicas current for "latest" reads).
            let ready = locked || (knownCommitted > w.after
                && ((w.need > w.after && knownCommitted >= w.need)
                    || knownCommitted >= w.after &+ config.replicaFreshness
                    || hasData(w.tag, after: w.after, through: knownCommitted)))
            if ready {
                handlePeek(rid: w.rid, tag: w.tag, after: w.after, upTo: 0, need: w.need, from: w.from, waiterID: w.id)
            } else {
                peekWaiters.append(w)
            }
        }
    }
}
