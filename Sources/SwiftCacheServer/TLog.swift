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
    public var generation: UInt64 { spec.generation }
    private var entries = Deque<LogEntry>()
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

    public func start() {
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
            // A consumer has at most one held peek per tag: answer a superseded one now.
            if let i = peekWaiters.firstIndex(where: { $0.from == from && $0.tag == tag }) {
                let old = peekWaiters.remove(at: i)
                context.send(.tlogPeekReply(rid: old.rid, PeekReply(status: .ok, entries: [], end: old.after)), to: old.from)
            }
            handlePeek(rid: rid, tag: tag, after: after, upTo: upTo, need: need, from: from, waiterID: nil)
        case .tlogPop(let tag, let version):
            if let p = popped[tag], version > p {
                popped[tag] = version
                while let v = tagVersions[tag]?.first, v <= version { tagVersions[tag]!.removeFirst() }
                collectGarbage()
            }
        case .tlogLock(let rid, let generation):
            if generation == spec.generation {
                if !locked { context.log(.info, "locked by recovery (durable \(durable), committed \(knownCommitted))") }
                locked = true
                wakePeekWaiters()
                context.send(.tlogLockReply(rid: rid, LockReply(known: true, generation: spec.generation,
                                                                 durableVersion: durable, knownCommitted: knownCommitted)), to: from)
            } else {
                context.send(.tlogLockReply(rid: rid, LockReply(known: false, generation: spec.generation, durableVersion: 0, knownCommitted: 0)), to: from)
            }
        case .tlogCommitted(let generation, let version):
            guard ready, generation == spec.generation else { return }
            advanceKnownCommitted(version)
        case .tlogConfirm(let rid, let generation):
            context.send(.tlogConfirmReply(rid: rid, ok: ready && !locked && generation == spec.generation), to: from)
        case .tlogFetch(let rid, let upTo):
            let es = entries.elements.filter { $0.version <= upTo }
            let pops = popped.keys.sorted().map { (tag: $0, version: popped[$0]!) }
            context.send(.tlogFetchReply(rid: rid, FetchReply(ok: ready, entries: Array(es), popped: pops)), to: from)
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
        if push.entry.version <= durable {
            // A retransmission of something we already have.
            context.send(.tlogPushReply(rid: rid, ok: true), to: from)
            return
        }
        pendingPushes[push.prevVersion] = (from, rid, push)
        var appended = false
        while let next = pendingPushes.removeValue(forKey: durable) {
            append(next.push)
            context.send(.tlogPushReply(rid: next.rid, ok: true), to: next.from)
            appended = true
        }
        if appended {
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
        while let s = pendingSnapshots.first, s <= knownCommitted {
            pendingSnapshots.removeFirst()
            popped[metadataTag] = max(popped[metadataTag] ?? 0, s - 1)
        }
    }

    private func collectGarbage() {
        guard let floor = popped.values.min() else { return }
        while let first = entries.first, first.version <= floor { entries.removeFirst() }
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
            context.send(.tlogPeekReply(rid: rid, PeekReply(status: .stopped, entries: [], end: requested)), to: from)
            return
        }
        guard let tagPopped = popped[tag] else {
            context.send(.tlogPeekReply(rid: rid, PeekReply(status: .tagRemoved, entries: [], end: requested)), to: from)
            return
        }
        // A brand-new consumer (after = 0) starts at the tag's beginning.
        let after = requested == 0 ? tagPopped : requested
        if after < tagPopped {
            context.send(.tlogPeekReply(rid: rid, PeekReply(status: .tooOld, entries: [], end: requested)), to: from)
            return
        }
        let limit = upTo > 0 ? min(upTo, durable) : knownCommitted
        // Hold the request unless there's new data for the tag, the waiting read's version
        // has become available, or the caller asked for a fixed range.
        let answerable = upTo > 0 || locked || need == .max || (need > after && limit >= need) || hasData(tag, after: after, through: limit)
        if limit <= after || !answerable {
            if locked || upTo > 0 || need == .max {
                context.send(.tlogPeekReply(rid: rid, PeekReply(status: locked ? .stopped : .ok, entries: [], end: after)), to: from)
                return
            }
            // Long poll: answer when something new is committed, or after peekTimeout.
            let id = waiterID ?? { nextWaiterID += 1; return nextWaiterID }()
            peekWaiters.append(PeekWaiter(from: from, rid: rid, tag: tag, after: after, need: need, id: id))
            if waiterID == nil {
                context.schedule(after: config.peekTimeout) { [weak self] in
                    guard let self, let i = self.peekWaiters.firstIndex(where: { $0.id == id }) else { return }
                    let w = self.peekWaiters.remove(at: i)
                    // Answer with whatever is committed (a full scan: the reply's content
                    // never depends on the tag index, only its timing does).
                    self.handlePeek(rid: w.rid, tag: w.tag, after: w.after, upTo: 0, need: .max, from: w.from, waiterID: w.id)
                }
            }
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
        context.send(.tlogPeekReply(rid: rid, PeekReply(status: .ok, entries: out, end: end)), to: from)
    }

    private func wakePeekWaiters() {
        guard !peekWaiters.isEmpty else { return }
        let waiting = peekWaiters
        peekWaiters = []
        for w in waiting {
            let ready = locked || (knownCommitted > w.after
                && ((w.need > w.after && knownCommitted >= w.need) || hasData(w.tag, after: w.after, through: knownCommitted)))
            if ready {
                handlePeek(rid: w.rid, tag: w.tag, after: w.after, upTo: 0, need: w.need, from: w.from, waiterID: w.id)
            } else {
                peekWaiters.append(w)
            }
        }
    }
}
