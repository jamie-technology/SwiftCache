import SwiftCacheCore

/// The in-memory multi-version key/value store of a storage server.
///
/// Every key maps to its versions in ascending versionstamp order. Reads at version `v`
/// see the newest version with `stamp.version <= v`. Old versions are garbage collected
/// once they fall out of the MVCC window.
final class VersionedStore {
    private(set) var data: [Bytes: [VersionedValue]] = [:]
    private var index = SortedKeyIndex()
    private(set) var versionCount = 0
    private(set) var approximateBytes = 0

    /// (version, key) for every write, in version order: drives incremental GC.
    private var gcQueue = Deque<(Version, Bytes)>()
    /// Min-heap of (expiresAt, key) for TTL'd values.
    private var expiryHeap = MinHeap<ExpiryEntry>()

    struct ExpiryEntry: Comparable {
        var expiresAt: Version
        var key: Bytes
        static func < (a: ExpiryEntry, b: ExpiryEntry) -> Bool {
            a.expiresAt != b.expiresAt ? a.expiresAt < b.expiresAt : bytesLess(a.key, b.key)
        }
    }

    var keyCount: Int { data.count }

    private static func size(of v: VersionedValue) -> Int { (v.value?.count ?? 0) + 24 }

    // MARK: Writes

    /// Applies a mutation at `stamp`. Mutations must be applied in versionstamp order.
    func apply(_ m: Mutation, stamp: Versionstamp) {
        switch m {
        case .set(let key, let value, let ttl):
            append(key, VersionedValue(stamp: stamp, value: value, expiresAt: ttl == 0 ? 0 : stamp.version &+ ttl))
        case .clear(let key):
            // Clearing a key that doesn't exist (or is already cleared) needn't store anything.
            if let latest = data[key]?.last, latest.value != nil {
                append(key, VersionedValue(stamp: stamp, value: nil))
            }
        case .add(let key, let delta):
            let current = data[key]?.last?.visibleValue(at: stamp.version)
            let sum = LittleEndianInt64.decode(current) &+ delta
            append(key, VersionedValue(stamp: stamp, value: LittleEndianInt64.encode(sum)))
        }
    }

    private func append(_ key: Bytes, _ v: VersionedValue) {
        if data[key] == nil {
            // System keys (idempotency records) are never range-read: keep them out of
            // the sorted index.
            if !SystemKeys.isSystem(key) { index.insert(key) }
            approximateBytes += key.count + 48
            data[key] = [v]
        } else {
            data[key]!.append(v)
        }
        versionCount += 1
        approximateBytes += Self.size(of: v)
        gcQueue.append((v.stamp.version, key))
        if v.expiresAt != 0 { expiryHeap.push(ExpiryEntry(expiresAt: v.expiresAt, key: key)) }
    }

    // MARK: Reads

    /// The newest version of `key` at or before `version`, if any.
    func get(_ key: Bytes, at version: Version) -> VersionedValue? {
        guard let versions = data[key] else { return nil }
        // Fast path: most reads are of the latest version.
        if let last = versions.last, last.stamp.version <= version { return last }
        var lo = 0, hi = versions.count
        while lo < hi {
            let mid = (lo + hi) / 2
            if versions[mid].stamp.version <= version { lo = mid + 1 } else { hi = mid }
        }
        return lo == 0 ? nil : versions[lo - 1]
    }

    /// Visible key/values in `range` at `version`, restricted to keys for which `include`
    /// returns true. Returns up to `limit` entries and whether more may exist.
    func range(_ range: KeyRange, at version: Version, limit: Int, include: (Bytes) -> Bool) -> (entries: [KeyValueEntry], more: Bool) {
        var out = [KeyValueEntry]()
        var more = false
        index.forEach(from: range.begin, to: range.end) { key in
            guard include(key), let v = get(key, at: version), let value = v.visibleValue(at: version) else { return true }
            if out.count >= limit { more = true; return false }
            out.append(KeyValueEntry(key: key, value: value, stamp: v.stamp))
            return true
        }
        return (out, more)
    }

    // MARK: Maintenance

    /// Drops versions that can no longer be read: for each key, everything older than its
    /// newest version at or below `oldest`. Keys whose only remaining version is a deletion
    /// (or expired) at or below `oldest` are removed entirely.
    func collectGarbage(oldest: Version) {
        while let (v, key) = gcQueue.first, v < oldest {
            gcQueue.removeFirst()
            trim(key, oldest: oldest)
        }
        while let top = expiryHeap.peek, top.expiresAt < oldest {
            expiryHeap.pop()
            trim(top.key, oldest: oldest)
        }
    }

    private func trim(_ key: Bytes, oldest: Version) {
        guard var versions = data[key] else { return }
        // Index of newest version <= oldest.
        var keepFrom = -1
        for (i, v) in versions.enumerated() where v.stamp.version <= oldest { keepFrom = i }
        guard keepFrom >= 0 else { return }
        if keepFrom > 0 {
            for v in versions[0..<keepFrom] { approximateBytes -= Self.size(of: v) }
            versionCount -= keepFrom
            versions.removeFirst(keepFrom)
        }
        if versions.count == 1, versions[0].visibleValue(at: oldest) == nil {
            approximateBytes -= Self.size(of: versions[0]) + key.count + 48
            versionCount -= 1
            data[key] = nil
            index.remove(key)
        } else {
            data[key] = versions
        }
    }

    /// Removes every version newer than `version` (used by coordinator recovery).
    func truncate(after version: Version) {
        var emptied = [Bytes]()
        for (key, versions) in data {
            guard let last = versions.last, last.stamp.version > version else { continue }
            let kept = versions.filter { $0.stamp.version <= version }
            for v in versions where v.stamp.version > version { approximateBytes -= Self.size(of: v) }
            versionCount -= versions.count - kept.count
            if kept.isEmpty { emptied.append(key) } else { data[key] = kept }
        }
        for key in emptied {
            data[key] = nil
            index.remove(key)
            approximateBytes -= key.count + 48
        }
    }

    /// Every version of every key in `slots`, sorted by key.
    func extract(slots: SlotSet) -> [SnapshotChunk.Item] {
        var items = [SnapshotChunk.Item]()
        for (key, versions) in data where slots.contains(Slots.slot(for: key)) {
            items.append(SnapshotChunk.Item(key: key, versions: versions))
        }
        items.sort { bytesLess($0.key, $1.key) }
        return items
    }

    /// Installs copied versions (merging with anything already present, deduplicated by stamp).
    func install(_ item: SnapshotChunk.Item) {
        if let existing = data[item.key] {
            var merged = existing
            let seen = Set(existing.map(\.stamp))
            for v in item.versions where !seen.contains(v.stamp) {
                merged.append(v)
                versionCount += 1
                approximateBytes += Self.size(of: v)
            }
            merged.sort { $0.stamp < $1.stamp }
            data[item.key] = merged
        } else {
            guard !item.versions.isEmpty else { return }
            data[item.key] = item.versions
            if !SystemKeys.isSystem(item.key) { index.insert(item.key) }
            approximateBytes += item.key.count + 48
            versionCount += item.versions.count
            for v in item.versions { approximateBytes += Self.size(of: v) }
        }
        for v in item.versions {
            gcQueue.append((v.stamp.version, item.key))
            if v.expiresAt != 0 { expiryHeap.push(ExpiryEntry(expiresAt: v.expiresAt, key: item.key)) }
        }
    }

    /// Deletes all data for keys in `slots`.
    func drop(slots: SlotSet) {
        let doomed = data.keys.filter { slots.contains(Slots.slot(for: $0)) }
        for key in doomed {
            for v in data[key]! { approximateBytes -= Self.size(of: v) }
            versionCount -= data[key]!.count
            approximateBytes -= key.count + 48
            data[key] = nil
            index.remove(key)
        }
    }

    func removeAll() {
        data = [:]
        index.removeAll()
        versionCount = 0
        approximateBytes = 0
        gcQueue = Deque()
        expiryHeap = MinHeap()
    }
}

// MARK: - Small collections

/// A simple FIFO queue with amortized O(1) operations.
struct Deque<Element> {
    private var items: [Element] = []
    private var head = 0

    var isEmpty: Bool { head >= items.count }
    var count: Int { items.count - head }
    var first: Element? { isEmpty ? nil : items[head] }

    mutating func append(_ e: Element) { items.append(e) }

    @discardableResult
    mutating func removeFirst() -> Element {
        let e = items[head]
        head += 1
        if head > 1024 && head * 2 > items.count {
            items.removeFirst(head)
            head = 0
        }
        return e
    }

    var elements: ArraySlice<Element> { items[head...] }

    mutating func modifyEach(_ body: (inout Element) -> Void) {
        for i in head..<items.count { body(&items[i]) }
    }
}

struct MinHeap<Element: Comparable> {
    private var items: [Element] = []
    var isEmpty: Bool { items.isEmpty }
    var count: Int { items.count }
    var peek: Element? { items.first }

    mutating func push(_ e: Element) {
        items.append(e)
        var i = items.count - 1
        while i > 0 {
            let p = (i - 1) / 2
            if items[i] < items[p] { items.swapAt(i, p); i = p } else { break }
        }
    }

    @discardableResult
    mutating func pop() -> Element? {
        guard !items.isEmpty else { return nil }
        let top = items[0]
        let last = items.removeLast()
        if !items.isEmpty {
            items[0] = last
            var i = 0
            while true {
                let l = 2 * i + 1, r = l + 1
                var m = i
                if l < items.count && items[l] < items[m] { m = l }
                if r < items.count && items[r] < items[m] { m = r }
                if m == i { break }
                items.swapAt(i, m)
                i = m
            }
        }
        return top
    }
}
