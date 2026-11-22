import SwiftCacheCore

/// Optimistic concurrency control, FoundationDB style.
///
/// A transaction reads at `readVersion` and asks to commit at a later version. It
/// conflicts if any transaction that committed *after* its read version wrote a key in its
/// read set (point keys or ranges). Transactions in one batch are resolved in order, so a
/// transaction also conflicts with earlier transactions of the same batch.
struct Resolver {
    /// Last committed write version of each key written within the MVCC window.
    private var lastWrite: [Bytes: Version] = [:]
    /// Recent commits (version, sorted written keys), oldest first; used for range checks.
    private var recent = Deque<(version: Version, keys: [Bytes])>()
    /// Reads older than this are rejected (window edge or recovery point).
    private(set) var oldestReadable: Version = 0
    private var lastSweep: Version = 0

    enum Outcome: Equatable {
        case commit
        case conflict
        case tooOld
    }

    /// Decides one transaction and, if it commits, records its writes at `commitVersion`.
    mutating func resolve(_ req: CommitRequest, commitVersion: Version) -> Outcome {
        if req.readVersion < oldestReadable { return .tooOld }
        for key in req.readConflictKeys {
            if let w = lastWrite[key], w > req.readVersion { return .conflict }
        }
        if !req.readConflictRanges.isEmpty {
            // Walk commits newer than the read version (newest first).
            for entry in recent.elements.reversed() {
                if entry.version <= req.readVersion { break }
                for range in req.readConflictRanges where Self.intersects(entry.keys, range) {
                    return .conflict
                }
            }
        }
        record(req.writeConflictKeys, at: commitVersion)
        return .commit
    }

    private mutating func record(_ keys: [Bytes], at version: Version) {
        guard !keys.isEmpty else { return }
        for key in keys { lastWrite[key] = version }
        recent.append((version, keys.sorted(by: bytesLess)))
    }

    /// Whether any key of the sorted array falls in `range`.
    private static func intersects(_ sorted: [Bytes], _ range: KeyRange) -> Bool {
        var lo = 0, hi = sorted.count
        while lo < hi {
            let mid = (lo + hi) / 2
            if bytesLess(sorted[mid], range.begin) { lo = mid + 1 } else { hi = mid }
        }
        return lo < sorted.count && bytesLess(sorted[lo], range.end)
    }

    /// Moves the window forward: reads older than `oldest` are rejected from now on.
    mutating func advance(oldest: Version) {
        guard oldest > oldestReadable else { return }
        oldestReadable = oldest
        while let first = recent.first, first.version < oldest { recent.removeFirst() }
        // Sweeping the point map is O(n); do it at most once per window.
        if oldest > lastSweep &+ 1_000_000 {
            lastWrite = lastWrite.filter { $0.value >= oldest }
            lastSweep = oldest
        }
    }

    /// Forgets all history; reads older than `version` are rejected (coordinator recovery).
    mutating func reset(rejectingReadsBefore version: Version) {
        lastWrite = [:]
        recent = Deque()
        oldestReadable = version
        lastSweep = version
    }

    var trackedKeys: Int { lastWrite.count }
}
