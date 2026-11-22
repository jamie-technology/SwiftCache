import SwiftCacheCore

/// An ordered set of byte-string keys, stored as a list of sorted chunks
/// (in the style of Python's `sortedcontainers`). Inserts and removals are
/// O(log n + chunk), and range scans are sequential.
struct SortedKeyIndex {
    private var chunks: [[Bytes]] = []
    private(set) var count = 0
    private let load = 256

    /// Index of the first chunk whose last key is >= key (or the last chunk).
    private func chunkIndex(for key: Bytes) -> Int {
        var lo = 0, hi = chunks.count - 1
        while lo < hi {
            let mid = (lo + hi) / 2
            if bytesLess(chunks[mid].last!, key) { lo = mid + 1 } else { hi = mid }
        }
        return lo
    }

    private static func lowerBound(_ arr: [Bytes], _ key: Bytes) -> Int {
        var lo = 0, hi = arr.count
        while lo < hi {
            let mid = (lo + hi) / 2
            if bytesLess(arr[mid], key) { lo = mid + 1 } else { hi = mid }
        }
        return lo
    }

    /// Inserts `key`; returns false if it was already present.
    @discardableResult
    mutating func insert(_ key: Bytes) -> Bool {
        if chunks.isEmpty {
            chunks = [[key]]
            count = 1
            return true
        }
        let ci = chunkIndex(for: key)
        let pos = Self.lowerBound(chunks[ci], key)
        if pos < chunks[ci].count && chunks[ci][pos] == key { return false }
        chunks[ci].insert(key, at: pos)
        count += 1
        if chunks[ci].count > load * 2 {
            let half = chunks[ci].count / 2
            let right = Array(chunks[ci][half...])
            chunks[ci].removeLast(chunks[ci].count - half)
            chunks.insert(right, at: ci + 1)
        }
        return true
    }

    /// Removes `key`; returns false if it was absent.
    @discardableResult
    mutating func remove(_ key: Bytes) -> Bool {
        guard !chunks.isEmpty else { return false }
        let ci = chunkIndex(for: key)
        let pos = Self.lowerBound(chunks[ci], key)
        guard pos < chunks[ci].count && chunks[ci][pos] == key else { return false }
        chunks[ci].remove(at: pos)
        count -= 1
        if chunks[ci].isEmpty { chunks.remove(at: ci) }
        return true
    }

    mutating func removeAll() {
        chunks = []
        count = 0
    }

    /// Calls `body` with keys in `[begin, end)` in ascending order until it returns false.
    func forEach(from begin: Bytes, to end: Bytes, _ body: (Bytes) -> Bool) {
        guard !chunks.isEmpty else { return }
        var ci = chunkIndex(for: begin)
        var pos = Self.lowerBound(chunks[ci], begin)
        while ci < chunks.count {
            let chunk = chunks[ci]
            while pos < chunk.count {
                let k = chunk[pos]
                if !bytesLess(k, end) { return }
                if !body(k) { return }
                pos += 1
            }
            ci += 1
            pos = 0
        }
    }
}
