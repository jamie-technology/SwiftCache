import SwiftCacheCore

/// A flat open-addressing hash table from a 64-bit key hash to a version, for the
/// resolver's "last write of each key" map: 16 bytes per entry, no per-key allocation,
/// no SipHash. Keys are already well-mixed hashes, so the slot is `hash & mask`; linear
/// probing with backward-shift deletion (no tombstones). Hash 0 marks an empty slot, so a
/// real hash of 0 is stored as 1 (a merged hash can only add spurious conflicts).
struct VersionTable {
    private var hashes: [UInt64]
    private var versions: [Version]
    private var mask: Int
    private(set) var count = 0
    private let minCapacity: Int

    init(capacity: Int = 1024) {
        var c = 16
        while c < capacity { c <<= 1 }
        minCapacity = c
        hashes = Array(repeating: 0, count: c)
        versions = Array(repeating: 0, count: c)
        mask = c - 1
    }

    var capacity: Int { hashes.count }

    @inline(__always) private static func normalize(_ h: UInt64) -> UInt64 { h == 0 ? 1 : h }

    /// The slot holding `h`, or the empty slot where it would go.
    @inline(__always) private func find(_ h: UInt64) -> Int {
        var i = Int(truncatingIfNeeded: h) & mask
        while true {
            let x = hashes[i]
            if x == h || x == 0 { return i }
            i = (i + 1) & mask
        }
    }

    func get(_ hash: UInt64) -> Version? {
        let h = Self.normalize(hash)
        let i = find(h)
        return hashes[i] == h ? versions[i] : nil
    }

    /// Sets the version for `hash` (replacing any).
    mutating func set(_ hash: UInt64, _ version: Version) {
        let h = Self.normalize(hash)
        if (count + 1) * 2 > hashes.count { resize(hashes.count * 2) }
        let i = find(h)
        if hashes[i] == 0 { count += 1; hashes[i] = h }
        versions[i] = version
    }

    /// Removes `hash` if its version is still `version` (a newer write keeps it).
    mutating func remove(_ hash: UInt64, ifVersion version: Version) {
        let h = Self.normalize(hash)
        var i = find(h)
        guard hashes[i] == h, versions[i] == version else { return }
        // Backward-shift deletion: pull later entries of the probe run into the hole.
        var j = i
        while true {
            j = (j + 1) & mask
            let x = hashes[j]
            if x == 0 { break }
            let home = Int(truncatingIfNeeded: x) & mask
            // Move x into the hole if its home is not cyclically within (i, j].
            let inRange = i <= j ? (home > i && home <= j) : (home > i || home <= j)
            if !inRange {
                hashes[i] = x
                versions[i] = versions[j]
                i = j
            }
        }
        hashes[i] = 0
        versions[i] = 0
        count -= 1
        if hashes.count > minCapacity, count * 8 < hashes.count { resize(hashes.count / 2) }
    }

    private mutating func resize(_ newCapacity: Int) {
        let oldH = hashes, oldV = versions
        hashes = Array(repeating: 0, count: newCapacity)
        versions = Array(repeating: 0, count: newCapacity)
        mask = newCapacity - 1
        for k in 0..<oldH.count where oldH[k] != 0 {
            let i = find(oldH[k])
            hashes[i] = oldH[k]
            versions[i] = oldV[k]
        }
    }

    mutating func removeAll() {
        self = VersionTable(capacity: minCapacity)
    }
}

/// A fast seeded 64-bit hash of a byte string (not cryptographic; the seed is per
/// resolver, so clients can't aim collisions).
@inline(__always)
func hash64(_ key: Bytes, seed: UInt64) -> UInt64 {
    @inline(__always) func mix(_ x: UInt64) -> UInt64 {
        var z = x
        z = (z ^ (z >> 30)) &* 0xBF58_476D_1CE4_E5B9
        z = (z ^ (z >> 27)) &* 0x94D0_49BB_1331_11EB
        return z ^ (z >> 31)
    }
    return key.withUnsafeBytes { p -> UInt64 in
        let n = p.count
        var h = mix(seed ^ (UInt64(n) &* 0x9E37_79B9_7F4A_7C15))
        var i = 0
        while i + 8 <= n {
            h = mix(h ^ p.loadUnaligned(fromByteOffset: i, as: UInt64.self))
            i += 8
        }
        if i < n {
            var tail: UInt64 = 0
            var shift: UInt64 = 0
            while i < n {
                tail |= UInt64(p[i]) << shift
                shift += 8
                i += 1
            }
            h = mix(h ^ tail)
        }
        return h
    }
}
