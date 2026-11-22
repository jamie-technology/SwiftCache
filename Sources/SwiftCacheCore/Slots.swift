/// Keys are partitioned into a fixed number of hash slots (like Redis Cluster). Each slot
/// is owned by exactly one shard; adding shards moves slots (and their data) onto them.
public enum Slots {
    public static let count = 16384

    /// CRC16/XMODEM, as used by Redis Cluster.
    @inlinable
    public static func crc16(_ bytes: some Sequence<UInt8>) -> UInt16 {
        var crc: UInt16 = 0
        for b in bytes {
            crc ^= UInt16(b) << 8
            for _ in 0..<8 {
                crc = (crc & 0x8000) != 0 ? (crc << 1) ^ 0x1021 : crc << 1
            }
        }
        return crc
    }

    /// The slot for a key. Supports Redis-style hash tags: if the key contains `{tag}`
    /// with a non-empty tag, only the tag is hashed, so `{user:1}:profile` and
    /// `{user:1}:sessions` always live on the same shard.
    public static func slot(for key: Bytes) -> Int {
        if let open = key.firstIndex(of: UInt8(ascii: "{")),
           let close = key[(open + 1)...].firstIndex(of: UInt8(ascii: "}")),
           close > open + 1 {
            return Int(crc16(key[(open + 1)..<close])) % count
        }
        return Int(crc16(key)) % count
    }
}

/// A set of slots, stored as a 16384-bit bitmap.
public struct SlotSet: Hashable, Sendable, CustomStringConvertible {
    @usableFromInline var words: [UInt64]

    public init() { words = Array(repeating: 0, count: Slots.count / 64) }

    public static var all: SlotSet {
        var s = SlotSet()
        for i in 0..<s.words.count { s.words[i] = .max }
        return s
    }

    public init(_ slots: some Sequence<Int>) {
        self.init()
        for s in slots { insert(s) }
    }

    @inlinable public func contains(_ slot: Int) -> Bool {
        words[slot >> 6] & (1 << UInt64(slot & 63)) != 0
    }

    @inlinable public mutating func insert(_ slot: Int) {
        words[slot >> 6] |= (1 << UInt64(slot & 63))
    }

    @inlinable public mutating func remove(_ slot: Int) {
        words[slot >> 6] &= ~(1 << UInt64(slot & 63))
    }

    public var count: Int { words.reduce(0) { $0 + $1.nonzeroBitCount } }
    public var isEmpty: Bool { words.allSatisfy { $0 == 0 } }

    public func union(_ other: SlotSet) -> SlotSet {
        var r = self
        for i in 0..<words.count { r.words[i] |= other.words[i] }
        return r
    }

    public func intersection(_ other: SlotSet) -> SlotSet {
        var r = self
        for i in 0..<words.count { r.words[i] &= other.words[i] }
        return r
    }

    public func subtracting(_ other: SlotSet) -> SlotSet {
        var r = self
        for i in 0..<words.count { r.words[i] &= ~other.words[i] }
        return r
    }

    public mutating func formUnion(_ other: SlotSet) { self = union(other) }
    public mutating func subtract(_ other: SlotSet) { self = subtracting(other) }

    public func isDisjoint(with other: SlotSet) -> Bool {
        for i in 0..<words.count where words[i] & other.words[i] != 0 { return false }
        return true
    }

    /// Slots in ascending order.
    public var sorted: [Int] {
        var out = [Int]()
        for (wi, w) in words.enumerated() where w != 0 {
            var w = w
            while w != 0 {
                let bit = w.trailingZeroBitCount
                out.append(wi * 64 + bit)
                w &= w - 1
            }
        }
        return out
    }

    /// Run-length ranges `[start, end)` — the wire encoding.
    public var ranges: [(start: Int, end: Int)] {
        var out = [(Int, Int)]()
        var start = -1
        for s in 0...Slots.count {
            let has = s < Slots.count && contains(s)
            if has && start < 0 { start = s }
            if !has && start >= 0 { out.append((start, s)); start = -1 }
        }
        return out
    }

    public init(ranges: [(start: Int, end: Int)]) {
        self.init()
        for r in ranges where r.start < r.end {
            for s in max(0, r.start)..<min(Slots.count, r.end) { insert(s) }
        }
    }

    public var description: String {
        let r = ranges
        if r.isEmpty { return "{}" }
        return "{" + r.prefix(6).map { $0.end - $0.start == 1 ? "\($0.start)" : "\($0.start)-\($0.end - 1)" }.joined(separator: ",") + (r.count > 6 ? ",…" : "") + "} (\(count))"
    }
}
