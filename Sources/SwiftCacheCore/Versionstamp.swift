/// A commit version: a cluster-wide, monotonically increasing 64-bit number assigned
/// by the coordinator. Versions advance at roughly one million per second (they track
/// microseconds), so a version difference can be read as elapsed time.
public typealias Version = UInt64

/// A 10-byte, totally ordered identifier for a committed write, modelled on
/// FoundationDB versionstamps:
///
/// ```
/// | 8 bytes: commit version (big-endian) | 2 bytes: order within the commit batch (big-endian) |
/// ```
///
/// Every transaction that commits writes is assigned exactly one versionstamp, and every
/// key it writes records that versionstamp. Comparing versionstamps compares commit order.
public struct Versionstamp: Hashable, Comparable, Sendable, CustomStringConvertible, Codable {
    public var version: Version
    public var order: UInt16

    public init(version: Version, order: UInt16) {
        self.version = version
        self.order = order
    }

    /// The smallest versionstamp. Never assigned to a commit.
    public static let zero = Versionstamp(version: 0, order: 0)

    public static func < (lhs: Versionstamp, rhs: Versionstamp) -> Bool {
        (lhs.version, lhs.order) < (rhs.version, rhs.order)
    }

    /// The canonical 10-byte big-endian encoding (sorts the same as the versionstamp).
    public var bytes: Bytes {
        var out = Bytes()
        out.reserveCapacity(10)
        for shift in stride(from: 56, through: 0, by: -8) {
            out.append(UInt8(truncatingIfNeeded: version >> UInt64(shift)))
        }
        out.append(UInt8(truncatingIfNeeded: order >> 8))
        out.append(UInt8(truncatingIfNeeded: order))
        return out
    }

    public init?(bytes: Bytes) {
        guard bytes.count == 10 else { return nil }
        var v: UInt64 = 0
        for i in 0..<8 { v = (v << 8) | UInt64(bytes[i]) }
        self.version = v
        self.order = (UInt16(bytes[8]) << 8) | UInt16(bytes[9])
    }

    /// 20 hex characters, e.g. `0000018c2f4e1a2b0003`.
    public var description: String { bytes.hexString }

    public init?(hex: String) {
        let chars = Array(hex.utf8)
        guard chars.count == 20 else { return nil }
        var out = Bytes()
        var i = 0
        func nibble(_ c: UInt8) -> UInt8? {
            switch c {
            case UInt8(ascii: "0")...UInt8(ascii: "9"): return c - UInt8(ascii: "0")
            case UInt8(ascii: "a")...UInt8(ascii: "f"): return c - UInt8(ascii: "a") + 10
            case UInt8(ascii: "A")...UInt8(ascii: "F"): return c - UInt8(ascii: "A") + 10
            default: return nil
            }
        }
        while i < 20 {
            guard let hi = nibble(chars[i]), let lo = nibble(chars[i + 1]) else { return nil }
            out.append(hi << 4 | lo)
            i += 2
        }
        self.init(bytes: out)
    }

    // Codable as its hex string so it round-trips through JSON cleanly.
    public init(from decoder: any Decoder) throws {
        let s = try decoder.singleValueContainer().decode(String.self)
        guard let v = Versionstamp(hex: s) else {
            throw DecodingError.dataCorrupted(.init(codingPath: decoder.codingPath, debugDescription: "invalid versionstamp \(s)"))
        }
        self = v
    }

    public func encode(to encoder: any Encoder) throws {
        var c = encoder.singleValueContainer()
        try c.encode(description)
    }
}
