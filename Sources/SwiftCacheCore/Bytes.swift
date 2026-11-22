/// Raw byte strings are used for keys and values throughout SwiftCache.
public typealias Bytes = [UInt8]

/// Lexicographic comparison of byte strings (the key order used by range reads).
@inlinable
public func compareBytes(_ a: Bytes, _ b: Bytes) -> Int {
    let n = min(a.count, b.count)
    var i = 0
    while i < n {
        let x = a[i], y = b[i]
        if x != y { return x < y ? -1 : 1 }
        i += 1
    }
    if a.count == b.count { return 0 }
    return a.count < b.count ? -1 : 1
}

@inlinable
public func bytesLess(_ a: Bytes, _ b: Bytes) -> Bool { compareBytes(a, b) < 0 }

extension Array where Element == UInt8 {
    /// UTF-8 bytes of a string.
    @inlinable public init(utf8 string: String) { self = Array(string.utf8) }

    /// Decodes the bytes as UTF-8 (lossy).
    public var utf8String: String { String(decoding: self, as: UTF8.self) }

    /// Lowercase hex encoding.
    public var hexString: String {
        let digits = Array("0123456789abcdef".utf8)
        var out = [UInt8]()
        out.reserveCapacity(count * 2)
        for b in self {
            out.append(digits[Int(b >> 4)])
            out.append(digits[Int(b & 0xF)])
        }
        return String(decoding: out, as: UTF8.self)
    }

    /// The smallest key strictly greater than every key that has `self` as a prefix,
    /// or nil if no such key exists (prefix is all 0xFF).
    public var prefixEnd: Bytes? {
        var out = self
        while let last = out.last {
            if last < 0xFF {
                out[out.count - 1] = last + 1
                return out
            }
            out.removeLast()
        }
        return nil
    }

    /// The immediate successor of a key in lexicographic order (key + 0x00).
    public var keyAfter: Bytes { self + [0] }
}

/// Little-endian Int64 helpers used by the atomic `add` mutation.
public enum LittleEndianInt64 {
    public static func decode(_ bytes: Bytes?) -> Int64 {
        guard let bytes else { return 0 }
        var v: UInt64 = 0
        for i in 0..<min(8, bytes.count) {
            v |= UInt64(bytes[i]) << (8 * UInt64(i))
        }
        return Int64(bitPattern: v)
    }

    public static func encode(_ value: Int64) -> Bytes {
        let v = UInt64(bitPattern: value)
        return (0..<8).map { UInt8(truncatingIfNeeded: v >> (8 * UInt64($0))) }
    }
}
