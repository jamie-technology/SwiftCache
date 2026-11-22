/// A tiny, deterministic binary encoder. All integers are little-endian; byte strings
/// and arrays are prefixed with a UInt32 length.
public struct WireWriter: Sendable {
    public private(set) var bytes: Bytes = []

    public init(capacity: Int = 64) { bytes.reserveCapacity(capacity) }

    public mutating func u8(_ v: UInt8) { bytes.append(v) }
    public mutating func bool(_ v: Bool) { bytes.append(v ? 1 : 0) }
    public mutating func u16(_ v: UInt16) { withUnsafeBytes(of: v.littleEndian) { bytes.append(contentsOf: $0) } }
    public mutating func u32(_ v: UInt32) { withUnsafeBytes(of: v.littleEndian) { bytes.append(contentsOf: $0) } }
    public mutating func u64(_ v: UInt64) { withUnsafeBytes(of: v.littleEndian) { bytes.append(contentsOf: $0) } }
    public mutating func i32(_ v: Int32) { u32(UInt32(bitPattern: v)) }
    public mutating func i64(_ v: Int64) { u64(UInt64(bitPattern: v)) }
    public mutating func blob(_ v: Bytes) { u32(UInt32(v.count)); bytes.append(contentsOf: v) }
    public mutating func string(_ v: String) { blob(Bytes(v.utf8)) }
    public mutating func optionalBlob(_ v: Bytes?) {
        if let v { bool(true); blob(v) } else { bool(false) }
    }
    public mutating func stamp(_ v: Versionstamp) { u64(v.version); u16(v.order) }
    public mutating func array<T>(_ items: [T], _ each: (inout WireWriter, T) -> Void) {
        u32(UInt32(items.count))
        for item in items { each(&self, item) }
    }
    public mutating func slots(_ s: SlotSet) {
        let r = s.ranges
        u32(UInt32(r.count))
        for (a, b) in r { u16(UInt16(a)); u16(UInt16(b - a)) }
    }
}

public struct WireError: Error, CustomStringConvertible, Sendable {
    public var description: String
    public init(_ d: String) { description = d }
}

public struct WireReader {
    let bytes: Bytes
    var offset: Int

    public init(_ bytes: Bytes) { self.bytes = bytes; self.offset = 0 }

    public var isAtEnd: Bool { offset >= bytes.count }

    mutating func need(_ n: Int) throws {
        if n < 0 || offset + n > bytes.count { throw WireError("truncated message (need \(n) at \(offset) of \(bytes.count))") }
    }

    public mutating func u8() throws -> UInt8 { try need(1); defer { offset += 1 }; return bytes[offset] }
    public mutating func bool() throws -> Bool { try u8() != 0 }
    public mutating func u16() throws -> UInt16 {
        try need(2); defer { offset += 2 }
        return UInt16(bytes[offset]) | UInt16(bytes[offset + 1]) << 8
    }
    public mutating func u32() throws -> UInt32 {
        try need(4); defer { offset += 4 }
        var v: UInt32 = 0
        for i in 0..<4 { v |= UInt32(bytes[offset + i]) << (8 * UInt32(i)) }
        return v
    }
    public mutating func u64() throws -> UInt64 {
        try need(8); defer { offset += 8 }
        var v: UInt64 = 0
        for i in 0..<8 { v |= UInt64(bytes[offset + i]) << (8 * UInt64(i)) }
        return v
    }
    public mutating func i32() throws -> Int32 { Int32(bitPattern: try u32()) }
    public mutating func i64() throws -> Int64 { Int64(bitPattern: try u64()) }
    public mutating func blob() throws -> Bytes {
        let n = Int(try u32())
        try need(n)
        defer { offset += n }
        return Array(bytes[offset..<(offset + n)])
    }
    public mutating func string() throws -> String { try blob().utf8String }
    public mutating func optionalBlob() throws -> Bytes? { try bool() ? try blob() : nil }
    public mutating func stamp() throws -> Versionstamp { Versionstamp(version: try u64(), order: try u16()) }
    public mutating func array<T>(_ each: (inout WireReader) throws -> T) throws -> [T] {
        let n = Int(try u32())
        guard n <= bytes.count - offset else { throw WireError("array length \(n) exceeds message") }
        var out = [T]()
        out.reserveCapacity(n)
        for _ in 0..<n { out.append(try each(&self)) }
        return out
    }
    public mutating func slots() throws -> SlotSet {
        let n = Int(try u32())
        var ranges = [(start: Int, end: Int)]()
        for _ in 0..<n {
            let a = Int(try u16()), len = Int(try u16())
            ranges.append((a, a + len))
        }
        return SlotSet(ranges: ranges)
    }
}

/// A type with a hand-written binary encoding (deterministic, compact, fast).
public protocol WireCodable: Sendable {
    func encode(to w: inout WireWriter)
    init(from r: inout WireReader) throws
}

extension WireWriter {
    public mutating func put<T: WireCodable>(_ v: T) { v.encode(to: &self) }
    public mutating func put<T: WireCodable>(_ v: T?) {
        if let v { bool(true); v.encode(to: &self) } else { bool(false) }
    }
    public mutating func put<T: WireCodable>(_ vs: [T]) {
        u32(UInt32(vs.count))
        for v in vs { v.encode(to: &self) }
    }
    public mutating func strings(_ vs: [String]) { array(vs) { $0.string($1) } }
    public mutating func blobs(_ vs: [Bytes]) { array(vs) { $0.blob($1) } }
    public mutating func u32s(_ vs: [UInt32]) { array(vs) { $0.u32($1) } }
    public mutating func optionalString(_ v: String?) { if let v { bool(true); string(v) } else { bool(false) } }
}

extension WireReader {
    public mutating func get<T: WireCodable>(_: T.Type = T.self) throws -> T { try T(from: &self) }
    public mutating func getOptional<T: WireCodable>(_: T.Type = T.self) throws -> T? { try bool() ? try T(from: &self) : nil }
    public mutating func getArray<T: WireCodable>(_: T.Type = T.self) throws -> [T] { try array { try T(from: &$0) } }
    public mutating func strings() throws -> [String] { try array { try $0.string() } }
    public mutating func blobs() throws -> [Bytes] { try array { try $0.blob() } }
    public mutating func u32s() throws -> [UInt32] { try array { try $0.u32() } }
    public mutating func optionalString() throws -> String? { try bool() ? try string() : nil }
}

extension Versionstamp: WireCodable {
    public func encode(to w: inout WireWriter) { w.stamp(self) }
    public init(from r: inout WireReader) throws { self = try r.stamp() }
}

extension SlotSet: WireCodable {
    public func encode(to w: inout WireWriter) { w.slots(self) }
    public init(from r: inout WireReader) throws { self = try r.slots() }
}
