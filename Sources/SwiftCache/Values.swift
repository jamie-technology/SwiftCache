import Foundation
import SwiftCacheCore

@_exported import struct SwiftCacheCore.Versionstamp
@_exported import struct SwiftCacheCore.KeyRange
@_exported import struct SwiftCacheCore.Topology
@_exported import struct SwiftCacheCore.ClusterStatus
@_exported import struct SwiftCacheCore.StorageStats
@_exported import typealias SwiftCacheCore.Version
@_exported import typealias SwiftCacheCore.Bytes
@_exported import protocol SwiftCacheCore.ClientTransport

/// Anything usable as a key: `String` (UTF-8), `[UInt8]`, `Substring`, `Data`.
public protocol CacheKey: Sendable {
    var cacheKeyBytes: Bytes { get }
}

extension String: CacheKey { public var cacheKeyBytes: Bytes { Bytes(utf8) } }
extension Substring: CacheKey { public var cacheKeyBytes: Bytes { Bytes(utf8) } }
extension Array: CacheKey where Element == UInt8 { public var cacheKeyBytes: Bytes { self } }
extension Data: CacheKey { public var cacheKeyBytes: Bytes { Bytes(self) } }

/// Value encoding: `String` is stored as UTF-8, `[UInt8]`/`Data` as raw bytes, and any
/// other `Encodable` as JSON.
enum ValueCodec {
    static func encode<T: Encodable>(_ value: T) throws -> Bytes {
        switch value {
        case let s as String: return Bytes(s.utf8)
        case let b as [UInt8]: return b
        case let d as Data: return Bytes(d)
        default:
            do {
                let encoder = JSONEncoder()
                encoder.outputFormatting = [.sortedKeys]
                return Bytes(try encoder.encode(value))
            } catch {
                throw SwiftCacheError.invalidRequest("could not encode value: \(error)")
            }
        }
    }

    static func decode<T: Decodable>(_ type: T.Type, from bytes: Bytes) throws -> T {
        if T.self == String.self { return String(decoding: bytes, as: UTF8.self) as! T }
        if T.self == [UInt8].self { return bytes as! T }
        if T.self == Data.self { return Data(bytes) as! T }
        do {
            return try JSONDecoder().decode(T.self, from: Data(bytes))
        } catch {
            throw SwiftCacheError.decodingFailed("\(T.self): \(error)")
        }
    }
}

/// A value together with the versionstamp of the transaction that wrote it.
public struct VersionedEntry: Sendable, Hashable {
    public var key: Bytes
    public var bytes: Bytes
    /// The versionstamp of the commit that last wrote this key. Pass it to
    /// `compareAndSwap(key:expectedVersion:newValue:)` for optimistic updates.
    public var versionstamp: Versionstamp

    public init(key: Bytes, bytes: Bytes, versionstamp: Versionstamp) {
        self.key = key
        self.bytes = bytes
        self.versionstamp = versionstamp
    }

    /// The key as a UTF-8 string.
    public var keyString: String { key.utf8String }
    /// The value as a UTF-8 string.
    public var string: String { bytes.utf8String }
    /// The value decoded as `T` (JSON unless `T` is `String`, `[UInt8]` or `Data`).
    public func decode<T: Decodable>(_ type: T.Type = T.self) throws -> T {
        try ValueCodec.decode(type, from: bytes)
    }
    /// The value as a little-endian Int64 (the format used by `add`).
    public var int64: Int64 { LittleEndianInt64.decode(bytes) }
}

/// Tunables for the client.
public struct DatabaseOptions: Sendable {
    /// Timeout for a single request (µs).
    public var requestTimeout: UInt64 = 2_000_000
    /// Maximum attempts of a transaction retry loop.
    public var maxRetries = 100
    /// Initial retry backoff (µs); doubles per attempt with jitter.
    public var initialBackoff: UInt64 = 5_000
    /// Maximum retry backoff (µs).
    public var maxBackoff: UInt64 = 500_000
    /// Seconds an idempotent operation's commit record is kept (bounds how long after an
    /// unknown result a retry can still be deduplicated).
    public var idempotencyRetention: UInt32 = 600

    public init() {}
}
