import SwiftCacheCore

/// A sequence of single-key operations sent together, like a Redis pipeline: one call,
/// far fewer round trips and task switches than issuing them one by one.
///
/// Semantics (stronger than Redis's):
/// - Operations take effect in pipeline order: each sees every earlier operation of the
///   pipeline (a `get` after a `set` of the same key returns the new value).
/// - A run of consecutive writes commits as one idempotent, atomic transaction (chunks of
///   up to `maxWritesPerCommit`), so it is applied exactly once even across retries.
/// - A run of consecutive reads is served from one consistent snapshot, taken with a read
///   version requested after every earlier operation of the pipeline committed.
/// Everything stays strictly serializable: each read sees every commit acknowledged
/// before its run started.
public struct Pipeline: Sendable {
    enum Op: Sendable {
        case get(Bytes)
        case set(Bytes, Bytes, ttl: Double?)
        case clear(Bytes)
        case add(Bytes, Int64)

        var isRead: Bool { if case .get = self { return true } else { return false } }
    }
    var ops: [Op] = []

    /// Writes per commit within a run of writes.
    public static let maxWritesPerCommit = 1_000

    public init() {}

    public var count: Int { ops.count }
    public var isEmpty: Bool { ops.isEmpty }

    public mutating func get(_ key: some CacheKey) { ops.append(.get(key.cacheKeyBytes)) }
    public mutating func set(_ key: some CacheKey, bytes: Bytes, ttl: Double? = nil) { ops.append(.set(key.cacheKeyBytes, bytes, ttl: ttl)) }
    public mutating func set<T: Encodable>(_ key: some CacheKey, _ value: T, ttl: Double? = nil) throws {
        ops.append(.set(key.cacheKeyBytes, try ValueCodec.encode(value), ttl: ttl))
    }
    public mutating func clear(_ key: some CacheKey) { ops.append(.clear(key.cacheKeyBytes)) }
    public mutating func add(_ key: some CacheKey, _ delta: Int64) { ops.append(.add(key.cacheKeyBytes, delta)) }
}

/// The result of one pipeline operation.
public enum PipelineResult: Sendable, Hashable {
    /// A `get`: the entry, or nil if the key doesn't exist.
    case read(VersionedEntry?)
    /// A write (`set`, `clear`, `add`): the versionstamp of the commit that applied it.
    case written(Versionstamp)

    /// The value read (nil for writes and missing keys).
    public var bytes: Bytes? { if case .read(let e) = self { return e?.bytes } else { return nil } }
    public var entry: VersionedEntry? { if case .read(let e) = self { return e } else { return nil } }
    public var versionstamp: Versionstamp? {
        switch self {
        case .read(let e): return e?.versionstamp
        case .written(let v): return v
        }
    }
    public func value<T: Decodable>(as type: T.Type) throws -> T? {
        guard let b = bytes else { return nil }
        return try ValueCodec.decode(type, from: b)
    }
}

extension Database {
    /// Builds and runs a pipeline; results are in operation order.
    public func pipeline(_ build: (inout Pipeline) throws -> Void) async throws -> [PipelineResult] {
        var p = Pipeline()
        try build(&p)
        return try await execute(p)
    }

    /// Runs a pipeline; results are in operation order.
    public func execute(_ pipeline: Pipeline) async throws -> [PipelineResult] {
        var results: [PipelineResult] = []
        results.reserveCapacity(pipeline.count)
        let ops = pipeline.ops
        var i = 0
        while i < ops.count {
            var j = i
            if ops[i].isRead {
                while j < ops.count, ops[j].isRead { j += 1 }
                results += try await readRun(ops[i..<j])
            } else {
                while j < ops.count, !ops[j].isRead, j - i < Pipeline.maxWritesPerCommit { j += 1 }
                let chunk = ops[i..<j]
                let (_, stamp) = try await transactionWithVersionstamp(idempotent: true) { tr in
                    for op in chunk {
                        switch op {
                        case .set(let k, let v, let ttl): tr.set(k, bytes: v, ttl: ttl)
                        case .clear(let k): tr.clear(k)
                        case .add(let k, let d): tr.add(k, d)
                        case .get: break
                        }
                    }
                }
                results += Array(repeating: .written(stamp ?? .zero), count: chunk.count)
            }
            i = j
        }
        return results
    }

    /// One snapshot for a run of gets: a fresh read version, then every key at it.
    private func readRun(_ ops: ArraySlice<Pipeline.Op>) async throws -> [PipelineResult] {
        let keys = ops.map { op -> Bytes in if case .get(let k) = op { return k } else { return [] } }
        var tries = 0
        var backoff = options.initialBackoff
        while true {
            do {
                let version = try await client.readVersion()
                let values = try await client.readMany(keys, at: version)
                return zip(keys, values).map { key, v in
                    .read(v.0.map { VersionedEntry(key: key, bytes: $0, versionstamp: v.1) })
                }
            } catch let error as SwiftCacheError where error.isRetryable {
                tries += 1
                if tries >= options.maxRetries { throw SwiftCacheError.retryLimitExceeded(last: error.description) }
                let jitter = client.transport.random(backoff / 2 + 1)
                try await client.transport.sleep(micros: backoff / 2 + jitter)
                backoff = min(options.maxBackoff, backoff * 2)
            }
        }
    }
}
