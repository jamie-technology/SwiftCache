import SwiftCacheCore

/// Errors surfaced by the SwiftCache client.
public enum SwiftCacheError: Error, Sendable, Equatable, CustomStringConvertible {
    /// The transaction conflicted with a concurrently committed transaction.
    /// Retried automatically by `Database.transaction`.
    case notCommitted
    /// The transaction's read version fell out of the MVCC window (or predates a
    /// transaction-system recovery). Retried automatically unless the version was pinned.
    case transactionTooOld
    /// A storage server hadn't caught up to the read version in time. Retried automatically.
    case futureVersion
    /// The commit was sent but no reply arrived; it may or may not have been applied.
    /// Retried automatically, so transaction bodies should be idempotent.
    case commitUnknownResult
    /// The cluster is recovering, has no storage yet, or a shard has no live replica.
    case unavailable(String)
    /// `compareAndSwap` found a different versionstamp than expected (nil = key absent).
    case versionMismatch(key: Bytes, expected: Versionstamp?, actual: Versionstamp?)
    /// A key, value or transaction exceeded the cluster's size limits.
    case tooLarge
    /// Invalid use of the API.
    case invalidRequest(String)
    /// The value couldn't be decoded as the requested type.
    case decodingFailed(String)
    /// A retry loop gave up.
    case retryLimitExceeded(last: String)
    case internalError(String)

    /// Whether `Database.transaction` retries after this error.
    public var isRetryable: Bool {
        switch self {
        case .notCommitted, .transactionTooOld, .futureVersion, .commitUnknownResult, .unavailable: true
        default: false
        }
    }

    public var description: String {
        switch self {
        case .notCommitted: "transaction conflicted with another transaction (not_committed)"
        case .transactionTooOld: "read version is too old (transaction_too_old)"
        case .futureVersion: "storage has not caught up to the read version (future_version)"
        case .commitUnknownResult: "commit result unknown"
        case .unavailable(let why): "cluster unavailable: \(why)"
        case .versionMismatch(let key, let e, let a):
            "version mismatch on \(key.utf8String): expected \(e?.description ?? "absent"), found \(a?.description ?? "absent")"
        case .tooLarge: "key, value or transaction too large"
        case .invalidRequest(let why): "invalid request: \(why)"
        case .decodingFailed(let why): "decoding failed: \(why)"
        case .retryLimitExceeded(let last): "retry limit exceeded (last error: \(last))"
        case .internalError(let why): "internal error: \(why)"
        }
    }

    init(code: ErrorCode) {
        switch code {
        case .notCommitted: self = .notCommitted
        case .transactionTooOld: self = .transactionTooOld
        case .futureVersion: self = .futureVersion
        case .wrongShard: self = .unavailable("stale topology")
        case .unavailable: self = .unavailable("cluster unavailable")
        case .invalidRequest: self = .invalidRequest("rejected by server")
        case .tooLarge: self = .tooLarge
        case .internalError: self = .internalError("server error")
        case .staleGeneration: self = .unavailable("transaction system changed generation")
        }
    }
}
