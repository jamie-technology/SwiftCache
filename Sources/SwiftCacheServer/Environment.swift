import SwiftCacheCore

public enum LogLevel: Int, Sendable, Comparable {
    case debug = 0, info, warning, error
    public static func < (a: LogLevel, b: LogLevel) -> Bool { a.rawValue < b.rawValue }
}

/// Everything a server node is allowed to do to the outside world. The production
/// runtime implements it with SwiftNIO; the simulator implements it with a virtual clock,
/// a seeded RNG and a simulated network. Nodes must not touch clocks, threads, sockets or
/// unseeded randomness directly — that is what keeps simulations deterministic.
public protocol NodeEnvironment: AnyObject {
    /// Monotonic time in microseconds.
    var now: UInt64 { get }
    /// Wall-clock-ish microseconds used to seed commit versions (equal to `now` in simulation).
    var versionClock: UInt64 { get }
    func send(_ message: Message, to endpoint: Endpoint)
    func schedule(after micros: UInt64, _ action: @escaping () -> Void)
    func random(_ upperBound: UInt64) -> UInt64
    func log(_ level: LogLevel, _ message: @autoclosure () -> String)
}

/// A message-driven server process.
public protocol ServerNode: AnyObject {
    var endpoint: Endpoint { get }
    func start()
    func receive(_ message: Message, from: Endpoint)
}

/// Tunables shared by the coordinator and storage servers. All durations are in
/// microseconds (which is also the unit of versions).
public struct ClusterConfig: Sendable, Hashable {
    /// Replicas per shard. A shard is formed once this many unassigned servers are available.
    public var replicationFactor = 2
    /// How far back (in versions ≈ µs) reads are guaranteed to be served.
    public var mvccWindow: UInt64 = 10_000_000
    /// How often the coordinator considers forming a batch.
    public var batchInterval: UInt64 = 1_000
    /// Maximum time between batches when idle (heartbeat / version advance).
    public var idleBatchInterval: UInt64 = 10_000
    /// Maximum unacknowledged batches in flight.
    public var maxInflightBatches = 16
    /// Maximum transactions per batch.
    public var maxTransactionsPerBatch = 2_000
    /// A storage server that hasn't acknowledged a batch for this long is marked failed.
    public var failureTimeout: UInt64 = 1_500_000
    /// How long the *last* live replica of a shard may be silent before it is declared
    /// failed (and the shard's data given up as lost). Until then, commits stall.
    public var shardLossTimeout: UInt64 = 60_000_000
    /// How long a (re)starting coordinator waits for storage servers to report.
    public var recoveryWindow: UInt64 = 2_000_000
    /// How long recovery waits for truncation acknowledgements before giving up on a server.
    public var recoveryTruncateTimeout: UInt64 = 1_500_000
    /// A storage server that hasn't heard from the coordinator for this long starts rejoining.
    public var coordinatorTimeout: UInt64 = 1_000_000
    /// Interval between join attempts.
    public var joinInterval: UInt64 = 250_000
    /// How long a storage server waits for a snapshot before retrying another source.
    public var snapshotTimeout: UInt64 = 5_000_000
    /// How long a read waits for a version to arrive before failing with future_version.
    public var readWaitTimeout: UInt64 = 1_000_000
    /// Slots moved per data-movement step when rebalancing.
    public var migrationChunkSlots = 2_048
    /// A data movement that hasn't completed in this long is aborted.
    public var moveTimeout: UInt64 = 30_000_000
    /// Keys per snapshot chunk message.
    public var snapshotChunkKeys = 1_000
    /// Limits.
    public var maxKeySize = 10_000
    public var maxValueSize = 1_000_000
    public var maxTransactionBytes = 10_000_000

    public init() {}
}
