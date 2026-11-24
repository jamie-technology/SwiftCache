import SwiftCacheCore

public enum LogLevel: Int, Sendable, Comparable {
    case debug = 0, info, warning, error
    public static func < (a: LogLevel, b: LogLevel) -> Bool { a.rawValue < b.rawValue }
}

/// Small, synchronous, durable key/value storage for a process (coordinator state).
/// Production writes files with fsync; the simulator keeps it across process restarts.
public protocol Disk: AnyObject {
    func read(_ key: String) -> Bytes?
    func write(_ key: String, _ value: Bytes)
}

/// Everything a process is allowed to do to the outside world. The production runtime
/// implements it with SwiftNIO; the simulator with a virtual clock, a seeded RNG and a
/// simulated network. Roles never touch clocks, threads, sockets or unseeded randomness
/// directly — that is what keeps simulations deterministic.
public protocol ProcessEnvironment: AnyObject {
    /// This process's network address (`host:port`).
    var address: Endpoint { get }
    /// Monotonic time in microseconds.
    var now: UInt64 { get }
    /// Wall-clock-ish microseconds used to seed commit versions.
    var versionClock: UInt64 { get }
    var disk: any Disk { get }
    func send(_ message: Message, from: Endpoint, to: Endpoint)
    /// Sends the same message to several endpoints (a runtime may encode it once).
    func send(_ message: Message, from: Endpoint, toAll: [Endpoint])
    func schedule(after micros: UInt64, _ action: @escaping () -> Void)
    func random(_ upperBound: UInt64) -> UInt64
    func log(_ level: LogLevel, _ message: @autoclosure () -> String)
}

extension ProcessEnvironment {
    public func send(_ message: Message, from: Endpoint, toAll: [Endpoint]) {
        for to in toAll { send(message, from: from, to: to) }
    }
}

/// A process as seen by its runtime: it receives every message addressed to any of its
/// role endpoints.
public protocol ProcessNode: AnyObject {
    func start()
    func receive(_ message: Message, from: Endpoint, to: Endpoint)
}

/// Tunables. All durations are in microseconds (also the unit of versions).
public struct ClusterConfig: Sendable, Hashable {
    /// Storage replicas per shard.
    public var replicationFactor = 2
    /// Transaction logs per generation (a commit needs all of them).
    public var logReplicas = 3
    public var commitProxies = 2
    public var resolvers = 1
    /// How far back (in versions ≈ µs) reads are guaranteed to be served.
    public var mvccWindow: UInt64 = 10_000_000

    // Commit pipeline.
    public var batchInterval: UInt64 = 1_000
    public var idleBatchInterval: UInt64 = 10_000
    /// Batches a proxy has pushed that aren't yet committed. One is best measured: while
    /// the logs and master work on a batch, arrivals accumulate into the next one, so
    /// messages (and syscalls) per transaction drop. Locally at 32 clients, server CPU per
    /// set fell 51 -> 35 µs with sets +12%; at 256 clients sets went 170k -> 199k/s.
    public var maxInflightBatches = 1
    public var maxTransactionsPerBatch = 2_000

    // Coordination and failure detection.
    /// Coordinators keep a nominee this long without a heartbeat.
    public var leaseTimeout: UInt64 = 1_000_000
    public var leaderHeartbeatInterval: UInt64 = 150_000
    /// A leader that hasn't had a majority of heartbeat acks for this long steps down
    /// (must be well under `leaseTimeout`).
    public var leaderStepDownTimeout: UInt64 = 600_000
    public var registrationInterval: UInt64 = 200_000
    /// A process that hasn't registered for this long is considered dead.
    public var failureTimeout: UInt64 = 1_500_000
    /// How long the *last* replica of a shard may be gone before its data is given up.
    public var shardLossTimeout: UInt64 = 60_000_000
    /// A new controller waits this long for registrations before its first recovery.
    public var controllerWarmup: UInt64 = 1_000_000
    /// A recovery that hasn't completed in this long is abandoned and retried.
    public var recoveryTimeout: UInt64 = 6_000_000
    /// Default timeout for role-to-role requests.
    public var requestTimeout: UInt64 = 1_000_000

    /// Read-version lease the master holds on every log. A recovery lock waits out the
    /// lease, so a superseded master can never hand out a stale read version.
    public var readVersionLease: UInt64 = 300_000
    public var leaseRenewInterval: UInt64 = 75_000

    // Storage.
    /// A replica with no new data still advances at least this often (versions ≈ µs), so
    /// "latest" reads on idle shards are rarely behind the read version.
    public var replicaFreshness: UInt64 = 1_000
    /// How long the master waits, after the committed version advances, for a commit batch
    /// to promise to carry it to the logs on its push before notifying the logs itself.
    /// Under load the next batch starts within microseconds, so the notices (3 messages per
    /// batch) are skipped; at idle they go out after this delay. 0 = always notify at once.
    public var committedNoticeDelay: UInt64 = 50
    public var peekTimeout: UInt64 = 200_000
    public var popInterval: UInt64 = 100_000
    public var snapshotTimeout: UInt64 = 5_000_000
    public var readWaitTimeout: UInt64 = 1_000_000
    public var migrationChunkSlots = 2_048
    public var moveTimeout: UInt64 = 30_000_000
    public var snapshotChunkKeys = 1_000

    /// Halt transaction roles left over from previous generations (debug switch).
    public var haltStrayRoles = true

    // Limits.
    public var maxKeySize = 10_000
    public var maxValueSize = 1_000_000
    public var maxTransactionBytes = 10_000_000

    public init() {}
}

// MARK: - Roles

/// A role's view of its process: its own endpoint, timers that die with the role, and a
/// request/reply helper. Each role is an independent state machine with its own
/// lifecycle; a process may host any number of them.
public final class RoleContext {
    public let endpoint: Endpoint
    let env: any ProcessEnvironment
    public private(set) var stopped = false
    private var nextRid: UInt64 = 1
    private var pending: [UInt64: (deadline: UInt64, completion: (Message?) -> Void)] = [:]
    private var sweepScheduled = false
    /// Granularity of request timeouts (one sweep timer per role, not one per request).
    static let timeoutResolution: UInt64 = 10_000
    /// Called once when the role is ready to serve (used by recruitment).
    var onReady: (() -> Void)?
    /// Run by the host before each of this role's timers fires (process-wide checks).
    var beforeEvent: (() -> Void)?
    /// Called when the role asks to stop itself.
    var onStop: (() -> Void)?

    init(endpoint: Endpoint, env: any ProcessEnvironment) {
        self.endpoint = endpoint
        self.env = env
    }

    public var now: UInt64 { env.now }
    public var versionClock: UInt64 { env.versionClock }
    public var disk: any Disk { env.disk }
    public var processAddress: Endpoint { env.address }

    public func send(_ message: Message, to: Endpoint) {
        guard !stopped else { return }
        env.send(message, from: endpoint, to: to)
    }

    /// Sends the same message to each of `toAll`, in order (encoded once where possible).
    public func send(_ message: Message, toAll: [Endpoint]) {
        guard !stopped else { return }
        env.send(message, from: endpoint, toAll: toAll)
    }

    public func schedule(after micros: UInt64, _ action: @escaping () -> Void) {
        env.schedule(after: micros) { [weak self] in
            guard let self, !self.stopped else { return }
            self.beforeEvent?()
            guard !self.stopped else { return }
            action()
        }
    }

    /// Repeats `action` every `interval` until the role stops.
    public func every(_ interval: UInt64, _ action: @escaping () -> Void) {
        schedule(after: interval) { [weak self] in
            action()
            self?.every(interval, action)
        }
    }

    public func random(_ upperBound: UInt64) -> UInt64 { env.random(upperBound) }
    public func randomID() -> UInt64 { env.random(.max) | 1 }

    public func log(_ level: LogLevel, _ message: @autoclosure () -> String) {
        env.log(level, "\(shortName): \(message())")
    }

    /// The endpoint without the process's port noise, for logs.
    public var shortName: String { endpoint }

    /// Sends a request and calls `completion` with the reply, or nil on timeout.
    public func request(to: Endpoint, timeout: UInt64? = nil, _ make: (UInt64) -> Message,
                        _ completion: @escaping (Message?) -> Void) {
        guard !stopped else { return }
        let rid = nextRid
        nextRid += 1
        pending[rid] = (env.now + (timeout ?? 1_000_000), completion)
        env.send(make(rid), from: endpoint, to: to)
        scheduleSweep()
    }

    private func scheduleSweep() {
        guard !sweepScheduled, !pending.isEmpty else { return }
        sweepScheduled = true
        env.schedule(after: Self.timeoutResolution) { [weak self] in
            guard let self else { return }
            self.sweepScheduled = false
            guard !self.stopped else { return }
            let now = self.env.now
            let expired = self.pending.filter { $0.value.deadline <= now }.keys.sorted()
            for rid in expired {
                guard let entry = self.pending.removeValue(forKey: rid) else { continue }
                if !self.stopped { entry.completion(nil) }
            }
            self.scheduleSweep()
        }
    }

    /// Routes a reply to its waiting request. Returns false if nobody was waiting.
    func deliverReply(_ message: Message) -> Bool {
        guard let rid = message.rid, let entry = pending.removeValue(forKey: rid) else { return false }
        entry.completion(message)
        return true
    }

    public func ready() {
        let r = onReady
        onReady = nil
        r?()
    }

    /// Stops this role: no more sends, timers or callbacks; the host forgets it.
    public func stopSelf() {
        guard !stopped else { return }
        stopped = true
        pending = [:]
        onStop?()
    }

    func markStopped() {
        stopped = true
        pending = [:]
    }
}

public protocol Role: AnyObject {
    var context: RoleContext { get }
    func start()
    func receive(_ message: Message, from: Endpoint)
}

/// Hosts the roles of one process and routes messages to them.
public final class ProcessHost: ProcessNode {
    public let env: any ProcessEnvironment
    public private(set) var roles: [Endpoint: any Role] = [:]
    private var startActions: [() -> Void] = []
    private var eventChecks: [() -> Void] = []

    /// Registers a check run before every message or timer this process handles.
    public func beforeEvent(_ check: @escaping () -> Void) { eventChecks.append(check) }
    private func runEventChecks() { for c in eventChecks { c() } }
    public let processClass: ProcessClass

    public init(env: any ProcessEnvironment, processClass: ProcessClass) {
        self.env = env
        self.processClass = processClass
    }

    public var address: Endpoint { env.address }

    /// A fresh role endpoint on this process.
    public func makeEndpoint(_ kind: String, unique: Bool = true) -> Endpoint {
        if !unique { return "\(env.address)#\(kind)" }
        let id = String(env.random(0xFFFF_FFFF), radix: 16)
        return "\(env.address)#\(kind)-\(id)"
    }

    public func makeContext(_ endpoint: Endpoint) -> RoleContext {
        RoleContext(endpoint: endpoint, env: env)
    }

    /// Adds and starts a role.
    public func add(_ role: any Role) {
        let ep = role.context.endpoint
        roles[ep] = role
        role.context.onStop = { [weak self] in self?.roles[ep] = nil }
        role.context.beforeEvent = { [weak self] in self?.runEventChecks() }
        role.start()
    }

    /// Stops and removes a role.
    public func remove(_ endpoint: Endpoint) {
        guard let role = roles.removeValue(forKey: endpoint) else { return }
        role.context.markStopped()
        env.log(.info, "\(endpoint): halted")
    }

    public func onStart(_ action: @escaping () -> Void) { startActions.append(action) }

    public func start() {
        let actions = startActions
        startActions = []
        for a in actions { a() }
    }

    public func receive(_ message: Message, from: Endpoint, to: Endpoint) {
        runEventChecks()
        guard let role = roles[to] else {
            // A definitive "this role doesn't exist (any more)": role endpoints are unique
            // per incarnation, so the sender can treat it as permanent.
            if let rid = message.rid, !message.isReply {
                env.send(.errorReply(rid: rid, .staleGeneration), from: to, to: from)
            }
            return
        }
        if message.isReply, role.context.deliverReply(message) { return }
        role.receive(message, from: from)
    }

    /// Readiness for orchestration (Kubernetes readiness probes): a storage process is
    /// ready only once its replica is fully caught up, so rolling restarts never take
    /// down the last good copy of a shard.
    public var isReady: Bool {
        role(StorageServer.self)?.isReadyForRollout ?? true
    }

    public func role<T: Role>(_ type: T.Type) -> T? {
        for ep in roles.keys.sorted() { if let r = roles[ep] as? T { return r } }
        return nil
    }
}

/// Majority size for `n` voters.
@inlinable public func majority(_ n: Int) -> Int { n / 2 + 1 }
