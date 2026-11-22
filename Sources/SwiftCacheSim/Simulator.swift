import Synchronization
import SwiftCacheCore
import SwiftCacheServer

/// xoshiro256** seeded through splitmix64: fast, high quality and fully deterministic.
public struct SimRandom: RandomNumberGenerator, Sendable {
    private var s: (UInt64, UInt64, UInt64, UInt64)

    public init(seed: UInt64) {
        var x = seed
        func splitmix() -> UInt64 {
            x &+= 0x9E37_79B9_7F4A_7C15
            var z = x
            z = (z ^ (z >> 30)) &* 0xBF58_476D_1CE4_E5B9
            z = (z ^ (z >> 27)) &* 0x94D0_49BB_1331_11EB
            return z ^ (z >> 31)
        }
        s = (splitmix(), splitmix(), splitmix(), splitmix())
    }

    public mutating func next() -> UInt64 {
        func rotl(_ x: UInt64, _ k: UInt64) -> UInt64 { (x << k) | (x >> (64 - k)) }
        let result = rotl(s.1 &* 5, 7) &* 9
        let t = s.1 << 17
        s.2 ^= s.0; s.3 ^= s.1; s.1 ^= s.2; s.0 ^= s.3
        s.2 ^= t
        s.3 = rotl(s.3, 45)
        return result
    }

    public mutating func below(_ n: UInt64) -> UInt64 { n == 0 ? 0 : next() % n }
    public mutating func chance(_ p: Double) -> Bool { Double(next() >> 11) / Double(1 << 53) < p }
}

/// A task executor that queues jobs and runs them only when the simulator drains it, on
/// the simulator's thread, in FIFO order. All client tasks in a simulation run here, so
/// their interleaving is a pure function of the seed.
final class SimExecutor: TaskExecutor, @unchecked Sendable {
    private let queue = Mutex<[UnownedJob]>([])

    func enqueue(_ job: consuming ExecutorJob) {
        let j = UnownedJob(job)
        queue.withLock { $0.append(j) }
    }

    /// Runs queued jobs until none remain. Returns the number of jobs run.
    @discardableResult
    func drain() -> Int {
        var n = 0
        while true {
            let batch: [UnownedJob] = queue.withLock { q in
                let b = q
                q.removeAll(keepingCapacity: true)
                return b
            }
            if batch.isEmpty { return n }
            for job in batch {
                n += 1
                job.runSynchronously(on: asUnownedTaskExecutor())
            }
        }
    }
}

/// Fault-injection knobs for the simulated network.
public struct NetworkProfile: Sendable {
    /// One-way latency range in microseconds.
    public var minLatency: UInt64 = 100
    public var maxLatency: UInt64 = 2_000
    /// Probability that a message gets an extra delay spike.
    public var spikeProbability = 0.002
    public var spikeLatency: UInt64 = 200_000
    /// Round-trip messages through the binary codec (exercises encoding).
    public var roundTripCodec = true

    public init() {}
}

/// A discrete-event simulation of a SwiftCache cluster: one coordinator, any number of
/// storage servers and clients, a simulated network, a virtual clock and fault injection.
/// Given the same seed it always produces exactly the same execution.
public final class Simulator: @unchecked Sendable {
    public private(set) var now: UInt64 = 1_000_000_000
    public var random: SimRandom
    public let seed: UInt64
    public var network = NetworkProfile()
    public var config: ClusterConfig

    // Event queue.
    private struct Event: Comparable {
        var time: UInt64
        var seq: UInt64
        var action: () -> Void
        static func < (a: Event, b: Event) -> Bool { (a.time, a.seq) < (b.time, b.seq) }
        static func == (a: Event, b: Event) -> Bool { a.time == b.time && a.seq == b.seq }
    }
    private var events = EventHeap<Event>()
    private var seq: UInt64 = 0
    let executor = SimExecutor()

    // Processes.
    final class Process {
        let endpoint: Endpoint
        var node: (any ServerNode)?
        var incarnation: UInt64 = 0
        var alive = true
        let make: (any NodeEnvironment) -> any ServerNode
        var env: SimNodeEnvironment?
        init(endpoint: Endpoint, make: @escaping (any NodeEnvironment) -> any ServerNode) {
            self.endpoint = endpoint
            self.make = make
        }
    }
    private(set) var processes: [Endpoint: Process] = [:]
    public let coordinatorEndpoint: Endpoint = "coordinator"
    public private(set) var storageEndpoints: [Endpoint] = []

    // Clients.
    final class ClientEndpoint {
        var pending: [UInt64: CheckedContinuation<Message, any Error>] = [:]
    }
    private var clients: [Endpoint: ClientEndpoint] = [:]
    private var nextRid: UInt64 = 1

    // Network state: per-directed-link connection epoch and last delivery time (FIFO).
    private struct Link: Hashable { var from: Endpoint; var to: Endpoint }
    private var linkEpoch: [Link: UInt64] = [:]
    private var linkLastDelivery: [Link: UInt64] = [:]
    private var partitionedUntil: [Endpoint: UInt64] = [:]

    // Observability.
    public private(set) var traceHash: UInt64 = 0xcbf2_9ce4_8422_2325
    public private(set) var messagesDelivered = 0
    public private(set) var log: [String] = []
    public var verbose = false
    public var logLimit = 4_000
    public private(set) var failures: [String] = []

    public init(seed: UInt64, config: ClusterConfig = ClusterConfig()) {
        self.seed = seed
        self.random = SimRandom(seed: seed)
        self.config = config
    }

    // MARK: Building the cluster

    public func addCoordinator() {
        let config = self.config
        let ep = coordinatorEndpoint
        addProcess(ep) { env in Coordinator(endpoint: ep, config: config, env: env) }
    }

    @discardableResult
    public func addStorageServer() -> Endpoint {
        let ep = "storage-\(storageEndpoints.count)"
        storageEndpoints.append(ep)
        let config = self.config
        let coord = coordinatorEndpoint
        addProcess(ep) { env in StorageServer(endpoint: ep, coordinator: coord, config: config, env: env) }
        return ep
    }

    private func addProcess(_ ep: Endpoint, make: @escaping (any NodeEnvironment) -> any ServerNode) {
        let p = Process(endpoint: ep, make: make)
        processes[ep] = p
        boot(p)
    }

    private func boot(_ p: Process) {
        p.incarnation += 1
        p.alive = true
        let env = SimNodeEnvironment(sim: self, endpoint: p.endpoint, incarnation: p.incarnation)
        p.env = env
        let node = p.make(env)
        p.node = node
        schedule(after: 0) { node.start() }
    }

    /// The live coordinator object (for inspection in tests/checkers).
    public var coordinator: Coordinator? {
        guard let p = processes[coordinatorEndpoint], p.alive else { return nil }
        return p.node as? Coordinator
    }

    public func storageServer(_ ep: Endpoint) -> StorageServer? {
        guard let p = processes[ep], p.alive else { return nil }
        return p.node as? StorageServer
    }

    public func isAlive(_ ep: Endpoint) -> Bool { processes[ep]?.alive ?? false }

    // MARK: Faults

    /// Kills a process: its memory is lost and all its connections break.
    public func kill(_ ep: Endpoint) {
        guard let p = processes[ep], p.alive else { return }
        p.alive = false
        p.node = nil
        p.env = nil
        breakConnections(of: ep)
        trace("KILL \(ep)")
    }

    /// Restarts a killed process with empty state.
    public func restart(_ ep: Endpoint) {
        guard let p = processes[ep], !p.alive else { return }
        breakConnections(of: ep)
        boot(p)
        trace("RESTART \(ep)")
    }

    /// Isolates a process from the network for `duration` µs (connections break).
    public func partition(_ ep: Endpoint, for duration: UInt64) {
        partitionedUntil[ep] = now + duration
        breakConnections(of: ep)
        trace("PARTITION \(ep) for \(duration / 1000)ms")
        schedule(after: duration) { [weak self] in
            guard let self else { return }
            if let until = self.partitionedUntil[ep], until <= self.now {
                self.partitionedUntil[ep] = nil
                self.breakConnections(of: ep)
                self.trace("HEAL \(ep)")
            }
        }
    }

    public func isPartitioned(_ ep: Endpoint) -> Bool {
        if let until = partitionedUntil[ep] { return until > now }
        return false
    }

    private func breakConnections(of ep: Endpoint) {
        for link in linkEpoch.keys where link.from == ep || link.to == ep {
            linkEpoch[link]! += 1
        }
        // Links not yet in the table start at 0; bump them lazily by recording a marker.
        brokenAt[ep] = (brokenAt[ep] ?? 0) + 1
    }
    private var brokenAt: [Endpoint: UInt64] = [:]

    private func currentEpoch(_ link: Link) -> UInt64 {
        (linkEpoch[link] ?? 0) &+ (brokenAt[link.from] ?? 0) &* 1_000_003 &+ (brokenAt[link.to] ?? 0) &* 7_919
    }

    // MARK: Network

    func send(_ message: Message, from: Endpoint, to: Endpoint) {
        if isPartitioned(from) || isPartitioned(to) { return }
        if let p = processes[from], !p.alive { return }
        let link = Link(from: from, to: to)
        if linkEpoch[link] == nil { linkEpoch[link] = 0 }
        let epoch = currentEpoch(link)
        var latency = network.minLatency + random.below(network.maxLatency - network.minLatency + 1)
        if random.chance(network.spikeProbability) { latency += random.below(network.spikeLatency) }
        let at = max(now + latency, (linkLastDelivery[link] ?? 0) + 1)
        linkLastDelivery[link] = at
        let payload: Message
        if network.roundTripCodec {
            do {
                payload = try Message.decode(message.encode())
            } catch {
                fail("codec round trip failed for \(message.name): \(error)")
                return
            }
            if payload != message { fail("codec round trip changed \(message.name)") }
        } else {
            payload = message
        }
        scheduleAt(at) { [weak self] in
            guard let self else { return }
            guard self.currentEpoch(link) == epoch, !self.isPartitioned(from), !self.isPartitioned(to) else { return }
            self.deliver(payload, from: from, to: to)
        }
    }

    private func deliver(_ message: Message, from: Endpoint, to: Endpoint) {
        messagesDelivered += 1
        mixTrace(from)
        mixTrace(to)
        mixTrace(message.name)
        if let client = clients[to] {
            if let rid = message.rid, let cont = client.pending.removeValue(forKey: rid) {
                cont.resume(returning: message)
            }
            return
        }
        guard let p = processes[to], p.alive, let node = p.node else { return }
        node.receive(message, from: from)
    }

    // MARK: Scheduling

    func schedule(after micros: UInt64, _ action: @escaping () -> Void) {
        scheduleAt(now + micros, action)
    }

    private func scheduleAt(_ time: UInt64, _ action: @escaping () -> Void) {
        seq += 1
        events.push(Event(time: max(time, now), seq: seq, action: action))
    }

    /// Schedules an action from inside the simulation (fault injection, workload control).
    public func at(_ time: UInt64, _ action: @escaping () -> Void) { scheduleAt(time, action) }
    public func after(_ micros: UInt64, _ action: @escaping () -> Void) { schedule(after: micros, action) }

    /// Runs until virtual time reaches `time` (or nothing is left to do).
    public func run(until time: UInt64) {
        executor.drain()
        while let next = events.peek, next.time <= time {
            let e = events.pop()!
            now = e.time
            e.action()
            executor.drain()
        }
        if now < time { now = time }
    }

    /// Runs until `condition` is true or `timeout` µs of virtual time pass. Returns whether
    /// the condition was met.
    @discardableResult
    public func run(for timeout: UInt64, until condition: () -> Bool) -> Bool {
        let deadline = now + timeout
        executor.drain()
        if condition() { return true }
        while let next = events.peek, next.time <= deadline {
            let e = events.pop()!
            now = e.time
            e.action()
            executor.drain()
            if condition() { return true }
        }
        return condition()
    }

    // MARK: Clients

    /// Creates a client endpoint and its transport.
    public func makeClientTransport(name: String? = nil) -> SimClientTransport {
        let ep = name ?? "client-\(clients.count)"
        clients[ep] = ClientEndpoint()
        return SimClientTransport(sim: self, endpoint: ep)
    }

    func clientRequest(from client: Endpoint, to: Endpoint, timeout: UInt64, _ make: (UInt64) -> Message,
                       _ cont: CheckedContinuation<Message, any Error>) {
        let rid = nextRid
        nextRid += 1
        clients[client]!.pending[rid] = cont
        send(make(rid), from: client, to: to)
        schedule(after: timeout) { [weak self] in
            guard let self, let c = self.clients[client]!.pending.removeValue(forKey: rid) else { return }
            c.resume(throwing: TransportError.timedOut(to))
        }
    }

    /// Spawns an async task on the deterministic executor.
    public func spawn(_ body: @escaping @Sendable () async -> Void) {
        Task(executorPreference: executor) { await body() }
        executor.drain()
    }

    // MARK: Logging, tracing, failures

    func nodeLog(_ endpoint: Endpoint, _ level: LogLevel, _ message: String) {
        appendLog("\(timestamp) [\(level)] \(message)")
    }

    public func trace(_ message: String) {
        mixTrace(message)
        appendLog("\(timestamp) [sim] \(message)")
    }

    private func appendLog(_ line: String) {
        if verbose { print(line) }
        log.append(line)
        if log.count > logLimit * 2 { log.removeFirst(log.count - logLimit) }
    }

    public var timestamp: String {
        let t = now - 1_000_000_000
        return "t=\(t / 1_000_000).\(String(format3(t % 1_000_000 / 1000)))s"
    }

    private func format3(_ v: UInt64) -> String {
        let s = String(v)
        return String(repeating: "0", count: max(0, 3 - s.count)) + s
    }

    private func mixTrace(_ s: String) {
        for b in s.utf8 {
            traceHash ^= UInt64(b)
            traceHash = traceHash &* 0x100_0000_01b3
        }
        traceHash ^= now
        traceHash = traceHash &* 0x100_0000_01b3
    }

    /// Records an invariant violation.
    public func fail(_ message: String) {
        failures.append("\(timestamp) \(message)")
        appendLog("\(timestamp) [FAIL] \(message)")
    }

    public var pendingEvents: Int { events.count }
}

/// The `NodeEnvironment` the simulator gives each server process.
final class SimNodeEnvironment: NodeEnvironment {
    unowned let sim: Simulator
    let endpoint: Endpoint
    let incarnation: UInt64

    init(sim: Simulator, endpoint: Endpoint, incarnation: UInt64) {
        self.sim = sim
        self.endpoint = endpoint
        self.incarnation = incarnation
    }

    private var isCurrent: Bool {
        guard let p = sim.processes[endpoint] else { return false }
        return p.alive && p.incarnation == incarnation
    }

    var now: UInt64 { sim.now }
    var versionClock: UInt64 { sim.now }

    func send(_ message: Message, to endpoint: Endpoint) {
        guard isCurrent else { return }
        sim.send(message, from: self.endpoint, to: endpoint)
    }

    func schedule(after micros: UInt64, _ action: @escaping () -> Void) {
        sim.schedule(after: micros) { [weak self] in
            guard let self, self.isCurrent else { return }
            action()
        }
    }

    func random(_ upperBound: UInt64) -> UInt64 { sim.random.below(upperBound) }

    func log(_ level: LogLevel, _ message: @autoclosure () -> String) {
        guard level >= .info else { return }
        sim.nodeLog(endpoint, level, message())
    }
}

/// The `ClientTransport` used by clients inside a simulation.
public final class SimClientTransport: ClientTransport, @unchecked Sendable {
    unowned let sim: Simulator
    public let endpoint: Endpoint

    init(sim: Simulator, endpoint: Endpoint) {
        self.sim = sim
        self.endpoint = endpoint
    }

    public func request(to endpoint: Endpoint, timeout: UInt64, _ make: (UInt64) -> Message) async throws -> Message {
        try await withCheckedThrowingContinuation { cont in
            sim.clientRequest(from: self.endpoint, to: endpoint, timeout: timeout, make, cont)
        }
    }

    public func sleep(micros: UInt64) async throws {
        await withCheckedContinuation { (cont: CheckedContinuation<Void, Never>) in
            sim.schedule(after: micros) { cont.resume() }
        }
    }

    public func now() -> UInt64 { sim.now }
    public func random(_ upperBound: UInt64) -> UInt64 { sim.random.below(upperBound) }
}

/// Binary min-heap for the event queue.
struct EventHeap<Element: Comparable> {
    private var items: [Element] = []
    var count: Int { items.count }
    var peek: Element? { items.first }

    mutating func push(_ e: Element) {
        items.append(e)
        var i = items.count - 1
        while i > 0 {
            let p = (i - 1) / 2
            if items[i] < items[p] { items.swapAt(i, p); i = p } else { break }
        }
    }

    mutating func pop() -> Element? {
        guard !items.isEmpty else { return nil }
        let top = items[0]
        let last = items.removeLast()
        if !items.isEmpty {
            items[0] = last
            var i = 0
            while true {
                let l = 2 * i + 1, r = l + 1
                var m = i
                if l < items.count && items[l] < items[m] { m = l }
                if r < items.count && items[r] < items[m] { m = r }
                if m == i { break }
                items.swapAt(i, m)
                i = m
            }
        }
        return top
    }
}
