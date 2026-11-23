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

/// A discrete-event simulation of a SwiftCache cluster: coordinators, stateless, log and
/// storage processes, any number of clients, a simulated network, a virtual clock and
/// fault injection. Given the same seed it always produces exactly the same execution.
public final class Simulator: @unchecked Sendable {
    public private(set) var now: UInt64 = 1_000_000_000
    public var random: SimRandom
    public let seed: UInt64
    public var network = NetworkProfile()
    public var config: ClusterConfig
    /// Maximum relative clock-rate skew per process (e.g. 0.08 = ±8%).
    public var clockSkew = 0.0

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
        let address: Endpoint
        let processClass: ProcessClass
        let isCoordinator: Bool
        var host: ProcessHost?
        var incarnation: UInt64 = 0
        var alive = true
        /// Survives restarts (only coordinators use it).
        let disk = SimDisk()
        /// This machine's clock runs at `clockRate` × real time (clock-rate skew).
        var clockRate = 1.0
        /// While `pausedUntil > now` the process is frozen (GC/scheduler stall): nothing is
        /// delivered and no timer fires; everything is released, in order, afterwards.
        var pausedUntil: UInt64 = 0
        /// While `holdOutgoingUntil > now`, everything the process sends is queued (it keeps
        /// running and deciding); the queue is released, in order, afterwards.
        var holdOutgoingUntil: UInt64 = 0
        init(address: Endpoint, processClass: ProcessClass, isCoordinator: Bool) {
            self.address = address
            self.processClass = processClass
            self.isCoordinator = isCoordinator
        }
    }
    private(set) var processes: [Endpoint: Process] = [:]
    public private(set) var coordinatorAddresses: [Endpoint] = []
    public private(set) var storageAddresses: [Endpoint] = []
    public private(set) var logAddresses: [Endpoint] = []
    public private(set) var statelessAddresses: [Endpoint] = []

    // Clients.
    final class ClientEndpoint {
        var pending: [UInt64: CheckedContinuation<Message, any Error>] = [:]
    }
    private var clients: [Endpoint: ClientEndpoint] = [:]
    private var nextRid: UInt64 = 1

    // Network state.
    private struct Link: Hashable { var from: Endpoint; var to: Endpoint }
    private var linkLastDelivery: [Link: UInt64] = [:]
    /// Bumped whenever a process's connections break (kill, restart, partition).
    private var connectionEpoch: [Endpoint: UInt64] = [:]
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

    /// Adds `n` coordinator processes (stateless class). Call before adding anything else.
    public func addCoordinators(_ n: Int) {
        let added = (0..<n).map { _ in addProcess(.stateless, coordinator: true, boot: false) }
        for a in added { boot(processes[a]!) }
    }

    /// Adds a process.
    @discardableResult
    public func addProcess(_ cls: ProcessClass, coordinator: Bool = false, boot shouldBoot: Bool = true) -> Endpoint {
        let prefix: String
        switch cls {
        case .storage: prefix = "storage-\(storageAddresses.count)"
        case .log: prefix = "log-\(logAddresses.count)"
        case .stateless: prefix = coordinator ? "coord-\(coordinatorAddresses.count)" : "stateless-\(statelessAddresses.count)"
        }
        let address = "\(prefix):4500"
        switch cls {
        case .storage: storageAddresses.append(address)
        case .log: logAddresses.append(address)
        case .stateless: statelessAddresses.append(address)
        }
        if coordinator { coordinatorAddresses.append(address) }
        let p = Process(address: address, processClass: cls, isCoordinator: coordinator)
        if clockSkew > 0 {
            p.clockRate = 1 + clockSkew * (Double(random.below(2_001)) / 1_000 - 1)
        }
        processes[address] = p
        if shouldBoot { boot(p) }
        return address
    }

    public var coordinatorEndpoints: [Endpoint] { coordinatorAddresses.map(ServerProcess.coordinatorEndpoint) }

    private func boot(_ p: Process) {
        p.incarnation += 1
        p.alive = true
        let env = SimProcessEnvironment(sim: self, address: p.address, incarnation: p.incarnation, disk: p.disk,
                                        clockRate: p.clockRate, clockBase: now)
        let host = ServerProcess.make(env: env, processClass: p.processClass, isCoordinator: p.isCoordinator,
                                      coordinators: coordinatorEndpoints, config: config)
        p.host = host
        schedule(after: 0) { host.start() }
    }

    public var allAddresses: [Endpoint] { processes.keys.sorted() }

    public func host(_ address: Endpoint) -> ProcessHost? {
        guard let p = processes[address], p.alive else { return nil }
        return p.host
    }

    public func isAlive(_ address: Endpoint) -> Bool { processes[address]?.alive ?? false }

    /// The cluster controller that is currently running a recovered generation, if any.
    public var controller: ClusterController? {
        for a in allAddresses {
            if let cc = host(a)?.role(ClusterController.self), !cc.context.stopped { return cc }
        }
        return nil
    }

    public func roles<T: Role>(_ type: T.Type) -> [T] {
        allAddresses.compactMap { host($0) }.flatMap { h in h.roles.keys.sorted().compactMap { h.roles[$0] as? T } }
    }

    // MARK: Faults

    /// Kills a process: its memory is lost (not its disk) and all its connections break.
    public func kill(_ address: Endpoint) {
        guard let p = processes[address], p.alive else { return }
        p.alive = false
        p.host = nil
        breakConnections(of: address)
        trace("KILL \(address)")
    }

    /// Restarts a killed process with empty memory (its disk is kept).
    public func restart(_ address: Endpoint) {
        guard let p = processes[address], !p.alive else { return }
        breakConnections(of: address)
        boot(p)
        trace("RESTART \(address)")
    }

    /// Crashes a single role, leaving the rest of its process running.
    public func killRole(_ endpoint: Endpoint) {
        guard let h = host(endpoint.processAddress), h.roles[endpoint] != nil else { return }
        h.remove(endpoint)
        trace("KILL ROLE \(endpoint)")
    }

    /// Freezes a process for `duration` µs, like a long GC or scheduler pause: its
    /// connections stay up, but it handles nothing until the pause ends.
    public func pause(_ address: Endpoint, for duration: UInt64) {
        guard let p = processes[address], p.alive else { return }
        p.pausedUntil = max(p.pausedUntil, now + duration)
        trace("PAUSE \(address) for \(duration / 1000)ms")
    }

    func pausedUntil(_ address: Endpoint) -> UInt64? {
        guard let p = processes[address], p.pausedUntil > now else { return nil }
        return p.pausedUntil
    }

    /// Delays everything a process sends for `duration` µs without breaking connections:
    /// models a process whose decisions reach the world late (stall after deciding, slow
    /// outbound path).
    public func holdOutgoing(_ address: Endpoint, for duration: UInt64) {
        guard let p = processes[address], p.alive else { return }
        p.holdOutgoingUntil = max(p.holdOutgoingUntil, now + duration)
        trace("HOLD OUTGOING \(address) for \(duration / 1000)ms")
    }

    /// Isolates a process from the network for `duration` µs (connections break).
    public func partition(_ address: Endpoint, for duration: UInt64) {
        partitionedUntil[address] = max(partitionedUntil[address] ?? 0, now + duration)
        breakConnections(of: address)
        trace("PARTITION \(address) for \(duration / 1000)ms")
        schedule(after: duration) { [weak self] in
            guard let self, let until = self.partitionedUntil[address], until <= self.now else { return }
            self.partitionedUntil[address] = nil
            self.breakConnections(of: address)
            self.trace("HEAL \(address)")
        }
    }

    public func isPartitioned(_ address: Endpoint) -> Bool {
        if let until = partitionedUntil[address] { return until > now }
        return false
    }

    private func breakConnections(of address: Endpoint) {
        connectionEpoch[address, default: 0] += 1
    }

    private func epoch(_ link: Link) -> (UInt64, UInt64) {
        (connectionEpoch[link.from] ?? 0, connectionEpoch[link.to] ?? 0)
    }

    // MARK: Network

    func send(_ message: Message, from: Endpoint, to: Endpoint) {
        let src = from.processAddress, dst = to.processAddress
        if let p = processes[src], p.holdOutgoingUntil > now {
            let incarnation = p.incarnation
            scheduleAt(p.holdOutgoingUntil) { [weak self] in
                guard let self, p.alive, p.incarnation == incarnation else { return }
                self.send(message, from: from, to: to)
            }
            return
        }
        if isPartitioned(src) || isPartitioned(dst) { return }
        if let p = processes[src], !p.alive { return }
        let link = Link(from: src, to: dst)
        let e = epoch(link)
        var latency = network.minLatency + random.below(network.maxLatency - network.minLatency + 1)
        if src == dst { latency = 1 }
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
            guard self.epoch(link) == e, !self.isPartitioned(src), !self.isPartitioned(dst) else { return }
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
        guard let p = processes[to.processAddress], p.alive, let host = p.host else { return }
        if p.pausedUntil > now {
            // Frozen: hold it (events at the same time keep their order).
            let incarnation = p.incarnation
            scheduleAt(p.pausedUntil) { [weak self] in
                guard let self, p.incarnation == incarnation else { return }
                self.deliver(message, from: from, to: to)
            }
            return
        }
        host.receive(message, from: from, to: to)
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

    /// Runs until `condition` is true or `timeout` µs of virtual time pass.
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
        let ep = "client:" + (name ?? "\(clients.count)")
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

    func nodeLog(_ level: LogLevel, _ message: String) {
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
        let ms = String(t % 1_000_000 / 1000)
        return "t=\(t / 1_000_000).\(String(repeating: "0", count: 3 - ms.count) + ms)s"
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

/// A simulated disk: survives process restarts.
final class SimDisk: Disk {
    private var values: [String: Bytes] = [:]
    func read(_ key: String) -> Bytes? { values[key] }
    func write(_ key: String, _ value: Bytes) { values[key] = value }
}

/// The `ProcessEnvironment` the simulator gives each process.
final class SimProcessEnvironment: ProcessEnvironment {
    unowned let sim: Simulator
    let address: Endpoint
    let incarnation: UInt64
    let disk: any Disk
    /// Local clock = clockBase + (sim time − clockBase) × clockRate.
    let clockRate: Double
    let clockBase: UInt64

    init(sim: Simulator, address: Endpoint, incarnation: UInt64, disk: any Disk, clockRate: Double, clockBase: UInt64) {
        self.sim = sim
        self.address = address
        self.incarnation = incarnation
        self.disk = disk
        self.clockRate = clockRate
        self.clockBase = clockBase
    }

    private var isCurrent: Bool {
        guard let p = sim.processes[address] else { return false }
        return p.alive && p.incarnation == incarnation
    }

    var now: UInt64 { clockBase + UInt64(Double(sim.now - clockBase) * clockRate) }
    var versionClock: UInt64 { now }

    func send(_ message: Message, from: Endpoint, to: Endpoint) {
        guard isCurrent else { return }
        sim.send(message, from: from, to: to)
    }

    func schedule(after micros: UInt64, _ action: @escaping () -> Void) {
        // `micros` of *local* time is micros / clockRate of real time.
        let real = UInt64((Double(micros) / clockRate).rounded(.up))
        sim.schedule(after: real) { [weak self] in self?.fire(action) }
    }

    private func fire(_ action: @escaping () -> Void) {
        guard isCurrent else { return }
        if let until = sim.pausedUntil(address) {
            sim.at(until) { [weak self] in self?.fire(action) }
            return
        }
        action()
    }

    func random(_ upperBound: UInt64) -> UInt64 { sim.random.below(upperBound) }

    func log(_ level: LogLevel, _ message: @autoclosure () -> String) {
        guard level >= .info else { return }
        sim.nodeLog(level, message())
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
