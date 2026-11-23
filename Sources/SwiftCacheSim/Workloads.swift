import Foundation
import SwiftCache
import SwiftCacheCore
import SwiftCacheServer
import Synchronization

/// Shared mutable counters for workloads (single-threaded inside the simulator).
final class Counters: @unchecked Sendable {
    var values: [String: Int] = [:]
    /// Every versionstamp a client was told it committed at: must be unique.
    var stamps: [Versionstamp: String] = [:]
    /// Order ids acknowledged by the idempotent-orders workload.
    var orders: Set<String> = []
    func inc(_ k: String, _ n: Int = 1) { values[k, default: 0] += n }
    subscript(_ k: String) -> Int { values[k] ?? 0 }
}

/// Options for a full simulation run.
public struct SimulationOptions: Sendable {
    public var coordinators = 3
    public var logProcesses = 3
    public var extraStateless = 1
    public var storageServers = 6
    public var replicationFactor = 3
    public var clients = 8
    /// Virtual seconds of workload + chaos.
    public var duration: UInt64 = 20_000_000
    /// Storage servers added part-way through (exercises shard formation + migration).
    public var scaleOutServers = 3
    public var chaos = true
    /// Include faults that hit the transaction system (roles, logs, controller, coordinators).
    public var transactionSystemFaults = true
    /// Per-process clock-rate skew (±). Safety must hold for any skew within the lease
    /// margin (20%).
    public var clockSkew = 0.08
    public var verbose = false
    /// Run a targeted fault scenario instead of random chaos.
    public var scenario: Scenario?
    public init() {}
}

public struct SimulationResult: Sendable {
    public var seed: UInt64
    public var failures: [String]
    public var traceHash: UInt64
    public var stats: [String: Int]
    public var messages: Int
    public var logTail: [String]
    public var passed: Bool { failures.isEmpty }
}

/// Runs the standard workload mix against a simulated cluster with fault injection and
/// checks invariants:
///
/// * **bank** — transfers between accounts; total balance is constant (serializability,
///   checked with point reads and range reads).
/// * **cycle** — keys form a permutation cycle mutated by 3-key swaps; it must remain a
///   single cycle (FoundationDB's classic workload).
/// * **cas** — compare-and-swap increments with a per-value log key written in the same
///   transaction; the log must be exactly 1...counter (no lost or duplicated updates).
/// * **add** — idempotent atomic adds; the total must equal the acknowledged adds exactly.
/// * **orders** — idempotent transactions that generate a new id per attempt; every
///   acknowledged order exists exactly once and the count matches.
/// * **history** — writes values and later re-reads them at the commit's version.
/// * Globally: no versionstamp is ever handed out twice.
public func runSimulation(seed: UInt64, options: SimulationOptions = SimulationOptions()) -> SimulationResult {
    var config = ClusterConfig()
    config.replicationFactor = options.replicationFactor
    config.mvccWindow = 5_000_000
    config.migrationChunkSlots = 4096
    config.shardLossTimeout = 20_000_000
    if ProcessInfo.processInfo.environment["SIM_NO_HALT_STRAY"] != nil { config.haltStrayRoles = false }
    let sim = Simulator(seed: seed, config: config)
    sim.clockSkew = options.clockSkew
    sim.verbose = options.verbose
    sim.addCoordinators(options.coordinators)
    for _ in 0..<options.logProcesses { sim.addProcess(.log) }
    for _ in 0..<options.extraStateless { sim.addProcess(.stateless) }
    for _ in 0..<options.storageServers { sim.addProcess(.storage) }

    let counters = Counters()
    let start = sim.now
    let workEnd = start + options.duration
    let accounts = 20, cycleSize = 16

    var setupOptions = DatabaseOptions()
    setupOptions.maxRetries = 1000
    let setupDB = Database(coordinators: sim.coordinatorAddresses, transport: sim.makeClientTransport(name: "setup"), options: setupOptions)
    let ready = Mutex(false)
    sim.spawn {
        do {
            try await setupDB.transaction { tr in
                for i in 0..<accounts { try tr.set("bank/\(pad(i))", 100) }
                for i in 0..<cycleSize { try tr.set("cycle/\(pad(i))", (i + 1) % cycleSize) }
                try tr.set("cas/counter", 0)
                try tr.set("rt/c", 0)
                tr.add("add/total", 0)
                tr.add("orders/count", 0)
            }
            ready.withLock { $0 = true }
        } catch {
            sim.fail("setup failed: \(error)")
        }
    }
    if !sim.run(for: 60_000_000, until: { ready.withLock { $0 } }) {
        sim.fail("cluster never became ready")
        return result(sim, counters)
    }
    sim.trace("setup complete")

    for c in 0..<options.clients {
        var opts = DatabaseOptions()
        opts.requestTimeout = 1_000_000
        opts.maxRetries = 400
        let db = Database(coordinators: sim.coordinatorAddresses, transport: sim.makeClientTransport(), options: opts)
        let kind = c % 7
        if kind == 6 {
            // Real-time order needs a second, independent client: the writer's acks must
            // be visible to transactions the reader starts afterwards.
            let reader = Database(coordinators: sim.coordinatorAddresses, transport: sim.makeClientTransport(), options: opts)
            sim.spawn { await realtimeWriter(sim, db, counters, until: workEnd) }
            sim.spawn { await realtimeReader(sim, reader, counters, until: workEnd) }
            continue
        }
        sim.spawn {
            switch kind {
            case 0: await bankWorkload(sim, db, counters, accounts: accounts, until: workEnd)
            case 1: await cycleWorkload(sim, db, counters, size: cycleSize, until: workEnd)
            case 2: await casWorkload(sim, db, counters, client: c, until: workEnd)
            case 3: await addWorkload(sim, db, counters, until: workEnd)
            case 4: await ordersWorkload(sim, db, counters, until: workEnd)
            default: await historyWorkload(sim, db, counters, client: c, until: workEnd)
            }
        }
    }

    if options.scaleOutServers > 0 {
        sim.at(start + options.duration / 4) {
            sim.trace("scaling out by \(options.scaleOutServers) storage servers")
            for _ in 0..<options.scaleOutServers { sim.addProcess(.storage) }
        }
    }
    let monitor = Monitor()
    monitor.install(sim)
    if let scenario = options.scenario {
        scheduleScenario(sim, scenario, monitor: monitor, counters: counters)
    } else if options.chaos {
        scheduleChaos(sim, options: options, until: workEnd - 2_000_000)
    }

    sim.run(until: workEnd)
    // Quiesce: heal everything, let clients finish.
    for a in sim.allAddresses where !sim.isAlive(a) { sim.restart(a) }
    sim.run(until: workEnd + 20_000_000)

    let done = Mutex(false)
    var checkOptions = DatabaseOptions()
    checkOptions.maxRetries = 1000
    let checkDB = Database(coordinators: sim.coordinatorAddresses, transport: sim.makeClientTransport(name: "checker"), options: checkOptions)
    sim.spawn {
        await finalChecks(sim, checkDB, counters, accounts: accounts, cycleSize: cycleSize)
        done.withLock { $0 = true }
    }
    if !sim.run(for: 120_000_000, until: { done.withLock { $0 } }) {
        sim.fail("final checks did not complete (cluster unavailable after quiescing)")
        dumpState(sim)
    }

    checkReplicaConsistency(sim)
    checkLogRetention(sim)
    checkCoordinatorsAgree(sim)
    if options.clients >= 6 {
        let realtime = options.clients >= 7 ? ["realtime.writes", "realtime.checks"] : []
        for k in ["bank.commits", "cycle.commits", "cas.success", "add.commits", "orders.commits", "history.commits"] + realtime
        where counters[k] == 0 {
            sim.fail("no progress: \(k) == 0")
        }
    }
    if let cc = sim.controller {
        let st = cc.status
        let total = options.storageServers + options.scaleOutServers
        if total >= 2 * options.replicationFactor && st.topology.shards.count < 2 {
            sim.fail("expected more than one shard, have \(st.topology.shards.count)")
        }
        for sh in st.topology.shards where sh.replicas.isEmpty { sim.fail("shard \(sh.id) has no replicas at end") }
    } else {
        sim.fail("no cluster controller at end")
    }
    return result(sim, counters)
}

/// Logs a snapshot of every role, for diagnosing stuck runs.
func dumpState(_ sim: Simulator) {
    for cc in sim.roles(ClusterController.self) {
        let s = cc.status
        sim.trace("DUMP controller \(cc.context.endpoint) stopped=\(cc.context.stopped) phase=\(cc.phase) gen=\(s.generation) master=\(s.master) proxies=\(s.proxies) tlogs=\(s.tlogs)")
    }
    for c in sim.roles(CandidateRole.self) { sim.trace("DUMP candidate \(c.context.endpoint) leader=\(c.isLeader)") }
    for m in sim.roles(MasterRole.self) { sim.trace("DUMP master \(m.context.endpoint) gen=\(m.generation) phase=\(m.phase) last=\(m.lastVersion) committed=\(m.committed)") }
    for p in sim.roles(CommitProxy.self) { sim.trace("DUMP proxy \(p.context.endpoint) gen=\(p.generation) dead=\(p.dead)") }
    for t in sim.roles(TLogRole.self) { sim.trace("DUMP tlog \(t.context.endpoint) gen=\(t.generation) locked=\(t.locked) durable=\(t.durable) kcv=\(t.knownCommitted)") }
    for s in sim.roles(StorageServer.self) { sim.trace("DUMP storage \(s.context.endpoint) tag=\(s.tag) shard=\(s.shardID) active=\(s.isActive) applied=\(s.applied)") }
    for a in sim.allAddresses { sim.trace("DUMP process \(a) alive=\(sim.isAlive(a)) partitioned=\(sim.isPartitioned(a))") }
}

private func result(_ sim: Simulator, _ counters: Counters) -> SimulationResult {
    var stats = counters.values
    if let cc = sim.controller { stats["recoveries"] = Int(cc.recoveries); stats["generation"] = Int(cc.info.generation) }
    return SimulationResult(seed: sim.seed, failures: sim.failures, traceHash: sim.traceHash, stats: stats,
                            messages: sim.messagesDelivered, logTail: Array(sim.log.suffix(80)))
}

func pad(_ i: Int) -> String {
    let s = String(i)
    return String(repeating: "0", count: max(0, 4 - s.count)) + s
}

/// Records a versionstamp a client was told it committed at; it must be unique.
func recordStamp(_ sim: Simulator, _ counters: Counters, _ stamp: Versionstamp?, _ who: String) {
    guard let stamp, stamp != .zero else { return }
    if let other = counters.stamps[stamp] { sim.fail("versionstamp \(stamp) issued twice (\(other) and \(who))") }
    counters.stamps[stamp] = who
}

// MARK: - Chaos

/// Injects faults while making sure the cluster never loses data it is designed to keep:
/// faults are only injected when the cluster is fully healthy (judged by the simulator's
/// ground truth *and* the controller's view), so at most one replica of any shard, one
/// log and one coordinator are down at a time — except the deliberate coordinator-quorum
/// outage, which must stall the cluster without losing anything.
func scheduleChaos(_ sim: Simulator, options: SimulationOptions, until end: UInt64) {
    func healthy() -> Bool {
        guard sim.allAddresses.allSatisfy({ sim.isAlive($0) && !sim.isPartitioned($0) }) else { return false }
        guard let cc = sim.controller, cc.phase == .running else { return false }
        let st = cc.status
        guard st.movesInProgress.isEmpty else { return false }
        // Ground truth for every listed replica: the controller lags behind restarts, so a
        // freshly restarted (empty) process may still be listed as an active replica.
        let live = sim.roles(StorageServer.self)
        for shard in st.topology.shards {
            guard shard.replicas.count >= options.replicationFactor else { return false }
            for ep in shard.replicas {
                guard let s = live.first(where: { $0.context.endpoint == ep }), s.tag != 0, s.isActive else { return false }
            }
        }
        return true
    }
    func pick(_ xs: [Endpoint]) -> Endpoint? { xs.isEmpty ? nil : xs[Int(sim.random.below(UInt64(xs.count)))] }
    func killAndRestart(_ a: Endpoint) {
        sim.kill(a)
        sim.after(200_000 + sim.random.below(3_000_000)) { sim.restart(a) }
    }
    func step() {
        guard sim.now < end else { return }
        let roll = sim.random.below(100)
        let txnFaults = options.transactionSystemFaults
        if roll < 20 {
            if healthy(), let v = pick(sim.storageAddresses) { killAndRestart(v) }
        } else if roll < 32 {
            if healthy(), let v = pick(sim.storageAddresses) { sim.partition(v, for: 100_000 + sim.random.below(2_500_000)) }
        } else if roll < 42 && txnFaults {
            // A log process dies: the generation must recover onto the survivors.
            if healthy(), let v = pick(sim.logAddresses) { killAndRestart(v) }
        } else if roll < 50 && txnFaults {
            // A stateless or coordinator process dies (maybe hosting the controller/master/proxies).
            if healthy(), let v = pick(sim.statelessAddresses) { killAndRestart(v) }
        } else if roll < 60 && txnFaults {
            // A single transaction role crashes inside a live process.
            if healthy() {
                let roles = sim.roles(MasterRole.self).map(\.context.endpoint) + sim.roles(CommitProxy.self).map(\.context.endpoint)
                    + sim.roles(ResolverRole.self).map(\.context.endpoint) + sim.roles(TLogRole.self).map(\.context.endpoint)
                    + sim.roles(ClusterController.self).map(\.context.endpoint)
                if let r = pick(roles) { sim.killRole(r) }
            }
        } else if roll < 67 && txnFaults {
            // Partition whichever process hosts the controller: it must step down and be
            // replaced, and nothing it does afterwards may take effect.
            if healthy(), let cc = sim.controller {
                sim.partition(cc.context.endpoint.processAddress, for: 500_000 + sim.random.below(3_000_000))
            }
        } else if roll < 71 && txnFaults {
            // Lose a coordinator quorum for a while: no progress is allowed (and none may be
            // unsafe); when they return the cluster must recover.
            if healthy() {
                let victims = Array(sim.coordinatorAddresses.shuffledDeterministically(&sim.random).prefix(majority(sim.coordinatorAddresses.count)))
                sim.trace("coordinator quorum outage: \(victims)")
                for v in victims { sim.kill(v) }
                sim.after(1_000_000 + sim.random.below(3_000_000)) { for v in victims { sim.restart(v) } }
            }
        } else if roll < 80 && txnFaults {
            if healthy(), let v = pick(sim.coordinatorAddresses + sim.logAddresses) {
                sim.partition(v, for: 50_000 + sim.random.below(1_500_000))
            }
        } else if roll < 84 {
            // A process stalls (GC / scheduler pause), possibly past its leases.
            if let v = pick(sim.allAddresses) { sim.pause(v, for: 50_000 + sim.random.below(900_000)) }
        } else if roll < 90 {
            sim.network.maxLatency = 20_000
            sim.after(1_000_000) { sim.network.maxLatency = 2_000 }
        }
        sim.after(200_000 + sim.random.below(600_000)) { step() }
    }
    sim.after(1_000_000) { step() }
}

extension Array {
    func shuffledDeterministically(_ rng: inout SimRandom) -> [Element] {
        var a = self
        if a.count < 2 { return a }
        for i in stride(from: a.count - 1, to: 0, by: -1) {
            let j = Int(rng.below(UInt64(i + 1)))
            a.swapAt(i, j)
        }
        return a
    }
}

// MARK: - Workloads

private func handle(_ sim: Simulator, _ counters: Counters, _ name: String, _ error: any Error) {
    if let e = error as? SwiftCacheError {
        switch e {
        case .retryLimitExceeded, .unavailable:
            counters.inc("\(name).gaveUp")
            return
        case .commitUnknownResult:
            counters.inc("\(name).unknown")
            return
        case .versionMismatch where name == "cas":
            return
        default: break
        }
    }
    sim.fail("\(name): unexpected error \(error)")
}

func bankWorkload(_ sim: Simulator, _ db: Database, _ counters: Counters, accounts: Int, until end: UInt64) async {
    while sim.now < end {
        do {
            if sim.random.chance(0.15) {
                // Audit: a consistent snapshot must always sum to the same total.
                let useRange = sim.random.chance(0.5)
                let total = try await db.transaction { tr -> Int in
                    if useRange {
                        let rows = try await tr.getRange(prefix: "bank/")
                        if rows.count != accounts { sim.fail("bank: range read returned \(rows.count) accounts") }
                        return try rows.reduce(0) { $0 + (try $1.decode(Int.self)) }
                    }
                    var sum = 0
                    for i in 0..<accounts { sum += try await tr.get("bank/\(pad(i))", as: Int.self) ?? 0 }
                    return sum
                }
                if total != accounts * 100 { sim.fail("bank: total \(total) != \(accounts * 100)") }
                counters.inc("bank.audits")
            } else {
                let a = Int(sim.random.below(UInt64(accounts)))
                var b = Int(sim.random.below(UInt64(accounts)))
                if a == b { b = (b + 1) % accounts }
                let amount = Int(sim.random.below(20)) + 1
                try await db.transaction { tr in
                    let ka = "bank/\(pad(a))", kb = "bank/\(pad(b))"
                    let va = try await tr.get(ka, as: Int.self) ?? 0
                    let vb = try await tr.get(kb, as: Int.self) ?? 0
                    guard va >= amount else { return }
                    try tr.set(ka, va - amount)
                    try tr.set(kb, vb + amount)
                }
                counters.inc("bank.commits")
            }
        } catch {
            handle(sim, counters, "bank", error)
        }
    }
}

func cycleWorkload(_ sim: Simulator, _ db: Database, _ counters: Counters, size: Int, until end: UInt64) async {
    while sim.now < end {
        do {
            let r = Int(sim.random.below(UInt64(size)))
            try await db.transaction { tr in
                func key(_ i: Int) -> String { "cycle/\(pad(i))" }
                guard let a = try await tr.get(key(r), as: Int.self),
                      let b = try await tr.get(key(a), as: Int.self),
                      let c = try await tr.get(key(b), as: Int.self) else {
                    sim.fail("cycle: missing key")
                    return
                }
                try tr.set(key(r), b)
                try tr.set(key(b), a)
                try tr.set(key(a), c)
            }
            counters.inc("cycle.commits")
        } catch {
            handle(sim, counters, "cycle", error)
        }
    }
}

func casWorkload(_ sim: Simulator, _ db: Database, _ counters: Counters, client: Int, until end: UInt64) async {
    while sim.now < end {
        do {
            if sim.random.chance(0.5) {
                // Database-level CAS (the getVersioned / compareAndSwap pattern), with the
                // log written in a second step only when we know we won.
                // Both happen in one transaction so the log is exact.
                try await db.transaction { tr in
                    guard let entry = try await tr.getVersioned("cas/counter") else {
                        sim.fail("cas: counter missing"); return
                    }
                    let n = try entry.decode(Int.self)
                    try await tr.compareAndSwap(key: "cas/counter", expectedVersion: entry.versionstamp, newValue: n + 1)
                    try tr.set("cas/log/\(pad(n + 1))", client)
                }
                counters.inc("cas.success")
            } else {
                // Two-step optimistic update across transactions: read the versionstamp,
                // then CAS later. Losing the race must surface as versionMismatch.
                guard let entry = try await db.getVersioned("cas/counter") else {
                    sim.fail("cas: counter missing"); continue
                }
                let n = try entry.decode(Int.self)
                do {
                    try await db.transaction { tr in
                        try await tr.compareAndSwap(key: "cas/counter", expectedVersion: entry.versionstamp, newValue: n + 1)
                        try tr.set("cas/log/\(pad(n + 1))", client)
                    }
                    counters.inc("cas.success")
                } catch let SwiftCacheError.versionMismatch(_, expected, actual) {
                    if expected == actual { sim.fail("cas: mismatch with equal versions") }
                    if let actual, actual <= entry.versionstamp { sim.fail("cas: versionstamp went backwards \(entry.versionstamp) -> \(actual)") }
                    counters.inc("cas.mismatch")
                }
            }
        } catch {
            handle(sim, counters, "cas", error)
        }
    }
}

func addWorkload(_ sim: Simulator, _ db: Database, _ counters: Counters, until end: UInt64) async {
    while sim.now < end {
        do {
            let stamp = try await db.add("add/total", 1)
            recordStamp(sim, counters, stamp, "add")
            counters.inc("add.commits")
        } catch {
            // An add whose retries gave up may or may not have been applied (once).
            handle(sim, counters, "add", error)
        }
    }
}

/// Each logical operation generates a fresh id *per attempt* (the case that breaks
/// non-idempotent retries) and creates `orders/<id>` plus increments a count. With
/// idempotency, exactly one attempt commits and its id is the one returned.
func ordersWorkload(_ sim: Simulator, _ db: Database, _ counters: Counters, until end: UInt64) async {
    while sim.now < end {
        do {
            let (id, stamp) = try await db.transactionWithVersionstamp(idempotent: true) { tr -> String in
                let id = String(sim.random.next(), radix: 36)
                try tr.set("orders/items/\(id)", "order")
                tr.add("orders/count", 1)
                return id
            }
            recordStamp(sim, counters, stamp, "orders")
            if counters.orders.contains(id) { sim.fail("orders: id \(id) returned twice") }
            counters.orders.insert(id)
            counters.inc("orders.commits")
        } catch {
            handle(sim, counters, "orders", error)
        }
    }
}

/// Increments `rt/c` (read-modify-write) and publishes each acknowledged value.
func realtimeWriter(_ sim: Simulator, _ db: Database, _ counters: Counters, until end: UInt64) async {
    while sim.now < end {
        do {
            let v = try await db.transaction { tr -> Int in
                let c = try await tr.get("rt/c", as: Int.self) ?? 0
                try tr.set("rt/c", c + 1)
                return c + 1
            }
            counters.values["rt.acked"] = max(counters["rt.acked"], v)
            counters.inc("realtime.writes")
        } catch {
            // Retries exhausted under faults; carry on.
        }
    }
}

struct StaleObservation: Error { let saw: Int, acked: Int }

/// Transactions started after an acknowledged write must reflect it (strict
/// serializability), however their read version was chosen: a read-only transaction
/// sees it; a body that throws on seeing a stale value never surfaces that error; a
/// committed read-modify-write never acted on a stale value.
func realtimeReader(_ sim: Simulator, _ db: Database, _ counters: Counters, until end: UInt64) async {
    var round = 0
    while sim.now < end {
        round += 1
        let acked = counters["rt.acked"]
        do {
            switch round % 3 {
            case 0:
                let v = try await db.transaction { tr in try await tr.get("rt/c", as: Int.self) ?? 0 }
                if v < acked { sim.fail("realtime: read-only transaction saw rt/c=\(v) after \(acked) was acknowledged") }
            case 1:
                try await db.transaction { tr in
                    let v = try await tr.get("rt/c", as: Int.self) ?? 0
                    if v < acked { throw StaleObservation(saw: v, acked: acked) }
                    try tr.set("rt/seen", v)
                }
            default:
                let v = try await db.transaction { tr -> Int in
                    let v = try await tr.get("rt/c", as: Int.self) ?? 0
                    try tr.set("rt/copy", v)
                    return v
                }
                if v < acked { sim.fail("realtime: committed read-modify-write read rt/c=\(v) after \(acked) was acknowledged") }
            }
            counters.inc("realtime.checks")
        } catch let e as StaleObservation {
            sim.fail("realtime: a transaction body observed rt/c=\(e.saw) after \(e.acked) was acknowledged, and that escaped")
        } catch {
            // Retries exhausted under faults: nothing was observed.
        }
    }
}

func historyWorkload(_ sim: Simulator, _ db: Database, _ counters: Counters, client: Int, until end: UInt64) async {
    let key = "history/\(client)"
    var written: [(value: Int, stamp: Versionstamp)] = []
    var lastStamp = Versionstamp.zero
    var i = 0
    while sim.now < end {
        do {
            i += 1
            let value = i
            let stamp = try await db.set(key, value, ttl: sim.random.chance(0.1) ? 0.5 : nil)
            recordStamp(sim, counters, stamp, "history")
            // Strict serializability: a read version obtained after an acknowledged commit
            // must include it.
            let rv = try await db.readVersion()
            if rv < stamp.version { sim.fail("history: read version \(rv) after commit at \(stamp.version) (stale)") }
            if stamp <= lastStamp { sim.fail("history: versionstamps not increasing \(lastStamp) -> \(stamp)") }
            lastStamp = stamp
            written.append((value, stamp))
            counters.inc("history.commits")

            // The key's versionstamp should be our commit's (nobody else writes it) —
            // unless the value has expired (ttl), in which case it's absent.
            if let e = try await db.getVersioned(key) {
                let v = try e.decode(Int.self)
                if e.versionstamp != stamp || v != value {
                    sim.fail("history: getVersioned returned \(e.versionstamp) want \(stamp)")
                }
            }

            // Re-read an earlier write at its commit version: must see exactly it.
            if written.count > 1 {
                let pick = written[Int(sim.random.below(UInt64(written.count)))]
                do {
                    let seen = try await db.read(atVersion: pick.stamp.version) { tr in try await tr.getVersioned(key) }
                    if let seen {
                        let v = try seen.decode(Int.self)
                        if seen.versionstamp != pick.stamp || v != pick.value {
                            sim.fail("history: read at \(pick.stamp.version) saw \(seen.versionstamp) want \(pick.stamp)")
                            for t in sim.roles(TLogRole.self) {
                                sim.trace("DEBUG \(t.context.endpoint) gen=\(t.generation) durable=\(t.durable) at \(pick.stamp.version): \(t.debugTags(at: pick.stamp.version).map { "\($0)" } ?? "absent")")
                            }
                            for s in sim.roles(StorageServer.self) {
                                let vs = s.debugVersions(Bytes(key.utf8))
                                if !vs.isEmpty || s.owned.contains(Slots.slot(for: Bytes(key.utf8))) {
                                    sim.trace("DEBUG \(s.context.endpoint) shard=\(s.shardID) tag=\(s.tag) applied=\(s.applied) owns=\(s.owned.contains(Slots.slot(for: Bytes(key.utf8)))) versions=\(vs.map { String($0.description.dropFirst(8)) })")
                                }
                            }
                        }
                    }
                    // nil is legal only if that write had a TTL that's expired by then (no:
                    // reading at its own commit version it can't be expired) — so flag it.
                    if seen == nil { sim.fail("history: read at \(pick.stamp.version) saw nothing, want \(pick.stamp)") }
                    counters.inc("history.historicalReads")
                } catch SwiftCacheError.transactionTooOld {
                    counters.inc("history.tooOld")
                    written.removeAll { $0.stamp <= pick.stamp }
                }
            }
            // Reading just before our first-remaining write must not see it.
            if let first = written.first, first.stamp.version > 1 {
                do {
                    let before = try await db.read(atVersion: first.stamp.version - 1) { tr in try await tr.getVersioned(key) }
                    if let before, before.versionstamp >= first.stamp {
                        sim.fail("history: read before \(first.stamp) saw \(before.versionstamp)")
                    }
                } catch SwiftCacheError.transactionTooOld {
                    written.removeFirst()
                }
            }
            if written.count > 50 { written.removeFirst(written.count - 50) }
        } catch {
            handle(sim, counters, "history", error)
        }
    }
}

// MARK: - Final invariant checks

func finalChecks(_ sim: Simulator, _ db: Database, _ counters: Counters, accounts: Int, cycleSize: Int) async {
    do {
        try await db.transaction { tr in
            // Bank.
            let rows = try await tr.getRange(prefix: "bank/")
            let total = try rows.reduce(0) { $0 + (try $1.decode(Int.self)) }
            if rows.count != accounts || total != accounts * 100 {
                sim.fail("final bank: \(rows.count) accounts, total \(total)")
            }
            // Cycle: follow pointers from 0; must return to 0 after exactly cycleSize steps.
            var seen = Set<Int>()
            var cur = 0
            for _ in 0..<cycleSize {
                guard let next = try await tr.get("cycle/\(pad(cur))", as: Int.self) else {
                    sim.fail("final cycle: missing key \(cur)"); return
                }
                seen.insert(cur)
                cur = next
            }
            if cur != 0 || seen.count != cycleSize { sim.fail("final cycle: not a single cycle (visited \(seen.count))") }

            // CAS: log entries must be exactly 1...counter.
            let counter = try await tr.get("cas/counter", as: Int.self) ?? -1
            let log = try await tr.getRange(prefix: "cas/log/", limit: 100_000)
            let nums = log.compactMap { Int($0.keyString.dropFirst("cas/log/".count)) }
            if nums != Array(1...max(1, counter)).prefix(counter).map({ $0 }) {
                sim.fail("final cas: counter \(counter) but log has \(nums.count) entries (first gap at \(zip(nums, 1...).first { $0 != $1 }?.1 ?? -1))")
            }
            if counter < counters["cas.success"] {
                sim.fail("final cas: counter \(counter) < acknowledged successes \(counters["cas.success"])")
            }

            // Idempotent adds: exactly the acknowledged ones (plus at most one per add whose
            // retries gave up with an unknown outcome).
            let added = Int(try await tr.getVersioned("add/total")?.int64 ?? 0)
            let addSlack = counters["add.gaveUp"] + counters["add.unknown"]
            if added < counters["add.commits"] || added > counters["add.commits"] + addSlack {
                sim.fail("final add: total \(added), acknowledged \(counters["add.commits"]), ambiguous \(addSlack)")
            }

            // Idempotent orders: every acknowledged id exists, and the count equals the
            // number of order keys (nothing applied twice).
            let items = try await tr.getRange(prefix: "orders/items/", limit: 100_000)
            let ids = Set(items.map { String($0.keyString.dropFirst("orders/items/".count)) })
            let count = Int(try await tr.getVersioned("orders/count")?.int64 ?? 0)
            if count != ids.count { sim.fail("final orders: count \(count) != \(ids.count) order keys (double-applied?)") }
            let missing = counters.orders.subtracting(ids)
            if !missing.isEmpty { sim.fail("final orders: \(missing.count) acknowledged orders missing") }
            let orderSlack = counters["orders.gaveUp"] + counters["orders.unknown"]
            if ids.count > counters.orders.count + orderSlack {
                sim.fail("final orders: \(ids.count) orders exist but only \(counters.orders.count) acknowledged (+\(orderSlack) ambiguous)")
            }
        }
        counters.inc("final.checked")
    } catch {
        sim.fail("final checks failed: \(error)")
    }
}
