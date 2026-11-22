import SwiftCache
import SwiftCacheCore
import SwiftCacheServer
import Synchronization

/// Shared mutable counters for workloads (single-threaded inside the simulator).
final class Counters: @unchecked Sendable {
    var values: [String: Int] = [:]
    func inc(_ k: String, _ n: Int = 1) { values[k, default: 0] += n }
    subscript(_ k: String) -> Int { values[k] ?? 0 }
}

/// Options for a full simulation run.
public struct SimulationOptions: Sendable {
    public var storageServers = 6
    public var replicationFactor = 3
    public var clients = 6
    /// Virtual seconds of workload + chaos.
    public var duration: UInt64 = 20_000_000
    /// Storage servers added part-way through (exercises shard formation + migration).
    public var scaleOutServers = 3
    public var chaos = true
    public var coordinatorCrashes = true
    public var verbose = false
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
/// * **add** — blind atomic adds; final total within [acknowledged, attempted].
/// * **history** — writes values and later re-reads them at the commit's version
///   (historical reads + versionstamps must match exactly).
public func runSimulation(seed: UInt64, options: SimulationOptions = SimulationOptions()) -> SimulationResult {
    var config = ClusterConfig()
    config.replicationFactor = options.replicationFactor
    config.mvccWindow = 5_000_000
    config.migrationChunkSlots = 4096
    let sim = Simulator(seed: seed, config: config)
    sim.verbose = options.verbose
    sim.addCoordinator()
    for _ in 0..<options.storageServers { sim.addStorageServer() }

    let counters = Counters()
    let start = sim.now
    let workEnd = start + options.duration
    let accounts = 20, cycleSize = 16

    // Wait for the cluster to come up, then set up data.
    let setupDB = Database(coordinator: sim.coordinatorEndpoint, transport: sim.makeClientTransport(name: "setup"))
    let ready = Mutex(false)
    sim.spawn {
        do {
            try await setupDB.transaction { tr in
                for i in 0..<accounts { try tr.set("bank/\(pad(i))", 100) }
                for i in 0..<cycleSize { try tr.set("cycle/\(pad(i))", (i + 1) % cycleSize) }
                try tr.set("cas/counter", 0)
                tr.add("add/total", 0)
            }
            ready.withLock { $0 = true }
        } catch {
            sim.fail("setup failed: \(error)")
        }
    }
    if !sim.run(for: 30_000_000, until: { ready.withLock { $0 } }) {
        sim.fail("cluster never became ready")
        return result(sim, counters)
    }
    sim.trace("setup complete")

    // Clients.
    for c in 0..<options.clients {
        var opts = DatabaseOptions()
        opts.requestTimeout = 1_000_000
        opts.maxRetries = 400
        let db = Database(coordinator: sim.coordinatorEndpoint, transport: sim.makeClientTransport(), options: opts)
        let kind = c % 5
        sim.spawn {
            switch kind {
            case 0: await bankWorkload(sim, db, counters, accounts: accounts, until: workEnd)
            case 1: await cycleWorkload(sim, db, counters, size: cycleSize, until: workEnd)
            case 2: await casWorkload(sim, db, counters, client: c, until: workEnd)
            case 3: await addWorkload(sim, db, counters, until: workEnd)
            default: await historyWorkload(sim, db, counters, client: c, until: workEnd)
            }
        }
    }

    // Scale out part-way through.
    if options.scaleOutServers > 0 {
        sim.at(start + options.duration / 4) {
            sim.trace("scaling out by \(options.scaleOutServers) storage servers")
            for _ in 0..<options.scaleOutServers { sim.addStorageServer() }
        }
    }
    if options.chaos { scheduleChaos(sim, options: options, until: workEnd - 2_000_000) }

    sim.run(until: workEnd)
    // Quiesce: heal everything, let clients finish.
    for ep in sim.storageEndpoints where !sim.isAlive(ep) { sim.restart(ep) }
    if !sim.isAlive(sim.coordinatorEndpoint) { sim.restart(sim.coordinatorEndpoint) }
    sim.run(until: workEnd + 15_000_000)

    // Final checks with a fresh client.
    let done = Mutex(false)
    let checkDB = Database(coordinator: sim.coordinatorEndpoint, transport: sim.makeClientTransport(name: "checker"))
    sim.spawn {
        await finalChecks(sim, checkDB, counters, accounts: accounts, cycleSize: cycleSize)
        done.withLock { $0 = true }
    }
    if !sim.run(for: 60_000_000, until: { done.withLock { $0 } }) {
        sim.fail("final checks did not complete (cluster unavailable after quiescing)")
    }

    // Liveness: workloads should have made progress.
    for k in ["bank.commits", "cycle.commits", "cas.success", "add.commits", "history.commits"] where options.clients >= 5 {
        if counters[k] == 0 { sim.fail("no progress: \(k) == 0") }
    }
    // Cluster health at the end.
    if let c = sim.coordinator {
        let st = c.status
        let shards = st.topology.shards
        if options.scaleOutServers > 0 && options.storageServers + options.scaleOutServers >= 2 * options.replicationFactor && shards.count < 2 {
            sim.fail("expected more than one shard after scale-out, have \(shards.count)")
        }
        for s in shards where s.replicas.isEmpty { sim.fail("shard \(s.id) has no replicas at end") }
    }
    return result(sim, counters)
}

private func result(_ sim: Simulator, _ counters: Counters) -> SimulationResult {
    SimulationResult(seed: sim.seed, failures: sim.failures, traceHash: sim.traceHash, stats: counters.values,
                     messages: sim.messagesDelivered, logTail: Array(sim.log.suffix(60)))
}

func pad(_ i: Int) -> String {
    let s = String(i)
    return String(repeating: "0", count: max(0, 4 - s.count)) + s
}

// MARK: - Chaos

/// Injects faults while making sure no shard ever loses every replica at once: a storage
/// server is only killed when the cluster is fully healthy (all replicas active, no data
/// movement), so at most one replica of any shard is down.
func scheduleChaos(_ sim: Simulator, options: SimulationOptions, until end: UInt64) {
    func healthy() -> Bool {
        // Ground truth first: the coordinator only notices failures after a timeout.
        guard sim.storageEndpoints.allSatisfy({ sim.isAlive($0) && !sim.isPartitioned($0) }) else { return false }
        guard let c = sim.coordinator else { return false }
        let st = c.status
        guard st.phase == "running", st.movesInProgress.isEmpty else { return false }
        return st.servers.allSatisfy { $0.state == "active" || $0.state == "unassigned" }
            && st.topology.shards.allSatisfy { $0.replicas.count >= options.replicationFactor }
    }
    func step() {
        guard sim.now < end else { return }
        let roll = sim.random.below(100)
        if roll < 35 {
            if healthy() {
                let candidates = sim.storageEndpoints.filter { sim.isAlive($0) }
                if !candidates.isEmpty {
                    let victim = candidates[Int(sim.random.below(UInt64(candidates.count)))]
                    sim.kill(victim)
                    let downFor = 200_000 + sim.random.below(3_000_000)
                    sim.after(downFor) { sim.restart(victim) }
                }
            }
        } else if roll < 55 {
            if healthy() {
                let candidates = sim.storageEndpoints.filter { sim.isAlive($0) }
                if !candidates.isEmpty {
                    let victim = candidates[Int(sim.random.below(UInt64(candidates.count)))]
                    sim.partition(victim, for: 100_000 + sim.random.below(2_500_000))
                }
            }
        } else if roll < 65 && options.coordinatorCrashes {
            if sim.isAlive(sim.coordinatorEndpoint) && sim.coordinator?.status.phase == "running" {
                sim.kill(sim.coordinatorEndpoint)
                sim.after(100_000 + sim.random.below(1_500_000)) { sim.restart(sim.coordinatorEndpoint) }
            }
        } else if roll < 72 && options.coordinatorCrashes {
            if sim.isAlive(sim.coordinatorEndpoint) {
                sim.partition(sim.coordinatorEndpoint, for: 50_000 + sim.random.below(800_000))
            }
        } else if roll < 85 {
            // Latency storm.
            sim.network.maxLatency = 20_000
            sim.after(1_000_000) { sim.network.maxLatency = 2_000 }
        }
        sim.after(500_000 + sim.random.below(1_500_000)) { step() }
    }
    sim.after(1_000_000) { step() }
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
        counters.inc("add.attempts")
        do {
            try await db.add("add/total", 1)
            counters.inc("add.commits")
        } catch {
            // An add whose retries gave up may still have been applied once.
            handle(sim, counters, "add", error)
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

            // Adds.
            let added = try await tr.getVersioned("add/total")?.int64 ?? 0
            if added < Int64(counters["add.commits"]) || added > Int64(counters["add.attempts"]) {
                sim.fail("final add: total \(added) outside [\(counters["add.commits"]), \(counters["add.attempts"])]")
            }
        }
        counters.inc("final.checked")
    } catch {
        sim.fail("final checks failed: \(error)")
    }
}
