import ArgumentParser
import Foundation
import SwiftCache

struct Connection: ParsableArguments {
    @Option(help: "Comma-separated coordinator addresses (default: $SWIFTCACHE_CLUSTER or localhost:4500).")
    var cluster: String = ProcessInfo.processInfo.environment["SWIFTCACHE_CLUSTER"] ?? "localhost:4500"

    func connect() async throws -> Database {
        try await Database.connect(cluster: cluster)
    }
}

@main
struct CLI: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "swiftcache-cli",
        abstract: "SwiftCache command-line client.",
        subcommands: [Status.self, Get.self, Set_.self, Del.self, Incr.self, CAS.self, Range_.self, ReadVersion.self,
                      WaitReady.self, Verify.self, Bench.self])
}

struct Status: AsyncParsableCommand {
    static let configuration = CommandConfiguration(abstract: "Show cluster status and per-server stats.")
    @OptionGroup var conn: Connection
    @Flag(help: "Also query every storage server for stats.") var stats = false

    func run() async throws {
        let db = try await conn.connect()
        let s = try await db.status()
        print("phase: \(s.phase)   generation: \(s.generation)   recoveries (this controller): \(s.recoveries)   replication: \(s.replicationFactor)")
        print("controller: \(s.clusterController)")
        print("master:     \(s.master)")
        print("proxies:    \(s.proxies.joined(separator: ", "))")
        print("resolvers:  \(s.resolvers.joined(separator: ", "))")
        print("logs:       \(s.tlogs.joined(separator: ", "))")
        var counts: [Int32: Int] = [:]
        for o in s.topology.slotOwner { counts[o, default: 0] += 1 }
        print("shards:")
        for shard in s.topology.shards {
            print("  shard \(shard.id): \(counts[shard.id] ?? 0) slots, replicas \(shard.replicas)")
        }
        print("storage:")
        for line in s.storage {
            var out = "  " + line
            if stats, let ep = line.split(separator: " ").first, let st = try? await db.storageStats(String(ep)) {
                out += "  keys=\(st.keyCount) versions=\(st.versionCount) bytes≈\(st.approximateBytes)"
            }
            print(out)
        }
        print("processes:")
        for p in s.processes {
            print("  \(p.address)  \(p.processClass)  \(p.alive ? "alive" : "DEAD")  roles: \(p.roles.map { $0.split(separator: "#").last.map(String.init) ?? $0 }.joined(separator: " "))")
        }
        for m in s.movesInProgress { print("moving: \(m)") }
    }
}

struct Get: AsyncParsableCommand {
    static let configuration = CommandConfiguration(abstract: "Read a key (with its versionstamp).")
    @OptionGroup var conn: Connection
    @Argument var key: String
    @Option(help: "Read at a historical version.") var atVersion: UInt64?

    func run() async throws {
        let db = try await conn.connect()
        let entry: VersionedEntry?
        if let v = atVersion {
            entry = try await db.read(atVersion: v) { try await $0.getVersioned(key) }
        } else {
            entry = try await db.getVersioned(key)
        }
        guard let entry else { print("(nil)"); throw ExitCode(1) }
        print(entry.string)
        print("versionstamp: \(entry.versionstamp)  (version \(entry.versionstamp.version))")
    }
}

struct Set_: AsyncParsableCommand {
    static let configuration = CommandConfiguration(commandName: "set", abstract: "Write a key.")
    @OptionGroup var conn: Connection
    @Argument var key: String
    @Argument var value: String
    @Option(help: "Expire after this many seconds.") var ttl: Double?

    func run() async throws {
        let db = try await conn.connect()
        let stamp = try await db.set(key, value, ttl: ttl)
        print("OK versionstamp: \(stamp)")
    }
}

struct Del: AsyncParsableCommand {
    static let configuration = CommandConfiguration(abstract: "Delete a key.")
    @OptionGroup var conn: Connection
    @Argument var key: String

    func run() async throws {
        let db = try await conn.connect()
        print("OK versionstamp: \(try await db.clear(key))")
    }
}

struct Incr: AsyncParsableCommand {
    static let configuration = CommandConfiguration(abstract: "Atomically add to an Int64 counter.")
    @OptionGroup var conn: Connection
    @Argument var key: String
    @Option var by: Int64 = 1

    func run() async throws {
        let db = try await conn.connect()
        _ = try await db.add(key, by)
        print(try await db.getVersioned(key)?.int64 ?? 0)
    }
}

struct CAS: AsyncParsableCommand {
    static let configuration = CommandConfiguration(abstract: "Compare-and-swap on a key's versionstamp ('none' = must not exist).")
    @OptionGroup var conn: Connection
    @Argument var key: String
    @Argument(help: "Expected versionstamp (20 hex chars) or 'none'.") var expected: String
    @Argument var value: String

    func run() async throws {
        let db = try await conn.connect()
        let exp: Versionstamp?
        if expected == "none" { exp = nil } else {
            guard let v = Versionstamp(hex: expected) else { throw ValidationError("bad versionstamp") }
            exp = v
        }
        do {
            print("OK versionstamp: \(try await db.compareAndSwap(key: key, expectedVersion: exp, newValue: value))")
        } catch let e as SwiftCacheError {
            print("FAILED: \(e)")
            throw ExitCode(2)
        }
    }
}

struct Range_: AsyncParsableCommand {
    static let configuration = CommandConfiguration(commandName: "range", abstract: "List keys with a prefix.")
    @OptionGroup var conn: Connection
    @Argument var prefix: String
    @Option var limit = 100

    func run() async throws {
        let db = try await conn.connect()
        for e in try await db.getRange(prefix: prefix, limit: limit) {
            print("\(e.keyString) = \(e.string)  [\(e.versionstamp)]")
        }
    }
}

struct ReadVersion: AsyncParsableCommand {
    static let configuration = CommandConfiguration(commandName: "read-version", abstract: "Print the latest committed version.")
    @OptionGroup var conn: Connection
    func run() async throws {
        print(try await conn.connect().readVersion())
    }
}

struct WaitReady: AsyncParsableCommand {
    static let configuration = CommandConfiguration(commandName: "wait-ready", abstract: "Wait until the cluster is running with N fully-replicated shards and no data movement.")
    @OptionGroup var conn: Connection
    @Option var shards = 1
    @Option(help: "Also wait until the transaction-system generation is at least this.") var minGeneration: UInt64 = 0
    @Option var timeout: Double = 120

    func run() async throws {
        let deadline = Date().addingTimeInterval(timeout)
        var last = ""
        while Date() < deadline {
            if let db = try? await conn.connect(), let s = try? await db.status() {
                let full = s.topology.shards.filter { $0.replicas.count >= Int(s.replicationFactor) }
                let allAssigned = !s.topology.slotOwner.contains(-1)
                last = "phase=\(s.phase) generation=\(s.generation) shards=\(s.topology.shards.count) fullyReplicated=\(full.count) moves=\(s.movesInProgress.count)"
                if s.phase == "running" && s.generation >= minGeneration && full.count >= shards && s.topology.shards.count == full.count
                    && s.movesInProgress.isEmpty && allAssigned {
                    // Balanced?
                    var counts: [Int32: Int] = [:]
                    for o in s.topology.slotOwner { counts[o, default: 0] += 1 }
                    if let mx = counts.values.max(), let mn = counts.values.min(), mx - mn <= 1 || counts.count == 1 {
                        print("ready: \(last)")
                        return
                    }
                }
            }
            try await Task.sleep(nanoseconds: 500_000_000)
        }
        print("not ready: \(last)")
        throw ExitCode(1)
    }
}

struct Bench: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        abstract: "Throughput/latency benchmark (comparable to redis-benchmark: no pipelining, fixed value size, random keys).")
    @OptionGroup var conn: Connection
    @Option(help: "get, set, mixed, or txn (read-modify-write of 2 keys).") var mode = "mixed"
    @Option var concurrency = 32
    @Option var seconds: Double = 10
    @Option var keys = 10_000
    @Option(help: "Value size in bytes.") var valueSize = 100
    @Option(help: "Fraction of reads in mixed mode.") var readRatio = 0.8
    @Option(help: "Client event-loop threads.") var threads = 4
    @Flag(help: "Write every key before measuring.") var preload = false

    func run() async throws {
        let db = try await Database.connect(coordinators: conn.cluster.split(separator: ",").map(String.init),
                                            threads: threads)
        let keys = self.keys, mode = self.mode, readRatio = self.readRatio
        let value = Bytes(repeating: UInt8(ascii: "x"), count: valueSize)
        if preload || mode == "get" || mode == "read-at" {
            try await withThrowingTaskGroup(of: Void.self) { g in
                for w in 0..<concurrency {
                    g.addTask {
                        var i = w
                        while i < keys {
                            try await db.transaction { tr in
                                for j in stride(from: i, to: min(keys, i + 50 * concurrency), by: concurrency) {
                                    tr.set("bench/\(j)", bytes: value)
                                }
                            }
                            i += 50 * concurrency
                        }
                    }
                }
                try await g.waitForAll()
            }
        }
        let pinned = try await db.readVersion()
        let start = Date()
        let end = start.addingTimeInterval(seconds)
        let results = try await withThrowingTaskGroup(of: (Int, Int, [Double]).self) { group in
            for _ in 0..<concurrency {
                group.addTask {
                    var ops = 0, errors = 0
                    var latencies: [Double] = []
                    latencies.reserveCapacity(100_000)
                    while Date() < end {
                        let k = "bench/\(Int.random(in: 0..<keys))"
                        let t0 = Date()
                        do {
                            switch mode {
                            case "grv": _ = try await db.createTransaction().readVersion()
                            case "read-at": _ = try await db.read(atVersion: pinned) { try await $0.get(k) }
                            case "get": _ = try await db.get(k)
                            case "set": try await db.set(k, value)
                            case "txn":
                                let k2 = "bench/\(Int.random(in: 0..<keys))"
                                try await db.transaction { tr in
                                    let a = try await tr.get(k) ?? value
                                    _ = try await tr.get(k2)
                                    tr.set(k2, bytes: a)
                                }
                            default:
                                if Double.random(in: 0..<1) < readRatio { _ = try await db.get(k) } else { try await db.set(k, value) }
                            }
                            ops += 1
                            latencies.append(Date().timeIntervalSince(t0) * 1000)
                        } catch { errors += 1 }
                    }
                    return (ops, errors, latencies)
                }
            }
            var all: [(Int, Int, [Double])] = []
            for try await r in group { all.append(r) }
            return all
        }
        let elapsed = Date().timeIntervalSince(start)
        let ops = results.reduce(0) { $0 + $1.0 }, errors = results.reduce(0) { $0 + $1.1 }
        let lat = results.flatMap(\.2).sorted()
        func pct(_ p: Double) -> String { lat.isEmpty ? "-" : String(format: "%.2f", lat[min(lat.count - 1, Int(Double(lat.count) * p))]) }
        print("\(mode): " + String(format: "%.0f ops/s", Double(ops) / elapsed)
              + "  p50=\(pct(0.5))ms p99=\(pct(0.99))ms p99.9=\(pct(0.999))ms  errors=\(errors)")
    }
}
