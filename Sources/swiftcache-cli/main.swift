import ArgumentParser
import Foundation
import SwiftCache

struct Connection: ParsableArguments {
    @Option(help: "Coordinator endpoint host:port (default: $SWIFTCACHE_COORDINATOR or localhost:7400).")
    var coordinator: String = ProcessInfo.processInfo.environment["SWIFTCACHE_COORDINATOR"] ?? "localhost:7400"

    func connect() async throws -> Database {
        try await Database.connect(coordinator: coordinator)
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
        print("phase: \(s.phase)   epoch: \(s.coordinatorEpoch)   replication: \(s.replicationFactor)")
        print("versions: latest=\(s.latestVersion) committed=\(s.committedVersion)")
        print("commits: \(s.commitsTotal)   conflicts: \(s.conflictsTotal)")
        var counts: [Int32: Int] = [:]
        for o in s.topology.slotOwner { counts[o, default: 0] += 1 }
        print("shards:")
        for shard in s.topology.shards {
            print("  shard \(shard.id): \(counts[shard.id] ?? 0) slots, replicas \(shard.replicas)")
        }
        print("servers:")
        for server in s.servers {
            var line = "  \(server.endpoint)  shard=\(server.shardID)  \(server.state)"
            if stats, let st = try? await db.storageStats(server.endpoint) {
                line += "  keys=\(st.keyCount) versions=\(st.versionCount) bytes≈\(st.approximateBytes) applied=\(st.appliedVersion)"
            }
            print(line)
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
    @Option var timeout: Double = 120

    func run() async throws {
        let deadline = Date().addingTimeInterval(timeout)
        var last = ""
        while Date() < deadline {
            if let db = try? await conn.connect(), let s = try? await db.status() {
                let full = s.topology.shards.filter { $0.replicas.count >= Int(s.replicationFactor) }
                let allAssigned = !s.topology.slotOwner.contains(-1)
                last = "phase=\(s.phase) shards=\(s.topology.shards.count) fullyReplicated=\(full.count) moves=\(s.movesInProgress.count)"
                if s.phase == "running" && full.count >= shards && s.topology.shards.count == full.count
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
    static let configuration = CommandConfiguration(abstract: "Simple throughput benchmark.")
    @OptionGroup var conn: Connection
    @Option var concurrency = 32
    @Option var seconds: Double = 10
    @Option var keys = 10_000
    @Option(help: "Fraction of operations that are reads.") var readRatio = 0.8

    func run() async throws {
        let db = try await conn.connect()
        let end = Date().addingTimeInterval(seconds)
        let keys = self.keys, readRatio = self.readRatio
        let results = try await withThrowingTaskGroup(of: (Int, Int, [Double]).self) { group in
            for _ in 0..<concurrency {
                group.addTask {
                    var ops = 0, errors = 0
                    var latencies: [Double] = []
                    while Date() < end {
                        let k = "bench/\(Int.random(in: 0..<keys))"
                        let t0 = Date()
                        do {
                            if Double.random(in: 0..<1) < readRatio { _ = try await db.get(k) } else { try await db.set(k, "v\(ops)") }
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
        let ops = results.reduce(0) { $0 + $1.0 }, errors = results.reduce(0) { $0 + $1.1 }
        let lat = results.flatMap(\.2).sorted()
        func pct(_ p: Double) -> String { lat.isEmpty ? "-" : String(format: "%.2fms", lat[min(lat.count - 1, Int(Double(lat.count) * p))]) }
        print(String(format: "%.0f ops/s", Double(ops) / seconds) + "  errors=\(errors)  p50=\(pct(0.5)) p99=\(pct(0.99))")
    }
}
