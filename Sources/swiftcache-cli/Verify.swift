import ArgumentParser
import Foundation
import SwiftCache

/// End-to-end correctness checks against a live cluster (used by `scripts/k3s-test.sh`).
struct Verify: AsyncParsableCommand {
    static let configuration = CommandConfiguration(abstract: "Run end-to-end correctness checks against a live cluster.")
    @OptionGroup var conn: Connection
    @Option(help: "Expect at least this many shards.") var minShards = 1
    @Option(help: "Concurrent tasks for the stress checks.") var concurrency = 16

    struct Failure: Error, CustomStringConvertible {
        var description: String
        init(_ d: String) { description = d }
    }

    func run() async throws {
        let db = try await conn.connect()
        let ns = "verify/\(UInt32.random(in: 0..<UInt32.max))/"
        final class Tally { var passed = 0, failed = 0 }
        let tally = Tally()

        func check(_ name: String, _ body: () async throws -> Void) async {
            let t0 = Date()
            do {
                try await body()
                tally.passed += 1
                print("✓ \(name) (\(Int(Date().timeIntervalSince(t0) * 1000))ms)")
            } catch {
                tally.failed += 1
                print("✗ \(name): \(error)")
            }
        }
        func expect(_ cond: Bool, _ msg: @autoclosure () -> String) throws {
            if !cond { throw Failure(msg()) }
        }

        await check("set/get round trip with versionstamps") {
            let s1 = try await db.set(ns + "a", "hello")
            let e = try await db.getVersioned(ns + "a")
            try expect(e?.string == "hello", "got \(String(describing: e?.string))")
            try expect(e?.versionstamp == s1, "versionstamp \(String(describing: e?.versionstamp)) != \(s1)")
            let s2 = try await db.set(ns + "a", "world")
            try expect(s2 > s1, "versionstamps not increasing: \(s1) then \(s2)")
        }

        await check("Codable values") {
            struct User: Codable, Equatable { var name: String; var age: Int }
            try await db.set(ns + "user", User(name: "Ada", age: 36))
            let u = try await db.get(ns + "user", as: User.self)
            try expect(u == User(name: "Ada", age: 36), "got \(String(describing: u))")
        }

        await check("compareAndSwap succeeds on matching versionstamp, fails on stale") {
            struct User: Codable { var name: String; var visits: Int }
            let key = ns + "users/123"
            try await db.set(key, User(name: "Ada", visits: 0))
            let entry = try await db.getVersioned(key)!
            var updated = try entry.decode(User.self)
            updated.visits += 1
            let newStamp = try await db.compareAndSwap(key: key, expectedVersion: entry.versionstamp, newValue: updated)
            try expect(newStamp > entry.versionstamp, "new versionstamp not greater")
            do {
                try await db.compareAndSwap(key: key, expectedVersion: entry.versionstamp, newValue: updated)
                throw Failure("stale compareAndSwap succeeded")
            } catch SwiftCacheError.versionMismatch(_, let expected, let actual) {
                try expect(expected == entry.versionstamp && actual == newStamp, "mismatch carried wrong versions")
            }
            let after = try await db.get(key, as: User.self)
            try expect(after?.visits == 1, "visits = \(String(describing: after?.visits))")
        }

        await check("compareAndSwap with nil expected = create-if-absent") {
            let key = ns + "lock"
            try await db.compareAndSwap(key: key, expectedVersion: nil, newValue: "owner-1")
            do {
                try await db.compareAndSwap(key: key, expectedVersion: nil, newValue: "owner-2")
                throw Failure("second create-if-absent succeeded")
            } catch SwiftCacheError.versionMismatch {}
            try expect(try await db.get(key, as: String.self) == "owner-1", "lock owner changed")
        }

        await check("CAS inside a multi-key transaction is atomic") {
            let a = ns + "txcas/a", b = ns + "txcas/b"
            let sa = try await db.set(a, "1")
            try await db.set(a, "2")  // makes `sa` stale
            do {
                try await db.transaction { tr in
                    tr.set(b, bytes: Array("written".utf8))
                    try await tr.compareAndSwap(key: a, expectedVersion: sa, newValue: "3")
                }
                throw Failure("stale CAS in transaction succeeded")
            } catch SwiftCacheError.versionMismatch {}
            try expect(try await db.get(b) == nil, "other write in failed CAS transaction was applied")
        }

        await check("conflicting transactions: the later commit fails with notCommitted") {
            let k = ns + "conflict", other = ns + "conflict-other"
            try await db.set(k, "0")
            let t1 = db.createTransaction()
            _ = try await t1.get(k)
            try await db.set(k, "1")  // commits after t1's read version
            t1.set(other, bytes: [1])
            do {
                try await t1.commit()
                throw Failure("conflicting transaction committed")
            } catch SwiftCacheError.notCommitted {}
        }

        await check("snapshot reads don't conflict") {
            let k = ns + "snap"
            try await db.set(k, "0")
            let t1 = db.createTransaction()
            _ = try await t1.get(k, snapshot: true)
            try await db.set(k, "1")
            t1.set(ns + "snap-other", bytes: [1])
            try await t1.commit()
        }

        await check("read-your-writes and atomic add inside a transaction") {
            let k = ns + "ryw"
            try await db.transaction { tr in
                try tr.set(k, "local")
                let v = try await tr.get(k, as: String.self)
                try expect(v == "local", "RYW got \(String(describing: v))")
                tr.add(ns + "ryw-counter", 5)
                tr.add(ns + "ryw-counter", 2)
                let c = try await tr.get(ns + "ryw-counter")
                try expect(c.map { LittleEndian.int64($0) } == 7, "counter RYW")
            }
        }

        await check("historical reads at a commit version") {
            let k = ns + "history"
            let s1 = try await db.set(k, "v1")
            let s2 = try await db.set(k, "v2")
            let old = try await db.read(atVersion: s1.version) { try await $0.getVersioned(k) }
            try expect(old?.string == "v1" && old?.versionstamp == s1, "at \(s1.version): \(String(describing: old?.string))")
            let new = try await db.read(atVersion: s2.version) { try await $0.get(k, as: String.self) }
            try expect(new == "v2", "at \(s2.version): \(String(describing: new))")
            let before = try await db.read(atVersion: s1.version - 1) { try await $0.get(k) }
            try expect(before == nil, "before first write saw a value")
        }

        await check("readVersion is monotonic and old versions are rejected") {
            let v1 = try await db.readVersion()
            try await db.set(ns + "rv", "x")
            let v2 = try await db.readVersion()
            try expect(v2 > v1, "read version did not advance: \(v1) -> \(v2)")
            do {
                _ = try await db.read(atVersion: v1 - 3_600_000_000) { try await $0.get(ns + "rv") }
                throw Failure("hour-old read version was accepted")
            } catch SwiftCacheError.transactionTooOld {}
        }

        await check("range reads across shards") {
            try await db.transaction { tr in
                for i in 0..<300 { try tr.set(ns + "range/\(String(format: "%04d", i))", "\(i)") }
            }
            let rows = try await db.getRange(prefix: ns + "range/", limit: 1000)
            try expect(rows.count == 300, "got \(rows.count) rows")
            try expect(rows.map(\.keyString) == rows.map(\.keyString).sorted(), "not sorted")
            let page = try await db.getRange(prefix: ns + "range/", limit: 50)
            try expect(page.count == 50 && page.last?.keyString.hasSuffix("0049") == true, "limit page wrong")
        }

        await check("TTL expiry") {
            let k = ns + "ttl"
            try await db.set(k, "temp", ttl: 1)
            try expect(try await db.get(k) != nil, "missing right after set")
            try await Task.sleep(nanoseconds: 2_000_000_000)
            try expect(try await db.get(k) == nil, "still present after TTL")
        }

        await check("concurrent atomic adds never conflict or get lost") {
            let k = ns + "adds"
            let perTask = 25
            try await withThrowingTaskGroup(of: Void.self) { g in
                for _ in 0..<concurrency {
                    g.addTask { for _ in 0..<perTask { try await db.add(k, 1) } }
                }
                try await g.waitForAll()
            }
            let total = try await db.getVersioned(k)?.int64 ?? 0
            try expect(total == Int64(concurrency * perTask), "total \(total) != \(concurrency * perTask)")
        }

        await check("concurrent conflicting transfers preserve the total (serializability)") {
            let accounts = 10
            try await db.transaction { tr in
                for i in 0..<accounts { try tr.set(ns + "bank/\(i)", 100) }
            }
            try await withThrowingTaskGroup(of: Void.self) { g in
                for _ in 0..<concurrency {
                    g.addTask {
                        for _ in 0..<20 {
                            let a = Int.random(in: 0..<accounts)
                            let b = (a + Int.random(in: 1..<accounts)) % accounts
                            try await db.transaction { tr in
                                let va = try await tr.get(ns + "bank/\(a)", as: Int.self) ?? 0
                                let vb = try await tr.get(ns + "bank/\(b)", as: Int.self) ?? 0
                                guard va >= 5 else { return }
                                try tr.set(ns + "bank/\(a)", va - 5)
                                try tr.set(ns + "bank/\(b)", vb + 5)
                            }
                        }
                    }
                }
                try await g.waitForAll()
            }
            let rows = try await db.getRange(prefix: ns + "bank/")
            let total = try rows.reduce(0) { $0 + (try $1.decode(Int.self)) }
            try expect(total == accounts * 100, "total \(total)")
        }

        await check("concurrent CAS increments: no lost updates") {
            let k = ns + "cas-counter"
            try await db.set(k, 0)
            let successes = try await withThrowingTaskGroup(of: Int.self) { g in
                for _ in 0..<concurrency {
                    g.addTask {
                        var wins = 0
                        for _ in 0..<10 {
                            while true {
                                let e = try await db.getVersioned(k)!
                                do {
                                    try await db.compareAndSwap(key: k, expectedVersion: e.versionstamp, newValue: try e.decode(Int.self) + 1)
                                    wins += 1
                                    break
                                } catch SwiftCacheError.versionMismatch { continue }
                            }
                        }
                        return wins
                    }
                }
                return try await g.reduce(0, +)
            }
            let final = try await db.get(k, as: Int.self)
            try expect(final == successes && successes == concurrency * 10, "final \(String(describing: final)), successes \(successes)")
        }

        await check("cluster has ≥ \(minShards) shard(s) and data is spread across them") {
            let s = try await db.status()
            try expect(s.topology.shards.count >= minShards, "only \(s.topology.shards.count) shards")
            if s.topology.shards.count > 1 {
                var withKeys = 0
                for shard in s.topology.shards {
                    guard let r = shard.replicas.first else { continue }
                    if try await db.storageStats(r).keyCount > 0 { withKeys += 1 }
                }
                try expect(withKeys >= 2, "keys on only \(withKeys) shard(s)")
            }
        }

        print("\(tally.passed) passed, \(tally.failed) failed")
        if tally.failed > 0 { throw ExitCode.failure }
    }
}

enum LittleEndian {
    static func int64(_ b: [UInt8]) -> Int64 {
        var v: UInt64 = 0
        for i in 0..<min(8, b.count) { v |= UInt64(b[i]) << (8 * UInt64(i)) }
        return Int64(bitPattern: v)
    }
}
