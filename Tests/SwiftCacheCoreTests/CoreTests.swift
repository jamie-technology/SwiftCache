import Testing
@testable import SwiftCacheCore
@testable import SwiftCacheServer

@Suite struct VersionstampTests {
    @Test func orderingAndEncoding() {
        let a = Versionstamp(version: 100, order: 2), b = Versionstamp(version: 100, order: 3), c = Versionstamp(version: 101, order: 0)
        #expect(a < b && b < c)
        #expect(a.bytes.count == 10)
        #expect(bytesLess(a.bytes, b.bytes) && bytesLess(b.bytes, c.bytes), "byte encoding sorts like the stamp")
        #expect(Versionstamp(bytes: c.bytes) == c)
        #expect(Versionstamp(hex: c.description) == c)
    }
}

@Suite struct SlotTests {
    @Test func redisCompatibleHashing() {
        // Known Redis Cluster values.
        #expect(Slots.slot(for: Bytes("foo".utf8)) == 12182)
        #expect(Slots.slot(for: Bytes("123456789".utf8)) == 12739)
    }

    @Test func hashTagsColocate() {
        #expect(Slots.slot(for: Bytes("{user:1}:profile".utf8)) == Slots.slot(for: Bytes("{user:1}:sessions".utf8)))
        #expect(Slots.slot(for: Bytes("{}x".utf8)) == Slots.slot(for: Bytes("{}x".utf8)))
    }

    @Test func slotSetRangesRoundTrip() {
        var rng = SystemRandomNumberGenerator()
        for _ in 0..<50 {
            var s = SlotSet()
            for _ in 0..<Int.random(in: 0..<300, using: &rng) {
                let start = Int.random(in: 0..<Slots.count, using: &rng)
                for i in start..<min(Slots.count, start + Int.random(in: 1..<200, using: &rng)) { s.insert(i) }
            }
            #expect(SlotSet(ranges: s.ranges) == s)
        }
        #expect(SlotSet(ranges: SlotSet.all.ranges) == .all)
        #expect(SlotSet.all.ranges.count == 1)
    }

    @Test func slotSetRanges() {
        var s = SlotSet([1, 2, 3, 10, 16383])
        #expect(s.count == 5)
        #expect(SlotSet(ranges: s.ranges) == s)
        s.subtract(SlotSet([2]))
        #expect(!s.contains(2) && s.contains(3))
        #expect(SlotSet.all.count == Slots.count)
    }
}

@Suite struct CodecTests {
    @Test func everyMessageRoundTrips() throws {
        var topo = Topology.empty
        topo.slotOwner[5] = 1
        topo.shards = [.init(id: 1, replicas: ["a:1#storage-1", "b:2#storage-2"])]
        var map = ShardMap()
        map.apply(.createShard(shard: 0, members: [ShardMember(tag: 1, endpoint: "s:1#storage-a", active: true)], slots: .all), at: 5)
        let entry = LogEntry(version: 10, messages: [LogMessage(order: 0, mutation: .set(key: [1], value: [2], ttl: 0), tags: [1, 2])],
                             meta: [TaggedMeta(op: .snapshot(map), tags: [metadataTag])])
        let messages: [Message] = [
            .getReadVersion(rid: 1),
            .readVersionReply(rid: 2, version: 99),
            .commit(rid: 3, CommitRequest(readVersion: 7, readConflictKeys: [[1]], readConflictRanges: [KeyRange(begin: [1], end: [2])],
                                          writeConflictKeys: [[3]], mutations: [.set(key: [1], value: [2], ttl: 5), .clear(key: [3]), .add(key: [4], delta: -9)],
                                          idempotencyID: [9, 9], idempotencyRetention: 60, attemptID: 77,
                                          metaOps: [.addReplica(shard: 1, member: ShardMember(tag: 3, endpoint: "x", active: false), moveID: 4, sources: ["y"], slots: SlotSet([1]))])),
            .commitReply(rid: 4, .committed(Versionstamp(version: 9, order: 1))),
            .commitReply(rid: 5, .failed(.notCommitted)),
            .clientInfoReply(rid: 6, ClientInfo(generation: 3, proxies: ["p:1#proxy-1"], topology: topo)),
            .readRange(rid: 7, range: KeyRange(begin: [1], end: [2]), version: 3, limit: 10),
            .rangeReply(rid: 9, entries: [KeyValueEntry(key: [1], value: [2], stamp: .zero)], more: true, servedSlots: SlotSet([1, 2])),
            .candidacy(rid: 10, LeaderInfo(endpoint: "c:1#cc-1", changeID: 42)),
            .nominationReply(rid: 11, nil),
            .genReadReply(rid: 12, ok: true, readBallot: Ballot(round: 2, id: 3), writeBallot: .zero,
                          value: CoreState(generation: 4, tlogs: ["l:1#tlog-1"], startVersion: 100).bytes),
            .registerWorker(rid: 13, WorkerRegistration(worker: "w:1#worker", processClass: .storage, roles: ["w:1#storage-1"],
                                                        storage: StorageReport(endpoint: "w:1#storage-1", tag: 5, appliedVersion: 6))),
            .recruit(rid: 14, .tlog(TLogSpec(generation: 2, startVersion: 9, seedFrom: ["l:1#tlog-a"], initialTags: [1]))),
            .recruit(rid: 15, .proxy(ProxySpec(generation: 2, index: 1, startVersion: 9, master: "m", resolvers: ["r"], tlogs: ["t"],
                                               clusterController: "cc", shardMap: map, writeSnapshot: true))),
            .recoveryComplete(rid: 16, RecoveryResult(recoveryID: 1, generation: 2, master: "m", proxies: ["p"], resolvers: ["r"],
                                                      tlogs: ["t"], shardMap: map, startVersion: 9)),
            .resolve(rid: 17, ResolveRequest(generation: 2, proxyIndex: 0, version: 10, prevVersion: 9, lastSeenVersion: 8,
                                             txns: [ResolveTxn(readVersion: 5, readKeys: [[1]], readRanges: [], writeKeys: [[2]])],
                                             metaOps: [.transfer(moveID: 1, slots: SlotSet([3]), from: 0, to: 1)])),
            .resolveReply(rid: 18, ResolveReply(version: 10, prevVersion: 9, verdicts: [0, 1, 2], stateOps: [StateOps(version: 9, ops: [.removeShard(shard: 4)])])),
            .versionAndResolve(rid: 25, requestNum: 3, ResolveRequest(generation: 2, proxyIndex: 1, version: 0, prevVersion: 0, lastSeenVersion: 8,
                                                                      txns: [], metaOps: [], replyTo: "p:1#proxy-1")),
            .readBatch(rid: 26, items: [ReadItem(key: [1], version: 5), ReadItem(key: [2, 3], version: 6)]),
            .readBatchReply(rid: 27, results: [.value([9], Versionstamp(version: 5, order: 1)), .value(nil, .zero), .error(.wrongShard)]),
            .commitBatch(rid: 28, [CommitRequest(readVersion: 3, mutations: [.add(key: [1], delta: 2)])]),
            .commitBatchReply(rid: 29, [.committed(Versionstamp(version: 7, order: 0)), .failed(.notCommitted)]),
            .tlogDurable(generation: 2, version: 10), .committedVersion(generation: 2, version: 10),
            .tlogCommitted(generation: 2, version: 10), .tlogNeed(tag: 3, need: 11), .tlogUnsubscribe(rid: 4, tag: 3), .tlogLease(rid: 30, generation: 2),
            .tlogPeek(rid: 31, tag: 4, from: 5, upTo: 0, need: 9),
            .tlogPush(rid: 19, TLogPush(generation: 2, prevVersion: 9, entry: entry, knownCommitted: 8)),
            .tlogPeekReply(rid: 20, PeekReply(status: .ok, entries: [PeekEntry(version: 10, mutations: [(0, .clear(key: [1]))], meta: [])], end: 10)),
            .tlogLockReply(rid: 21, LockReply(known: true, generation: 2, durableVersion: 10, knownCommitted: 9)),
            .tlogFetchReply(rid: 22, FetchReply(ok: true, entries: [entry], popped: [(1, 5), (metadataTag, 0)])),
            .snapshotChunk(rid: 23, SnapshotChunk(version: 5, items: [.init(key: [1], versions: [VersionedValue(stamp: .zero, value: nil, expiresAt: 3)])], done: true)),
            .reportCommitted(rid: 24, version: 10),
            .halt(role: "x:1#proxy-2"),
        ]
        for m in messages {
            #expect(try Message.decode(m.encode()) == m, "\(m.name)")
        }
    }

    @Test func rejectsGarbage() {
        #expect(throws: (any Error).self) { try Message.decode([200, 1, 2]) }
        #expect(throws: (any Error).self) { try Message.decode([3, 1]) }
    }

    @Test func idempotencyBatchRecords() {
        let entries = [SystemKeys.IdempotencyEntry(id: Bytes(repeating: 7, count: 16), attemptID: 99, order: 3),
                       SystemKeys.IdempotencyEntry(id: [1, 2], attemptID: .max, order: 0)]
        let v = SystemKeys.idempotencyBatchValue(entries)
        #expect(SystemKeys.parseIdempotencyBatchValue(v) == entries)
        #expect(SystemKeys.parseIdempotencyBatchValue(Array(v.dropLast())) == nil)
        // Keys sort in version order (range reads by version rely on it).
        let a = SystemKeys.idempotencyBatch(255), b = SystemKeys.idempotencyBatch(256), c = SystemKeys.idempotencyBatch(1 << 40)
        #expect(bytesLess(a, b) && bytesLess(b, c))
        #expect(SystemKeys.idempotencyBatchVersion(c) == 1 << 40)
        #expect(SystemKeys.isSystem(a) && SystemKeys.isIdempotencyBatch(a) && !SystemKeys.isIdempotencyBatch(SystemKeys.idempotency([1])))
    }
}

@Suite struct ShardMapTests {
    @Test func routingFollowsMetadata() {
        var m = ShardMap()
        m.apply(.createShard(shard: 0, members: [ShardMember(tag: 1, endpoint: "a", active: true), ShardMember(tag: 2, endpoint: "b", active: true)], slots: .all), at: 10)
        #expect(m.tags(forSlot: 5) == [1, 2])
        m.apply(.createShard(shard: 1, members: [ShardMember(tag: 3, endpoint: "c", active: true)], slots: SlotSet()), at: 11)
        m.apply(.beginMigration(moveID: 1, slots: SlotSet([5]), from: 0, to: 1, sources: ["a"]), at: 12)
        #expect(m.tags(forSlot: 5) == [1, 2, 3], "dual writes during migration")
        m.apply(.transfer(moveID: 1, slots: SlotSet([5]), from: 0, to: 1), at: 13)
        #expect(m.tags(forSlot: 5) == [3] && m.migration == nil)
        m.apply(.removeReplica(shard: 0, tag: 2), at: 14)
        #expect(m.tags(forSlot: 6) == [1])
        #expect(m.nextTag == 4 && m.nextShardID == 2)
    }
}

@Suite struct StoreTests {
    @Test func mvccReadsAndGC() {
        let s = VersionedStore()
        let k = Bytes("k".utf8)
        s.apply(.set(key: k, value: [1], ttl: 0), stamp: Versionstamp(version: 10, order: 0))
        s.apply(.set(key: k, value: [2], ttl: 0), stamp: Versionstamp(version: 20, order: 0))
        s.apply(.clear(key: k), stamp: Versionstamp(version: 30, order: 0))
        #expect(s.get(k, at: 5) == nil)
        #expect(s.get(k, at: 15)?.value == [1])
        #expect(s.get(k, at: 25)?.value == [2])
        #expect(s.get(k, at: 35)?.value == nil)
        s.collectGarbage(oldest: 25)
        #expect(s.get(k, at: 25)?.value == [2], "the version visible at the GC horizon is kept")
        s.collectGarbage(oldest: 31)
        #expect(s.keyCount == 0, "a key whose only remaining version is a deletion is removed")
    }

    @Test func atomicAddAndTTL() {
        let s = VersionedStore()
        let k = Bytes("c".utf8)
        s.apply(.add(key: k, delta: 5), stamp: Versionstamp(version: 1, order: 0))
        s.apply(.add(key: k, delta: -2), stamp: Versionstamp(version: 2, order: 0))
        #expect(LittleEndianInt64.decode(s.get(k, at: 2)?.value) == 3)
        s.apply(.set(key: [7], value: [1], ttl: 100), stamp: Versionstamp(version: 1_000, order: 0))
        #expect(s.get([7], at: 1_099)?.visibleValue(at: 1_099) == [1])
        #expect(s.get([7], at: 1_100)?.visibleValue(at: 1_100) == nil)
    }

    @Test func rangesTruncateAndSlots() {
        let s = VersionedStore()
        for i in 0..<100 { s.apply(.set(key: Bytes("k\(i)".utf8), value: [UInt8(i)], ttl: 0), stamp: Versionstamp(version: UInt64(i + 1), order: 0)) }
        let r = s.range(KeyRange(begin: Bytes("k1".utf8), end: Bytes("k2".utf8)), at: 1_000, limit: 5) { _ in true }
        #expect(r.entries.map(\.key.utf8String) == ["k1", "k10", "k11", "k12", "k13"] && r.more)
        s.truncate(after: 50)
        #expect(s.keyCount == 50)
        let slots = SlotSet([Slots.slot(for: Bytes("k1".utf8))])
        #expect(s.extract(slots: slots).contains { $0.key == Bytes("k1".utf8) })
        s.drop(slots: slots)
        #expect(s.get(Bytes("k1".utf8), at: 1_000) == nil)
    }
}

@Suite struct ResolverTests {
    @Test func pointAndRangeConflicts() {
        var r = ConflictSet()
        let w = CommitRequest(readVersion: 0, writeConflictKeys: [Bytes("a".utf8)], mutations: [])
        #expect(r.resolve(w, commitVersion: 10) == .commit)
        #expect(r.resolve(CommitRequest(readVersion: 5, readConflictKeys: [Bytes("a".utf8)]), commitVersion: 11) == .conflict)
        #expect(r.resolve(CommitRequest(readVersion: 10, readConflictKeys: [Bytes("a".utf8)]), commitVersion: 11) == .commit)
        #expect(r.resolve(CommitRequest(readVersion: 5, readConflictRanges: [KeyRange(begin: [0], end: Bytes("b".utf8))]), commitVersion: 12) == .conflict)
        #expect(r.resolve(CommitRequest(readVersion: 5, readConflictRanges: [KeyRange(begin: Bytes("b".utf8), end: Bytes("c".utf8))]), commitVersion: 12) == .commit)
        r.advance(oldest: 8)
        #expect(r.resolve(CommitRequest(readVersion: 7), commitVersion: 13) == .tooOld)
    }
}
