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
        topo.shards = [.init(id: 1, replicas: ["a:1", "b:2"])]
        let messages: [Message] = [
            .getReadVersion(rid: 1),
            .readVersionReply(rid: 2, version: 99),
            .commit(rid: 3, CommitRequest(readVersion: 7, readConflictKeys: [[1]], readConflictRanges: [KeyRange(begin: [1], end: [2])],
                                          writeConflictKeys: [[3]], mutations: [.set(key: [1], value: [2], ttl: 5), .clear(key: [3]), .add(key: [4], delta: -9)])),
            .commitReply(rid: 4, .committed(Versionstamp(version: 9, order: 1))),
            .commitReply(rid: 5, .failed(.notCommitted)),
            .topologyReply(rid: 6, topo),
            .read(rid: 7, key: [1, 2], version: 3),
            .readReply(rid: 8, value: nil, stamp: .zero),
            .rangeReply(rid: 9, entries: [KeyValueEntry(key: [1], value: [2], stamp: .zero)], more: true, servedSlots: SlotSet([1, 2])),
            .join(StorageReport(endpoint: "x:1", shardID: 2, coordinatorEpoch: 3, appliedVersion: 4, committedVersion: 3, isActive: true, ownedSlots: .all)),
            .batch(Batch(coordinatorEpoch: 1, version: 10, prevVersion: 9, committedVersion: 8, oldestVersion: 1,
                         entries: [.init(order: 0, mutations: [.clear(key: [9])])],
                         meta: [.beginReceive(moveID: 1, shardID: 2, slots: SlotSet([7]), sources: ["s:1"]),
                                .transfer(moveID: 1, slots: SlotSet([7]), from: 1, to: 2), .gainEmpty(shardID: 3, slots: SlotSet([8])),
                                .abortReceive(moveID: 4, shardID: 5)])),
            .snapshotChunk(rid: 10, SnapshotChunk(version: 5, items: [.init(key: [1], versions: [VersionedValue(stamp: .zero, value: nil, expiresAt: 3)])], done: true)),
        ]
        for m in messages {
            #expect(try Message.decode(m.encode()) == m, "\(m.name)")
        }
    }

    @Test func rejectsGarbage() {
        #expect(throws: (any Error).self) { try Message.decode([200, 1, 2]) }
        #expect(throws: (any Error).self) { try Message.decode([3, 1]) }
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
        var r = Resolver()
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
