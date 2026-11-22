/// A storage server's identity in the mutation stream. Commit proxies tag every mutation
/// with the tags of the storage servers that must apply it; each storage server pulls
/// exactly its tag from the transaction logs. A tag belongs to one storage *incarnation*
/// in one shard: a restarted server gets a fresh tag.
public typealias Tag = UInt32

/// The tag carrying cluster metadata (the shard map), used by recovery.
public let metadataTag: Tag = .max

public struct ShardMember: Hashable, Sendable, WireCodable {
    public var tag: Tag
    public var endpoint: Endpoint
    /// False while the replica is copying its data (catching up).
    public var active: Bool

    public init(tag: Tag, endpoint: Endpoint, active: Bool) {
        self.tag = tag; self.endpoint = endpoint; self.active = active
    }
    public func encode(to w: inout WireWriter) { w.u32(tag); w.string(endpoint); w.bool(active) }
    public init(from r: inout WireReader) throws { tag = try r.u32(); endpoint = try r.string(); active = try r.bool() }
}

public struct Migration: Hashable, Sendable, WireCodable {
    public var moveID: UInt64
    public var slots: SlotSet
    public var from: Int32
    public var to: Int32
    public init(moveID: UInt64, slots: SlotSet, from: Int32, to: Int32) {
        self.moveID = moveID; self.slots = slots; self.from = from; self.to = to
    }
    public func encode(to w: inout WireWriter) { w.u64(moveID); w.slots(slots); w.i32(from); w.i32(to) }
    public init(from r: inout WireReader) throws { moveID = try r.u64(); slots = try r.slots(); from = try r.i32(); to = try r.i32() }
}

/// Cluster metadata changes. They are committed through the transaction system like any
/// other write, so every proxy, storage server and recovery sees them at exactly the same
/// version. An op committed at version `v` changes routing for versions `> v`.
public indirect enum MetaOp: Hashable, Sendable, WireCodable {
    /// A new shard whose members are immediately active, owning `slots` (no data yet).
    case createShard(shard: Int32, members: [ShardMember], slots: SlotSet)
    /// A replica joins `shard` and copies the shard's `slots` from `sources` (`moveID` names the copy).
    case addReplica(shard: Int32, member: ShardMember, moveID: UInt64, sources: [Endpoint], slots: SlotSet)
    /// A catching-up replica finished copying.
    case replicaReady(shard: Int32, tag: Tag)
    /// A replica leaves its shard (failed or replaced). Its tag is retired.
    case removeReplica(shard: Int32, tag: Tag)
    /// Destination members start receiving `slots` (dual writes) and copy them from `sources`.
    case beginMigration(moveID: UInt64, slots: SlotSet, from: Int32, to: Int32, sources: [Endpoint])
    /// Ownership of a migration's slots flips from `from` to `to`.
    case transfer(moveID: UInt64, slots: SlotSet, from: Int32, to: Int32)
    case abortMigration(moveID: UInt64, to: Int32)
    /// `slots` (whose shard was lost) are given, empty, to `to`.
    case reassignSlots(slots: SlotSet, to: Int32)
    case removeShard(shard: Int32)
    /// A complete copy of the shard map (written to the metadata tag to compact it).
    case snapshot(ShardMap)

    public func encode(to w: inout WireWriter) {
        switch self {
        case .createShard(let s, let m, let slots): w.u8(1); w.i32(s); w.put(m); w.slots(slots)
        case .addReplica(let s, let m, let id, let src, let slots): w.u8(2); w.i32(s); w.put(m); w.u64(id); w.strings(src); w.slots(slots)
        case .replicaReady(let s, let t): w.u8(3); w.i32(s); w.u32(t)
        case .removeReplica(let s, let t): w.u8(4); w.i32(s); w.u32(t)
        case .beginMigration(let id, let slots, let f, let t, let src): w.u8(5); w.u64(id); w.slots(slots); w.i32(f); w.i32(t); w.strings(src)
        case .transfer(let id, let slots, let f, let t): w.u8(6); w.u64(id); w.slots(slots); w.i32(f); w.i32(t)
        case .abortMigration(let id, let t): w.u8(7); w.u64(id); w.i32(t)
        case .reassignSlots(let slots, let t): w.u8(8); w.slots(slots); w.i32(t)
        case .removeShard(let s): w.u8(9); w.i32(s)
        case .snapshot(let m): w.u8(10); w.put(m)
        }
    }

    public init(from r: inout WireReader) throws {
        switch try r.u8() {
        case 1: self = .createShard(shard: try r.i32(), members: try r.getArray(), slots: try r.slots())
        case 2: self = .addReplica(shard: try r.i32(), member: try r.get(), moveID: try r.u64(), sources: try r.strings(), slots: try r.slots())
        case 3: self = .replicaReady(shard: try r.i32(), tag: try r.u32())
        case 4: self = .removeReplica(shard: try r.i32(), tag: try r.u32())
        case 5: self = .beginMigration(moveID: try r.u64(), slots: try r.slots(), from: try r.i32(), to: try r.i32(), sources: try r.strings())
        case 6: self = .transfer(moveID: try r.u64(), slots: try r.slots(), from: try r.i32(), to: try r.i32())
        case 7: self = .abortMigration(moveID: try r.u64(), to: try r.i32())
        case 8: self = .reassignSlots(slots: try r.slots(), to: try r.i32())
        case 9: self = .removeShard(shard: try r.i32())
        case 10: self = .snapshot(try r.get())
        case let t: throw WireError("bad meta op \(t)")
        }
    }
}

/// The authoritative routing state: shards, their members (by tag) and slot ownership.
/// Derived purely by applying committed `MetaOp`s in version order.
public struct ShardMap: Hashable, Sendable, WireCodable {
    /// shard id -> members, sorted by tag.
    public var shards: [Int32: [ShardMember]] = [:]
    public var slotOwner: [Int32] = Array(repeating: -1, count: Slots.count)
    public var migration: Migration?
    public var nextTag: Tag = 1
    public var nextShardID: Int32 = 0
    public var nextMoveID: UInt64 = 1
    /// Version of the last op applied.
    public var version: Version = 0

    public init() {}

    public var shardIDs: [Int32] { shards.keys.sorted() }

    public func members(of shard: Int32) -> [ShardMember] { shards[shard] ?? [] }
    public func activeMembers(of shard: Int32) -> [ShardMember] { members(of: shard).filter(\.active) }

    public func shardOf(tag: Tag) -> Int32? {
        for id in shardIDs where shards[id]!.contains(where: { $0.tag == tag }) { return id }
        return nil
    }

    public func member(tag: Tag) -> ShardMember? {
        for id in shardIDs { if let m = shards[id]!.first(where: { $0.tag == tag }) { return m } }
        return nil
    }

    public func slots(of shard: Int32) -> SlotSet {
        var s = SlotSet()
        for (slot, o) in slotOwner.enumerated() where o == shard { s.insert(slot) }
        return s
    }

    public var allTags: [Tag] { shardIDs.flatMap { shards[$0]!.map(\.tag) } }

    /// Tags that must receive a mutation to `slot`.
    public func tags(forSlot slot: Int) -> [Tag] {
        var out: [Tag] = []
        let owner = slotOwner[slot]
        if owner >= 0 { out.append(contentsOf: members(of: owner).map(\.tag)) }
        if let m = migration, m.slots.contains(slot) {
            out.append(contentsOf: members(of: m.to).map(\.tag))
        }
        return out
    }

    /// Storage tags that must see `op` (evaluated before applying it).
    public func tagsAffected(by op: MetaOp) -> [Tag] {
        switch op {
        case .createShard(_, let members, _): return members.map(\.tag)
        case .addReplica(_, let member, _, _, _): return [member.tag]
        case .replicaReady(_, let tag): return [tag]
        case .removeReplica(_, let tag): return [tag]
        case .beginMigration(_, _, _, let to, _): return members(of: to).map(\.tag)
        case .transfer(_, _, let from, let to): return members(of: from).map(\.tag) + members(of: to).map(\.tag)
        case .abortMigration(_, let to): return members(of: to).map(\.tag)
        case .reassignSlots(_, let to): return members(of: to).map(\.tag)
        case .removeShard, .snapshot: return []
        }
    }

    public mutating func apply(_ op: MetaOp, at version: Version) {
        self.version = max(self.version, version)
        switch op {
        case .createShard(let id, let members, let slots):
            shards[id] = members.sorted { $0.tag < $1.tag }
            for slot in slots.sorted { slotOwner[slot] = id }
            nextShardID = max(nextShardID, id + 1)
            for m in members { nextTag = max(nextTag, m.tag + 1) }
        case .addReplica(let id, let member, let moveID, _, _):
            var ms = shards[id] ?? []
            ms.removeAll { $0.tag == member.tag }
            ms.append(member)
            shards[id] = ms.sorted { $0.tag < $1.tag }
            nextTag = max(nextTag, member.tag + 1)
            nextMoveID = max(nextMoveID, moveID + 1)
        case .replicaReady(let id, let tag):
            if let i = shards[id]?.firstIndex(where: { $0.tag == tag }) { shards[id]![i].active = true }
        case .removeReplica(let id, let tag):
            shards[id]?.removeAll { $0.tag == tag }
        case .beginMigration(let moveID, let slots, let from, let to, _):
            migration = Migration(moveID: moveID, slots: slots, from: from, to: to)
            nextMoveID = max(nextMoveID, moveID + 1)
        case .transfer(let moveID, let slots, _, let to):
            for slot in slots.sorted { slotOwner[slot] = to }
            if migration?.moveID == moveID { migration = nil }
        case .abortMigration(let moveID, _):
            if migration?.moveID == moveID { migration = nil }
        case .reassignSlots(let slots, let to):
            for slot in slots.sorted { slotOwner[slot] = to }
        case .removeShard(let id):
            shards[id] = nil
            for slot in 0..<Slots.count where slotOwner[slot] == id { slotOwner[slot] = -1 }
            if let m = migration, m.from == id || m.to == id { migration = nil }
        case .snapshot(let m):
            self = m
            self.version = max(m.version, version)
        }
    }

    /// The client-visible topology (active replicas only).
    public func topology(epoch: UInt64) -> Topology {
        Topology(epoch: epoch, slotOwner: slotOwner,
                 shards: shardIDs.map { Topology.Shard(id: $0, replicas: activeMembers(of: $0).map(\.endpoint)) })
    }

    public func encode(to w: inout WireWriter) {
        let ids = shardIDs
        w.array(ids) { w, id in w.i32(id); w.put(shards[id]!) }
        w.slotOwners(slotOwner)
        w.put(migration)
        w.u32(nextTag); w.i32(nextShardID); w.u64(nextMoveID); w.u64(version)
    }

    public init(from r: inout WireReader) throws {
        let entries = try r.array { r in (try r.i32(), try r.getArray(ShardMember.self)) }
        for (id, ms) in entries { shards[id] = ms }
        slotOwner = try r.slotOwners()
        migration = try r.getOptional()
        nextTag = try r.u32(); nextShardID = try r.i32(); nextMoveID = try r.u64(); version = try r.u64()
    }
}
