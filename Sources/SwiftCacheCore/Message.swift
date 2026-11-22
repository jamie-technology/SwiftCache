/// Every message exchanged between clients, the coordinator and storage servers.
/// `rid` is a request id chosen by the requester and echoed in the reply.
public enum Message: Hashable, Sendable {
    // MARK: client <-> coordinator
    case getReadVersion(rid: UInt64)
    case readVersionReply(rid: UInt64, version: Version)
    case commit(rid: UInt64, CommitRequest)
    case commitReply(rid: UInt64, CommitResult)
    case getTopology(rid: UInt64)
    case topologyReply(rid: UInt64, Topology)
    case getStatus(rid: UInt64)
    case statusReply(rid: UInt64, ClusterStatus)
    case errorReply(rid: UInt64, ErrorCode)

    // MARK: client <-> storage
    case read(rid: UInt64, key: Bytes, version: Version)
    case readReply(rid: UInt64, value: Bytes?, stamp: Versionstamp)
    case readRange(rid: UInt64, range: KeyRange, version: Version, limit: UInt32)
    case rangeReply(rid: UInt64, entries: [KeyValueEntry], more: Bool, servedSlots: SlotSet)
    case getStorageStats(rid: UInt64)
    case storageStatsReply(rid: UInt64, StorageStats)

    // MARK: coordinator <-> storage
    case join(StorageReport)
    case assign(Assignment)
    case batch(Batch)
    case batchAck(version: Version, coordinatorEpoch: UInt64)
    case moveReady(moveID: UInt64, coordinatorEpoch: UInt64)
    case recover(coordinatorEpoch: UInt64, truncateTo: Version)
    case recoverReply(StorageReport)

    // MARK: storage <-> storage
    case fetchSnapshot(rid: UInt64, slots: SlotSet, minVersion: Version)
    case snapshotChunk(rid: UInt64, SnapshotChunk)

    /// The request id if this is a request or reply.
    public var rid: UInt64? {
        switch self {
        case .getReadVersion(let r), .readVersionReply(let r, _), .commit(let r, _), .commitReply(let r, _),
             .getTopology(let r), .topologyReply(let r, _), .getStatus(let r), .statusReply(let r, _),
             .errorReply(let r, _), .read(let r, _, _), .readReply(let r, _, _), .readRange(let r, _, _, _),
             .rangeReply(let r, _, _, _), .getStorageStats(let r), .storageStatsReply(let r, _),
             .fetchSnapshot(let r, _, _), .snapshotChunk(let r, _):
            return r
        case .join, .assign, .batch, .batchAck, .moveReady, .recover, .recoverReply:
            return nil
        }
    }

    /// Short name for logs and traces.
    public var name: String {
        switch self {
        case .getReadVersion: "getReadVersion"
        case .readVersionReply: "readVersionReply"
        case .commit: "commit"
        case .commitReply: "commitReply"
        case .getTopology: "getTopology"
        case .topologyReply: "topologyReply"
        case .getStatus: "getStatus"
        case .statusReply: "statusReply"
        case .errorReply: "errorReply"
        case .read: "read"
        case .readReply: "readReply"
        case .readRange: "readRange"
        case .rangeReply: "rangeReply"
        case .getStorageStats: "getStorageStats"
        case .storageStatsReply: "storageStatsReply"
        case .join: "join"
        case .assign: "assign"
        case .batch: "batch"
        case .batchAck: "batchAck"
        case .moveReady: "moveReady"
        case .recover: "recover"
        case .recoverReply: "recoverReply"
        case .fetchSnapshot: "fetchSnapshot"
        case .snapshotChunk: "snapshotChunk"
        }
    }
}

// MARK: - Binary codec

extension WireWriter {
    mutating func mutation(_ m: Mutation) {
        switch m {
        case .set(let k, let v, let ttl): u8(1); blob(k); blob(v); u64(ttl)
        case .clear(let k): u8(2); blob(k)
        case .add(let k, let d): u8(3); blob(k); i64(d)
        }
    }

    mutating func versioned(_ v: VersionedValue) {
        stamp(v.stamp); optionalBlob(v.value); u64(v.expiresAt)
    }

    mutating func report(_ r: StorageReport) {
        string(r.endpoint); i32(r.shardID); u64(r.coordinatorEpoch); u64(r.appliedVersion); u64(r.committedVersion)
        bool(r.isActive); slots(r.ownedSlots)
    }

    mutating func topology(_ t: Topology) {
        u64(t.epoch)
        // Run-length encode the slot map.
        var runs = [(start: Int, length: Int, owner: Int32)]()
        for (i, o) in t.slotOwner.enumerated() {
            if let last = runs.last, last.owner == o, last.start + last.length == i {
                runs[runs.count - 1].length += 1
            } else {
                runs.append((i, 1, o))
            }
        }
        array(runs) { w, r in w.u16(UInt16(r.start)); w.u16(UInt16(r.length)); w.i32(r.owner) }
        array(t.shards) { w, s in w.i32(s.id); w.array(s.replicas) { $0.string($1) } }
    }

    mutating func meta(_ m: MetaOp) {
        switch m {
        case .beginReceive(let id, let shard, let s, let sources):
            u8(1); u64(id); i32(shard); slots(s); array(sources) { $0.string($1) }
        case .abortReceive(let id, let shard):
            u8(2); u64(id); i32(shard)
        case .transfer(let id, let s, let from, let to):
            u8(3); u64(id); slots(s); i32(from); i32(to)
        case .gainEmpty(let shard, let s):
            u8(4); i32(shard); slots(s)
        }
    }
}

extension WireReader {
    mutating func mutation() throws -> Mutation {
        switch try u8() {
        case 1: return .set(key: try blob(), value: try blob(), ttl: try u64())
        case 2: return .clear(key: try blob())
        case 3: return .add(key: try blob(), delta: try i64())
        case let t: throw WireError("bad mutation tag \(t)")
        }
    }

    mutating func versioned() throws -> VersionedValue {
        VersionedValue(stamp: try stamp(), value: try optionalBlob(), expiresAt: try u64())
    }

    mutating func report() throws -> StorageReport {
        StorageReport(endpoint: try string(), shardID: try i32(), coordinatorEpoch: try u64(),
                      appliedVersion: try u64(), committedVersion: try u64(), isActive: try bool(), ownedSlots: try slots())
    }

    mutating func topology() throws -> Topology {
        let epoch = try u64()
        var owner = [Int32](repeating: -1, count: Slots.count)
        let runs = try array { r in (Int(try r.u16()), Int(try r.u16()), try r.i32()) }
        for (start, length, o) in runs {
            guard start + length <= Slots.count else { throw WireError("bad slot run") }
            for i in start..<(start + length) { owner[i] = o }
        }
        let shards = try array { r in Topology.Shard(id: try r.i32(), replicas: try r.array { try $0.string() }) }
        return Topology(epoch: epoch, slotOwner: owner, shards: shards)
    }

    mutating func meta() throws -> MetaOp {
        switch try u8() {
        case 1: return .beginReceive(moveID: try u64(), shardID: try i32(), slots: try slots(), sources: try array { try $0.string() })
        case 2: return .abortReceive(moveID: try u64(), shardID: try i32())
        case 3: return .transfer(moveID: try u64(), slots: try slots(), from: try i32(), to: try i32())
        case 4: return .gainEmpty(shardID: try i32(), slots: try slots())
        case let t: throw WireError("bad meta tag \(t)")
        }
    }

    mutating func errorCode() throws -> ErrorCode {
        let raw = try u16()
        return ErrorCode(rawValue: raw) ?? .internalError
    }
}

extension Message {
    public func encode() -> Bytes {
        var w = WireWriter()
        switch self {
        case .getReadVersion(let rid):
            w.u8(1); w.u64(rid)
        case .readVersionReply(let rid, let v):
            w.u8(2); w.u64(rid); w.u64(v)
        case .commit(let rid, let req):
            w.u8(3); w.u64(rid); w.u64(req.readVersion)
            w.array(req.readConflictKeys) { $0.blob($1) }
            w.array(req.readConflictRanges) { $0.blob($1.begin); $0.blob($1.end) }
            w.array(req.writeConflictKeys) { $0.blob($1) }
            w.array(req.mutations) { $0.mutation($1) }
        case .commitReply(let rid, let result):
            w.u8(4); w.u64(rid)
            switch result {
            case .committed(let s): w.u8(1); w.stamp(s)
            case .failed(let e): w.u8(2); w.u16(e.rawValue)
            }
        case .getTopology(let rid):
            w.u8(5); w.u64(rid)
        case .topologyReply(let rid, let t):
            w.u8(6); w.u64(rid); w.topology(t)
        case .getStatus(let rid):
            w.u8(7); w.u64(rid)
        case .statusReply(let rid, let s):
            w.u8(8); w.u64(rid); w.u64(s.coordinatorEpoch); w.string(s.phase); w.u64(s.latestVersion)
            w.u64(s.committedVersion); w.u32(s.replicationFactor); w.topology(s.topology)
            w.array(s.servers) { $0.string($1.endpoint); $0.i32($1.shardID); $0.string($1.state) }
            w.array(s.movesInProgress) { $0.string($1) }
            w.u64(s.commitsTotal); w.u64(s.conflictsTotal)
        case .errorReply(let rid, let e):
            w.u8(9); w.u64(rid); w.u16(e.rawValue)
        case .read(let rid, let key, let v):
            w.u8(10); w.u64(rid); w.blob(key); w.u64(v)
        case .readReply(let rid, let value, let stamp):
            w.u8(11); w.u64(rid); w.optionalBlob(value); w.stamp(stamp)
        case .readRange(let rid, let range, let v, let limit):
            w.u8(12); w.u64(rid); w.blob(range.begin); w.blob(range.end); w.u64(v); w.u32(limit)
        case .rangeReply(let rid, let entries, let more, let served):
            w.u8(13); w.u64(rid)
            w.array(entries) { $0.blob($1.key); $0.blob($1.value); $0.stamp($1.stamp) }
            w.bool(more); w.slots(served)
        case .getStorageStats(let rid):
            w.u8(14); w.u64(rid)
        case .storageStatsReply(let rid, let s):
            w.u8(15); w.u64(rid); w.string(s.endpoint); w.i32(s.shardID); w.bool(s.isActive)
            w.u64(s.appliedVersion); w.u64(s.committedVersion); w.u64(s.keyCount); w.u64(s.versionCount)
            w.u64(s.approximateBytes); w.u32(s.ownedSlots)
        case .join(let r):
            w.u8(20); w.report(r)
        case .assign(let a):
            w.u8(21); w.u64(a.coordinatorEpoch); w.i32(a.shardID); w.u8(a.mode.rawValue); w.slots(a.slots)
        case .batch(let b):
            w.u8(22); w.u64(b.coordinatorEpoch); w.u64(b.version); w.u64(b.prevVersion); w.u64(b.committedVersion)
            w.u64(b.oldestVersion)
            w.array(b.entries) { w, e in w.u16(e.order); w.array(e.mutations) { $0.mutation($1) } }
            w.array(b.meta) { $0.meta($1) }
        case .batchAck(let v, let epoch):
            w.u8(23); w.u64(v); w.u64(epoch)
        case .moveReady(let id, let epoch):
            w.u8(24); w.u64(id); w.u64(epoch)
        case .recover(let epoch, let v):
            w.u8(25); w.u64(epoch); w.u64(v)
        case .recoverReply(let r):
            w.u8(26); w.report(r)
        case .fetchSnapshot(let rid, let slots, let v):
            w.u8(30); w.u64(rid); w.slots(slots); w.u64(v)
        case .snapshotChunk(let rid, let c):
            w.u8(31); w.u64(rid); w.u64(c.version)
            w.array(c.items) { w, item in w.blob(item.key); w.array(item.versions) { $0.versioned($1) } }
            w.bool(c.done)
        }
        return w.bytes
    }

    public static func decode(_ bytes: Bytes) throws -> Message {
        var r = WireReader(bytes)
        let tag = try r.u8()
        let msg: Message
        switch tag {
        case 1: msg = .getReadVersion(rid: try r.u64())
        case 2: msg = .readVersionReply(rid: try r.u64(), version: try r.u64())
        case 3:
            let rid = try r.u64()
            let rv = try r.u64()
            let rk = try r.array { try $0.blob() }
            let rr = try r.array { KeyRange(begin: try $0.blob(), end: try $0.blob()) }
            let wk = try r.array { try $0.blob() }
            let m = try r.array { try $0.mutation() }
            msg = .commit(rid: rid, CommitRequest(readVersion: rv, readConflictKeys: rk, readConflictRanges: rr, writeConflictKeys: wk, mutations: m))
        case 4:
            let rid = try r.u64()
            switch try r.u8() {
            case 1: msg = .commitReply(rid: rid, .committed(try r.stamp()))
            case 2: msg = .commitReply(rid: rid, .failed(try r.errorCode()))
            case let t: throw WireError("bad commit result \(t)")
            }
        case 5: msg = .getTopology(rid: try r.u64())
        case 6: msg = .topologyReply(rid: try r.u64(), try r.topology())
        case 7: msg = .getStatus(rid: try r.u64())
        case 8:
            let rid = try r.u64()
            let s = ClusterStatus(
                coordinatorEpoch: try r.u64(), phase: try r.string(), latestVersion: try r.u64(),
                committedVersion: try r.u64(), replicationFactor: try r.u32(), topology: try r.topology(),
                servers: try r.array { ClusterStatus.Server(endpoint: try $0.string(), shardID: try $0.i32(), state: try $0.string()) },
                movesInProgress: try r.array { try $0.string() },
                commitsTotal: try r.u64(), conflictsTotal: try r.u64())
            msg = .statusReply(rid: rid, s)
        case 9: msg = .errorReply(rid: try r.u64(), try r.errorCode())
        case 10: msg = .read(rid: try r.u64(), key: try r.blob(), version: try r.u64())
        case 11: msg = .readReply(rid: try r.u64(), value: try r.optionalBlob(), stamp: try r.stamp())
        case 12:
            msg = .readRange(rid: try r.u64(), range: KeyRange(begin: try r.blob(), end: try r.blob()), version: try r.u64(), limit: try r.u32())
        case 13:
            let rid = try r.u64()
            let entries = try r.array { KeyValueEntry(key: try $0.blob(), value: try $0.blob(), stamp: try $0.stamp()) }
            msg = .rangeReply(rid: rid, entries: entries, more: try r.bool(), servedSlots: try r.slots())
        case 14: msg = .getStorageStats(rid: try r.u64())
        case 15:
            let rid = try r.u64()
            msg = .storageStatsReply(rid: rid, StorageStats(
                endpoint: try r.string(), shardID: try r.i32(), isActive: try r.bool(), appliedVersion: try r.u64(),
                committedVersion: try r.u64(), keyCount: try r.u64(), versionCount: try r.u64(),
                approximateBytes: try r.u64(), ownedSlots: try r.u32()))
        case 20: msg = .join(try r.report())
        case 21:
            let epoch = try r.u64(), shard = try r.i32()
            guard let mode = Assignment.Mode(rawValue: try r.u8()) else { throw WireError("bad assignment mode") }
            msg = .assign(Assignment(coordinatorEpoch: epoch, shardID: shard, mode: mode, slots: try r.slots()))
        case 22:
            let epoch = try r.u64(), v = try r.u64(), pv = try r.u64(), cv = try r.u64(), ov = try r.u64()
            let entries = try r.array { r in Batch.Entry(order: try r.u16(), mutations: try r.array { try $0.mutation() }) }
            let meta = try r.array { try $0.meta() }
            msg = .batch(Batch(coordinatorEpoch: epoch, version: v, prevVersion: pv, committedVersion: cv, oldestVersion: ov, entries: entries, meta: meta))
        case 23: msg = .batchAck(version: try r.u64(), coordinatorEpoch: try r.u64())
        case 24: msg = .moveReady(moveID: try r.u64(), coordinatorEpoch: try r.u64())
        case 25: msg = .recover(coordinatorEpoch: try r.u64(), truncateTo: try r.u64())
        case 26: msg = .recoverReply(try r.report())
        case 30: msg = .fetchSnapshot(rid: try r.u64(), slots: try r.slots(), minVersion: try r.u64())
        case 31:
            let rid = try r.u64(), v = try r.u64()
            let items = try r.array { r in SnapshotChunk.Item(key: try r.blob(), versions: try r.array { try $0.versioned() }) }
            msg = .snapshotChunk(rid: rid, SnapshotChunk(version: v, items: items, done: try r.bool()))
        default:
            throw WireError("unknown message tag \(tag)")
        }
        guard r.isAtEnd else { throw WireError("trailing bytes in \(msg.name)") }
        return msg
    }
}
