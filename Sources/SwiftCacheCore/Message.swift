/// Every message exchanged between clients and roles. `rid` is a request id chosen by
/// the requester and echoed in the reply.
public enum Message: Hashable, Sendable {
    // MARK: Clients <-> proxies / controller
    case getReadVersion(rid: UInt64)
    case readVersionReply(rid: UInt64, version: Version)
    case commit(rid: UInt64, CommitRequest)
    case commitReply(rid: UInt64, CommitResult)
    case getClientInfo(rid: UInt64)
    case clientInfoReply(rid: UInt64, ClientInfo)
    case getStatus(rid: UInt64)
    case statusReply(rid: UInt64, ClusterStatus)
    case errorReply(rid: UInt64, ErrorCode)

    // MARK: Storage
    case read(rid: UInt64, key: Bytes, version: Version)
    case readReply(rid: UInt64, value: Bytes?, stamp: Versionstamp)
    case readRange(rid: UInt64, range: KeyRange, version: Version, limit: UInt32)
    case rangeReply(rid: UInt64, entries: [KeyValueEntry], more: Bool, servedSlots: SlotSet)
    case getStorageStats(rid: UInt64)
    case storageStatsReply(rid: UInt64, StorageStats)
    case fetchSnapshot(rid: UInt64, slots: SlotSet, minVersion: Version)
    case snapshotChunk(rid: UInt64, SnapshotChunk)
    case moveReady(moveID: UInt64, tag: Tag)

    // MARK: Coordinators
    case candidacy(rid: UInt64, LeaderInfo)
    case nominationReply(rid: UInt64, LeaderInfo?)
    case leaderHeartbeat(rid: UInt64, LeaderInfo)
    case heartbeatReply(rid: UInt64, ok: Bool)
    case getLeader(rid: UInt64)
    case leaderReply(rid: UInt64, LeaderInfo?)
    case genRead(rid: UInt64, Ballot)
    case genReadReply(rid: UInt64, ok: Bool, readBallot: Ballot, writeBallot: Ballot, value: Bytes?)
    case genWrite(rid: UInt64, Ballot, value: Bytes)
    case genWriteReply(rid: UInt64, ok: Bool, readBallot: Ballot)

    // MARK: Workers / cluster controller
    case registerWorker(rid: UInt64, WorkerRegistration)
    case registerReply(rid: UInt64, ServerDBInfo, assignedTag: Tag)
    case serverDBInfo(ServerDBInfo)
    case recruit(rid: UInt64, RoleSpec)
    case recruitReply(rid: UInt64, endpoint: Endpoint)
    case halt(role: Endpoint)
    case recoveryComplete(rid: UInt64, RecoveryResult)
    case ack(rid: UInt64)
    case getShardMap(rid: UInt64, minVersion: Version)
    case shardMapReply(rid: UInt64, ShardMap)

    // MARK: Master <-> proxies
    case getCommitVersion(rid: UInt64, proxyIndex: UInt32, requestNum: UInt64)
    case commitVersionReply(rid: UInt64, prevVersion: Version, version: Version)
    case reportCommitted(rid: UInt64, version: Version)
    case getLiveCommitted(rid: UInt64)
    case liveCommittedReply(rid: UInt64, version: Version)

    // MARK: Resolvers
    case resolve(rid: UInt64, ResolveRequest)
    case resolveReply(rid: UInt64, ResolveReply)

    // MARK: Transaction logs
    case tlogPush(rid: UInt64, TLogPush)
    case tlogPushReply(rid: UInt64, ok: Bool)
    /// `upTo` = 0 means "up to what is known committed".
    case tlogPeek(rid: UInt64, tag: Tag, from: Version, upTo: Version)
    case tlogPeekReply(rid: UInt64, PeekReply)
    case tlogPop(tag: Tag, version: Version)
    case tlogLock(rid: UInt64, generation: UInt64)
    case tlogLockReply(rid: UInt64, LockReply)
    case tlogConfirm(rid: UInt64, generation: UInt64)
    case tlogConfirmReply(rid: UInt64, ok: Bool)
    case tlogFetch(rid: UInt64, upTo: Version)
    case tlogFetchReply(rid: UInt64, FetchReply)

    /// The request id if this is a request or reply.
    public var rid: UInt64? {
        switch self {
        case .getReadVersion(let r), .readVersionReply(let r, _), .commit(let r, _), .commitReply(let r, _),
             .getClientInfo(let r), .clientInfoReply(let r, _), .getStatus(let r), .statusReply(let r, _),
             .errorReply(let r, _), .read(let r, _, _), .readReply(let r, _, _), .readRange(let r, _, _, _),
             .rangeReply(let r, _, _, _), .getStorageStats(let r), .storageStatsReply(let r, _),
             .fetchSnapshot(let r, _, _), .snapshotChunk(let r, _),
             .candidacy(let r, _), .nominationReply(let r, _), .leaderHeartbeat(let r, _), .heartbeatReply(let r, _),
             .getLeader(let r), .leaderReply(let r, _), .genRead(let r, _), .genReadReply(let r, _, _, _, _),
             .genWrite(let r, _, _), .genWriteReply(let r, _, _),
             .registerWorker(let r, _), .registerReply(let r, _, _), .recruit(let r, _), .recruitReply(let r, _),
             .recoveryComplete(let r, _), .ack(let r), .getShardMap(let r, _), .shardMapReply(let r, _),
             .getCommitVersion(let r, _, _), .commitVersionReply(let r, _, _), .getLiveCommitted(let r), .reportCommitted(let r, _),
             .liveCommittedReply(let r, _), .resolve(let r, _), .resolveReply(let r, _),
             .tlogPush(let r, _), .tlogPushReply(let r, _), .tlogPeek(let r, _, _, _), .tlogPeekReply(let r, _),
             .tlogLock(let r, _), .tlogLockReply(let r, _), .tlogConfirm(let r, _), .tlogConfirmReply(let r, _),
             .tlogFetch(let r, _), .tlogFetchReply(let r, _):
            return r
        case .moveReady, .serverDBInfo, .halt, .tlogPop:
            return nil
        }
    }

    /// Whether this message is a reply (routed back to whoever is waiting on `rid`).
    public var isReply: Bool {
        switch self {
        case .readVersionReply, .commitReply, .clientInfoReply, .statusReply, .errorReply, .readReply, .rangeReply,
             .storageStatsReply, .snapshotChunk, .nominationReply, .heartbeatReply, .leaderReply, .genReadReply,
             .genWriteReply, .registerReply, .recruitReply, .ack, .shardMapReply, .commitVersionReply,
             .liveCommittedReply, .resolveReply, .tlogPushReply, .tlogPeekReply, .tlogLockReply, .tlogConfirmReply,
             .tlogFetchReply:
            return true
        default:
            return false
        }
    }

    /// Short name for logs and traces.
    public var name: String {
        let s = String(describing: self)
        return String(s.prefix { $0 != "(" })
    }
}

// MARK: - Binary codec

extension Message {
    public func encode() -> Bytes {
        var w = WireWriter()
        encode(to: &w)
        return w.bytes
    }

    public func encode(to w: inout WireWriter) {
        switch self {
        case .getReadVersion(let rid): w.u8(1); w.u64(rid)
        case .readVersionReply(let rid, let v): w.u8(2); w.u64(rid); w.u64(v)
        case .commit(let rid, let req): w.u8(3); w.u64(rid); w.put(req)
        case .commitReply(let rid, let res): w.u8(4); w.u64(rid); w.put(res)
        case .getClientInfo(let rid): w.u8(5); w.u64(rid)
        case .clientInfoReply(let rid, let i): w.u8(6); w.u64(rid); w.put(i)
        case .getStatus(let rid): w.u8(7); w.u64(rid)
        case .statusReply(let rid, let s): w.u8(8); w.u64(rid); w.put(s)
        case .errorReply(let rid, let e): w.u8(9); w.u64(rid); w.u16(e.rawValue)

        case .read(let rid, let key, let v): w.u8(10); w.u64(rid); w.blob(key); w.u64(v)
        case .readReply(let rid, let value, let stamp): w.u8(11); w.u64(rid); w.optionalBlob(value); w.stamp(stamp)
        case .readRange(let rid, let range, let v, let limit): w.u8(12); w.u64(rid); w.put(range); w.u64(v); w.u32(limit)
        case .rangeReply(let rid, let entries, let more, let served): w.u8(13); w.u64(rid); w.put(entries); w.bool(more); w.slots(served)
        case .getStorageStats(let rid): w.u8(14); w.u64(rid)
        case .storageStatsReply(let rid, let s): w.u8(15); w.u64(rid); w.put(s)
        case .fetchSnapshot(let rid, let slots, let v): w.u8(16); w.u64(rid); w.slots(slots); w.u64(v)
        case .snapshotChunk(let rid, let c): w.u8(17); w.u64(rid); w.put(c)
        case .moveReady(let id, let tag): w.u8(18); w.u64(id); w.u32(tag)

        case .candidacy(let rid, let l): w.u8(20); w.u64(rid); w.put(l)
        case .nominationReply(let rid, let l): w.u8(21); w.u64(rid); w.put(l)
        case .leaderHeartbeat(let rid, let l): w.u8(22); w.u64(rid); w.put(l)
        case .heartbeatReply(let rid, let ok): w.u8(23); w.u64(rid); w.bool(ok)
        case .getLeader(let rid): w.u8(24); w.u64(rid)
        case .leaderReply(let rid, let l): w.u8(25); w.u64(rid); w.put(l)
        case .genRead(let rid, let b): w.u8(26); w.u64(rid); w.put(b)
        case .genReadReply(let rid, let ok, let rb, let wb, let v): w.u8(27); w.u64(rid); w.bool(ok); w.put(rb); w.put(wb); w.optionalBlob(v)
        case .genWrite(let rid, let b, let v): w.u8(28); w.u64(rid); w.put(b); w.blob(v)
        case .genWriteReply(let rid, let ok, let rb): w.u8(29); w.u64(rid); w.bool(ok); w.put(rb)

        case .registerWorker(let rid, let reg): w.u8(30); w.u64(rid); w.put(reg)
        case .registerReply(let rid, let info, let tag): w.u8(31); w.u64(rid); w.put(info); w.u32(tag)
        case .serverDBInfo(let info): w.u8(32); w.put(info)
        case .recruit(let rid, let spec): w.u8(33); w.u64(rid); w.put(spec)
        case .recruitReply(let rid, let ep): w.u8(34); w.u64(rid); w.string(ep)
        case .halt(let role): w.u8(35); w.string(role)
        case .recoveryComplete(let rid, let res): w.u8(36); w.u64(rid); w.put(res)
        case .ack(let rid): w.u8(37); w.u64(rid)
        case .getShardMap(let rid, let v): w.u8(38); w.u64(rid); w.u64(v)
        case .shardMapReply(let rid, let m): w.u8(39); w.u64(rid); w.put(m)

        case .getCommitVersion(let rid, let p, let n): w.u8(40); w.u64(rid); w.u32(p); w.u64(n)
        case .commitVersionReply(let rid, let prev, let v): w.u8(41); w.u64(rid); w.u64(prev); w.u64(v)
        case .reportCommitted(let rid, let v): w.u8(42); w.u64(rid); w.u64(v)
        case .getLiveCommitted(let rid): w.u8(43); w.u64(rid)
        case .liveCommittedReply(let rid, let v): w.u8(44); w.u64(rid); w.u64(v)
        case .resolve(let rid, let req): w.u8(45); w.u64(rid); w.put(req)
        case .resolveReply(let rid, let rep): w.u8(46); w.u64(rid); w.put(rep)

        case .tlogPush(let rid, let p): w.u8(50); w.u64(rid); w.put(p)
        case .tlogPushReply(let rid, let ok): w.u8(51); w.u64(rid); w.bool(ok)
        case .tlogPeek(let rid, let tag, let from, let upTo): w.u8(52); w.u64(rid); w.u32(tag); w.u64(from); w.u64(upTo)
        case .tlogPeekReply(let rid, let p): w.u8(53); w.u64(rid); w.put(p)
        case .tlogPop(let tag, let v): w.u8(54); w.u32(tag); w.u64(v)
        case .tlogLock(let rid, let g): w.u8(55); w.u64(rid); w.u64(g)
        case .tlogLockReply(let rid, let l): w.u8(56); w.u64(rid); w.put(l)
        case .tlogConfirm(let rid, let g): w.u8(57); w.u64(rid); w.u64(g)
        case .tlogConfirmReply(let rid, let ok): w.u8(58); w.u64(rid); w.bool(ok)
        case .tlogFetch(let rid, let v): w.u8(59); w.u64(rid); w.u64(v)
        case .tlogFetchReply(let rid, let f): w.u8(60); w.u64(rid); w.put(f)
        }
    }

    public static func decode(_ bytes: Bytes) throws -> Message {
        var r = WireReader(bytes)
        let m = try decode(from: &r)
        guard r.isAtEnd else { throw WireError("trailing bytes in \(m.name)") }
        return m
    }

    public static func decode(from r: inout WireReader) throws -> Message {
        let tag = try r.u8()
        switch tag {
        case 1: return .getReadVersion(rid: try r.u64())
        case 2: return .readVersionReply(rid: try r.u64(), version: try r.u64())
        case 3: return .commit(rid: try r.u64(), try r.get())
        case 4: return .commitReply(rid: try r.u64(), try r.get())
        case 5: return .getClientInfo(rid: try r.u64())
        case 6: return .clientInfoReply(rid: try r.u64(), try r.get())
        case 7: return .getStatus(rid: try r.u64())
        case 8: return .statusReply(rid: try r.u64(), try r.get())
        case 9: return .errorReply(rid: try r.u64(), ErrorCode(rawValue: try r.u16()) ?? .internalError)

        case 10: return .read(rid: try r.u64(), key: try r.blob(), version: try r.u64())
        case 11: return .readReply(rid: try r.u64(), value: try r.optionalBlob(), stamp: try r.stamp())
        case 12: return .readRange(rid: try r.u64(), range: try r.get(), version: try r.u64(), limit: try r.u32())
        case 13: return .rangeReply(rid: try r.u64(), entries: try r.getArray(), more: try r.bool(), servedSlots: try r.slots())
        case 14: return .getStorageStats(rid: try r.u64())
        case 15: return .storageStatsReply(rid: try r.u64(), try r.get())
        case 16: return .fetchSnapshot(rid: try r.u64(), slots: try r.slots(), minVersion: try r.u64())
        case 17: return .snapshotChunk(rid: try r.u64(), try r.get())
        case 18: return .moveReady(moveID: try r.u64(), tag: try r.u32())

        case 20: return .candidacy(rid: try r.u64(), try r.get())
        case 21: return .nominationReply(rid: try r.u64(), try r.getOptional())
        case 22: return .leaderHeartbeat(rid: try r.u64(), try r.get())
        case 23: return .heartbeatReply(rid: try r.u64(), ok: try r.bool())
        case 24: return .getLeader(rid: try r.u64())
        case 25: return .leaderReply(rid: try r.u64(), try r.getOptional())
        case 26: return .genRead(rid: try r.u64(), try r.get())
        case 27: return .genReadReply(rid: try r.u64(), ok: try r.bool(), readBallot: try r.get(), writeBallot: try r.get(), value: try r.optionalBlob())
        case 28: return .genWrite(rid: try r.u64(), try r.get(), value: try r.blob())
        case 29: return .genWriteReply(rid: try r.u64(), ok: try r.bool(), readBallot: try r.get())

        case 30: return .registerWorker(rid: try r.u64(), try r.get())
        case 31: return .registerReply(rid: try r.u64(), try r.get(), assignedTag: try r.u32())
        case 32: return .serverDBInfo(try r.get())
        case 33: return .recruit(rid: try r.u64(), try r.get())
        case 34: return .recruitReply(rid: try r.u64(), endpoint: try r.string())
        case 35: return .halt(role: try r.string())
        case 36: return .recoveryComplete(rid: try r.u64(), try r.get())
        case 37: return .ack(rid: try r.u64())
        case 38: return .getShardMap(rid: try r.u64(), minVersion: try r.u64())
        case 39: return .shardMapReply(rid: try r.u64(), try r.get())

        case 40: return .getCommitVersion(rid: try r.u64(), proxyIndex: try r.u32(), requestNum: try r.u64())
        case 41: return .commitVersionReply(rid: try r.u64(), prevVersion: try r.u64(), version: try r.u64())
        case 42: return .reportCommitted(rid: try r.u64(), version: try r.u64())
        case 43: return .getLiveCommitted(rid: try r.u64())
        case 44: return .liveCommittedReply(rid: try r.u64(), version: try r.u64())
        case 45: return .resolve(rid: try r.u64(), try r.get())
        case 46: return .resolveReply(rid: try r.u64(), try r.get())

        case 50: return .tlogPush(rid: try r.u64(), try r.get())
        case 51: return .tlogPushReply(rid: try r.u64(), ok: try r.bool())
        case 52: return .tlogPeek(rid: try r.u64(), tag: try r.u32(), from: try r.u64(), upTo: try r.u64())
        case 53: return .tlogPeekReply(rid: try r.u64(), try r.get())
        case 54: return .tlogPop(tag: try r.u32(), version: try r.u64())
        case 55: return .tlogLock(rid: try r.u64(), generation: try r.u64())
        case 56: return .tlogLockReply(rid: try r.u64(), try r.get())
        case 57: return .tlogConfirm(rid: try r.u64(), generation: try r.u64())
        case 58: return .tlogConfirmReply(rid: try r.u64(), ok: try r.bool())
        case 59: return .tlogFetch(rid: try r.u64(), upTo: try r.u64())
        case 60: return .tlogFetchReply(rid: try r.u64(), try r.get())
        default:
            throw WireError("unknown message tag \(tag)")
        }
    }
}
