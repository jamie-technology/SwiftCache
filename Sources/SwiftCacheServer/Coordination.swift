import SwiftCacheCore

/// A coordinator. A cluster has 3 or 5; a majority must be reachable for the cluster to
/// elect a controller or start a new transaction-system generation. Coordinators provide
/// two things:
///
/// 1. **Leader election** for the cluster controller. Each coordinator nominates one
///    candidate and keeps nominating it while it heartbeats (a lease). A candidate that
///    holds a majority of nominations is the leader; a leader that can't renew a majority
///    of its leases steps down *before* any coordinator would nominate someone else.
///    Election is a liveness mechanism; safety does not depend on it.
///
/// 2. **A generation register** (single-decree Paxos over `CoreState`), persisted to disk.
///    This is the hard fence: a master can only start a generation by writing the
///    register with a ballot no coordinator majority has promised to exceed, so at most
///    one master can ever complete a given recovery, and a superseded master's write fails.
public final class CoordinatorRole: Role {
    public let context: RoleContext
    let config: ClusterConfig

    // Election (in memory; a restarted coordinator abstains for a lease period).
    private var nominee: LeaderInfo?
    private var nomineeRefreshed: UInt64 = 0
    private var candidates: [Endpoint: (info: LeaderInfo, lastSeen: UInt64)] = [:]
    private var abstainUntil: UInt64 = 0
    /// The last candidate that heartbeated as leader, and when.
    private var recentLeader: (info: LeaderInfo, at: UInt64)?

    // Generation register (durable).
    private(set) var readBallot = Ballot.zero
    private(set) var writeBallot = Ballot.zero
    private(set) var value: Bytes?
    static let diskKey = "generation-register"

    public init(context: RoleContext, config: ClusterConfig) {
        self.context = context
        self.config = config
    }

    public func start() {
        if let bytes = context.disk.read(Self.diskKey) {
            let state = try? WireReader.read(bytes) { r in (try r.get(Ballot.self), try r.get(Ballot.self), try r.optionalBlob()) }
            if let (rb, wb, v) = state {
                readBallot = rb; writeBallot = wb; value = v
            }
        }
        // We may have nominated someone before restarting: don't nominate anyone else
        // until that lease would have expired.
        abstainUntil = context.now + config.leaseTimeout
        context.log(.info, "coordinator started (register at \(writeBallot))")
    }

    private func persist() {
        var w = WireWriter()
        w.put(readBallot); w.put(writeBallot); w.optionalBlob(value)
        context.disk.write(Self.diskKey, w.bytes)
    }

    /// The candidate this coordinator currently holds a live lease for (checks).
    public var liveNominee: LeaderInfo? { nomineeLive ? nominee : nil }
    /// Restarted recently: nominates no one until a lease it may have granted expired.
    public var abstaining: Bool { context.now < abstainUntil }

    private var nomineeLive: Bool {
        nominee != nil && context.now &- nomineeRefreshed < config.leaseTimeout
    }

    public func receive(_ message: Message, from: Endpoint) {
        let now = context.now
        switch message {
        case .candidacy(let rid, let info):
            candidates[info.endpoint.processAddress] = (info, now)
            if !nomineeLive && now >= abstainUntil {
                // Nominate a candidate seen heartbeating as leader recently (a majority
                // elected it: join it), else the best recent candidate (deterministic so
                // coordinators agree).
                let recent = candidates.values.filter { now &- $0.lastSeen < config.leaseTimeout / 2 }
                let leader = recentLeader.flatMap { now &- $0.at < config.leaseTimeout / 2 ? $0.info : nil }
                if let best = leader ?? recent.map(\.info).min(by: { $0.endpoint < $1.endpoint }) {
                    if nominee != best { context.log(.info, "nominating \(best.endpoint)") }
                    nominee = best
                    nomineeRefreshed = now
                }
            }
            context.send(.nominationReply(rid: rid, nomineeLive ? nominee : nil), to: from)

        case .leaderHeartbeat(let rid, let info):
            recentLeader = (info, now)
            // Free to nominate (no live lease, not abstaining): back the leader that is
            // heartbeating. Only adds backing to a leader a majority already elected, and
            // keeps coordinators outside that majority from pointing clients at a candidate
            // that never became controller.
            if !nomineeLive && now >= abstainUntil {
                if nominee != info { context.log(.info, "nominating \(info.endpoint) (leader heartbeat)") }
                nominee = info
                nomineeRefreshed = now
            }
            let ok = nominee == info && nomineeLive
            if ok { nomineeRefreshed = now }
            context.send(.heartbeatReply(rid: rid, ok: ok), to: from)

        case .getLeader(let rid):
            context.send(.leaderReply(rid: rid, nomineeLive ? nominee : nil), to: from)

        case .genRead(let rid, let ballot):
            // Promise not to accept writes below `ballot`.
            if ballot > readBallot {
                readBallot = ballot
                persist()
            }
            let ok = ballot == readBallot
            context.send(.genReadReply(rid: rid, ok: ok, readBallot: readBallot, writeBallot: writeBallot, value: value), to: from)

        case .genWrite(let rid, let ballot, let newValue):
            let ok = ballot >= readBallot && ballot > writeBallot
            if ok {
                readBallot = ballot
                writeBallot = ballot
                value = newValue
                persist()
            }
            context.send(.genWriteReply(rid: rid, ok: ok, readBallot: readBallot), to: from)

        default:
            if let rid = message.rid, !message.isReply { context.send(.errorReply(rid: rid, .invalidRequest), to: from) }
        }
    }
}

/// Runs on every stateless process: campaigns to become cluster controller and, while
/// elected, hosts the `ClusterController` role. Steps down (halting the controller) as
/// soon as it can't renew a majority of coordinator leases.
public final class CandidateRole: Role {
    public let context: RoleContext
    let config: ClusterConfig
    let coordinators: [Endpoint]
    let host: ProcessHost
    let makeController: (LeaderInfo) -> ClusterController

    private var info: LeaderInfo
    /// This candidate's identity in elections.
    public var identity: LeaderInfo { info }

    private enum Leadership {
        case none
        /// A majority nominated us. A nomination's lease runs from when the coordinator
        /// granted it, possibly long before, so we don't lead until a majority acknowledges
        /// a heartbeat (which refreshes their leases on receipt, after we sent it).
        case confirming(since: UInt64)
        case leading
    }
    private var leadership = Leadership.none
    private var controllerEndpoint: Endpoint?
    /// Send time of the latest heartbeat round a majority acknowledged.
    private var lastMajority: UInt64 = 0
    private var round: UInt64 = 0

    /// Leader only while our lease holds by our own clock: a heartbeat round sent within
    /// `leaderStepDownTimeout` was acknowledged by a majority. Checked before every event
    /// this process handles, so a leader that wakes from a stall past its lease steps down
    /// before acting on anything.
    public var isLeader: Bool {
        if case .leading = leadership { return context.now &- lastMajority <= config.leaderStepDownTimeout }
        return false
    }

    init(context: RoleContext, config: ClusterConfig, coordinators: [Endpoint], host: ProcessHost,
         makeController: @escaping (LeaderInfo) -> ClusterController) {
        self.context = context
        self.config = config
        self.coordinators = coordinators
        self.host = host
        self.makeController = makeController
        self.info = LeaderInfo(endpoint: "", changeID: 0)
    }

    public func start() {
        newIdentity()
        host.beforeEvent { [weak self] in self?.checkLease() }
        context.every(config.leaderHeartbeatInterval) { [weak self] in self?.tick() }
    }

    private func checkLease() {
        switch leadership {
        case .leading where !isLeader:
            stepDown(reason: "lost majority of coordinator leases")
        case .confirming(let since) where context.now &- since > config.leaderStepDownTimeout:
            stepDown(reason: "election not confirmed by a majority of heartbeats")
        default:
            break
        }
    }

    private func newIdentity() {
        let id = context.randomID()
        info = LeaderInfo(endpoint: "\(context.processAddress)#cc-\(String(id % 0xFFFF_FFFF, radix: 16))", changeID: id)
    }

    private func tick() {
        round += 1
        switch leadership {
        case .leading:
            if let ep = controllerEndpoint, host.roles[ep] == nil { return stepDown(reason: "controller role stopped") }
            checkLease()
            if case .leading = leadership { heartbeat() }
        case .confirming:
            checkLease()
            if case .confirming = leadership { heartbeat() }
        case .none:
            campaign()
        }
    }

    private func campaign() {
        var votes = 0
        let me = info
        for c in coordinators {
            context.request(to: c, timeout: config.leaderHeartbeatInterval * 2, { .candidacy(rid: $0, me) }) { [weak self] reply in
                guard let self, case .none = self.leadership, self.info == me,
                      case .nominationReply(_, let nominee)? = reply, nominee == me else { return }
                votes += 1
                if votes == majority(self.coordinators.count) {
                    self.leadership = .confirming(since: self.context.now)
                    self.heartbeat()
                }
            }
        }
    }

    private func heartbeat() {
        var acks = 0
        let me = info
        // Leases are measured from when the heartbeat was *sent*: a coordinator
        // refreshes the lease no earlier than that, so our view never outlives theirs.
        let sentAt = context.now
        for c in coordinators {
            context.request(to: c, timeout: config.leaderHeartbeatInterval * 2, { .leaderHeartbeat(rid: $0, me) }) { [weak self] reply in
                guard let self, self.info == me, case .heartbeatReply(_, true)? = reply else { return }
                acks += 1
                guard acks == majority(self.coordinators.count) else { return }
                switch self.leadership {
                case .none: return
                case .leading: self.lastMajority = max(self.lastMajority, sentAt)
                case .confirming:
                    self.lastMajority = sentAt
                    self.leadership = .leading
                    guard self.isLeader else { return self.stepDown(reason: "heartbeat acks arrived after the lease they grant expired") }
                    self.context.log(.info, "elected cluster controller \(self.info.endpoint)")
                    let cc = self.makeController(self.info)
                    self.controllerEndpoint = cc.context.endpoint
                    self.host.add(cc)
                }
            }
        }
    }

    private func stepDown(reason: String) {
        context.log(.warning, "stepping down as cluster controller: \(reason)")
        leadership = .none
        lastMajority = 0
        if let ep = controllerEndpoint { host.remove(ep) }
        controllerEndpoint = nil
        newIdentity()
    }

    public func receive(_ message: Message, from: Endpoint) {}
}
