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
            var r = WireReader(bytes)
            if let rb = try? r.get(Ballot.self), let wb = try? r.get(Ballot.self), let v = try? r.optionalBlob() {
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

    private var nomineeLive: Bool {
        nominee != nil && context.now &- nomineeRefreshed < config.leaseTimeout
    }

    public func receive(_ message: Message, from: Endpoint) {
        let now = context.now
        switch message {
        case .candidacy(let rid, let info):
            candidates[info.endpoint.processAddress] = (info, now)
            if !nomineeLive && now >= abstainUntil {
                // Nominate the best recent candidate (deterministic so coordinators agree).
                let recent = candidates.values.filter { now &- $0.lastSeen < config.leaseTimeout / 2 }
                if let best = recent.map(\.info).min(by: { $0.endpoint < $1.endpoint }) {
                    if nominee != best { context.log(.info, "nominating \(best.endpoint)") }
                    nominee = best
                    nomineeRefreshed = now
                }
            }
            context.send(.nominationReply(rid: rid, nomineeLive ? nominee : nil), to: from)

        case .leaderHeartbeat(let rid, let info):
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
    public private(set) var isLeader = false
    private var controllerEndpoint: Endpoint?
    private var lastMajority: UInt64 = 0
    private var round: UInt64 = 0

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
        context.every(config.leaderHeartbeatInterval) { [weak self] in self?.tick() }
    }

    private func newIdentity() {
        let id = context.randomID()
        info = LeaderInfo(endpoint: "\(context.processAddress)#cc-\(String(id % 0xFFFF_FFFF, radix: 16))", changeID: id)
    }

    private func tick() {
        round += 1
        let thisRound = round
        if isLeader {
            if let ep = controllerEndpoint, host.roles[ep] == nil {
                stepDown(reason: "controller role stopped")
                return
            }
            if context.now &- lastMajority > config.leaderStepDownTimeout {
                stepDown(reason: "lost majority of coordinator leases")
                return
            }
            var acks = 0
            // Leases are measured from when the heartbeat was *sent*: a coordinator
            // refreshes the lease no earlier than that, so our view never outlives theirs.
            let sentAt = context.now
            for c in coordinators {
                context.request(to: c, timeout: config.leaderHeartbeatInterval * 2, { .leaderHeartbeat(rid: $0, self.info) }) { [weak self] reply in
                    guard let self, self.isLeader, case .heartbeatReply(_, let ok)? = reply else { return }
                    if ok {
                        acks += 1
                        if acks == majority(self.coordinators.count) { self.lastMajority = max(self.lastMajority, sentAt) }
                    } else if thisRound == self.round {
                        // A coordinator has moved on: we may no longer hold a majority.
                    }
                }
            }
        } else {
            var votes = 0
            let me = info
            for c in coordinators {
                context.request(to: c, timeout: config.leaderHeartbeatInterval * 2, { .candidacy(rid: $0, me) }) { [weak self] reply in
                    guard let self, !self.isLeader, self.info == me, case .nominationReply(_, let nominee)? = reply else { return }
                    if nominee == me {
                        votes += 1
                        if votes == majority(self.coordinators.count) { self.becomeLeader() }
                    }
                }
            }
        }
    }

    private func becomeLeader() {
        isLeader = true
        lastMajority = context.now
        context.log(.info, "elected cluster controller \(info.endpoint)")
        let cc = makeController(info)
        controllerEndpoint = cc.context.endpoint
        host.add(cc)
    }

    private func stepDown(reason: String) {
        context.log(.warning, "stepping down as cluster controller: \(reason)")
        isLeader = false
        if let ep = controllerEndpoint { host.remove(ep) }
        controllerEndpoint = nil
        newIdentity()
    }

    public func receive(_ message: Message, from: Endpoint) {}
}
