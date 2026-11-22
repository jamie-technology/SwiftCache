import SwiftCacheCore

/// Runs on every process at `address#worker`. Finds the cluster controller through the
/// coordinators, registers with it periodically (the registration is the heartbeat),
/// creates roles when recruited, halts them when told, and forwards cluster information
/// (`ServerDBInfo`) to the roles that need it.
public final class WorkerAgent: Role {
    public let context: RoleContext
    let config: ClusterConfig
    let coordinators: [Endpoint]
    unowned let host: ProcessHost

    private(set) var dbInfo = ServerDBInfo.empty
    private var leader: Endpoint?
    private var coordinatorCursor = 0
    private var registering = false

    init(context: RoleContext, config: ClusterConfig, coordinators: [Endpoint], host: ProcessHost) {
        self.context = context
        self.config = config
        self.coordinators = coordinators
        self.host = host
    }

    var storage: StorageServer? { host.role(StorageServer.self) }

    public func start() {
        tick()
        context.every(config.registrationInterval) { [weak self] in self?.tick() }
    }

    private func tick() {
        guard let leader else {
            findLeader()
            return
        }
        guard !registering else { return }
        registering = true
        let reg = WorkerRegistration(worker: context.endpoint, processClass: host.processClass,
                                     roles: host.roles.keys.sorted(), storage: storage?.report)
        context.request(to: leader, timeout: config.failureTimeout / 2, { .registerWorker(rid: $0, reg) }) { [weak self] reply in
            guard let self else { return }
            self.registering = false
            switch reply {
            case .registerReply(_, let info, let tag)?:
                self.apply(info)
                self.storage?.assignTag(tag)
            default:
                // The controller is gone or no longer leader: rediscover.
                self.leader = nil
            }
        }
    }

    private func findLeader() {
        guard !coordinators.isEmpty else { return }
        let c = coordinators[coordinatorCursor % coordinators.count]
        coordinatorCursor += 1
        context.request(to: c, timeout: config.registrationInterval, { .getLeader(rid: $0) }) { [weak self] reply in
            guard let self, case .leaderReply(_, let info?)? = reply else { return }
            if self.leader == nil {
                self.leader = info.endpoint
                self.tick()
            }
        }
    }

    /// Accepts newer cluster info. A newer generation always wins; within a generation,
    /// a controller's later info wins, and a different controller's info only if recovered.
    private func apply(_ info: ServerDBInfo) {
        let cur = dbInfo
        let newer: Bool
        if info.generation != cur.generation {
            newer = info.generation > cur.generation
        } else if info.ccChangeID == cur.ccChangeID {
            newer = info.infoVersion > cur.infoVersion
        } else {
            newer = info.recovered
        }
        guard newer else { return }
        dbInfo = info
        if !info.clusterController.isEmpty { leader = info.clusterController }
        storage?.updateDBInfo(info)
    }

    public func receive(_ message: Message, from: Endpoint) {
        switch message {
        case .serverDBInfo(let info):
            apply(info)
        case .recruit(let rid, let spec):
            recruit(spec, rid: rid, from: from)
        case .halt(let role):
            host.remove(role)
        default:
            if let rid = message.rid, !message.isReply { context.send(.errorReply(rid: rid, .invalidRequest), to: from) }
        }
    }

    private func recruit(_ spec: RoleSpec, rid: UInt64, from: Endpoint) {
        let ep = host.makeEndpoint(spec.name)
        let ctx = host.makeContext(ep)
        let role: any Role
        switch spec {
        case .master(let s): role = MasterRole(context: ctx, config: config, spec: s, coordinators: coordinators)
        case .tlog(let s): role = TLogRole(context: ctx, config: config, spec: s)
        case .resolver(let s): role = ResolverRole(context: ctx, config: config, spec: s)
        case .proxy(let s): role = CommitProxy(context: ctx, config: config, spec: s)
        }
        context.log(.info, "recruited as \(spec.name): \(ep)")
        ctx.onReady = { [weak self] in
            self?.context.send(.recruitReply(rid: rid, endpoint: ep), to: from)
        }
        host.add(role)
    }
}

/// Builds the roles of a process.
public enum ServerProcess {
    /// - Parameters:
    ///   - coordinators: the cluster's coordinator endpoints (`host:port#coord`).
    ///   - isCoordinator: whether this process is one of them.
    public static func make(env: any ProcessEnvironment, processClass: ProcessClass, isCoordinator: Bool,
                            coordinators: [Endpoint], config: ClusterConfig) -> ProcessHost {
        let host = ProcessHost(env: env, processClass: processClass)
        host.onStart {
            if isCoordinator {
                host.add(CoordinatorRole(context: host.makeContext(coordinatorEndpoint(env.address)), config: config))
            }
            if processClass == .storage {
                host.add(StorageServer(context: host.makeContext(host.makeEndpoint("storage")), config: config))
            }
            host.add(WorkerAgent(context: host.makeContext(host.makeEndpoint("worker", unique: false)),
                                 config: config, coordinators: coordinators, host: host))
            if processClass == .stateless {
                let candidate = CandidateRole(
                    context: host.makeContext(host.makeEndpoint("candidate", unique: false)),
                    config: config, coordinators: coordinators, host: host,
                    makeController: { info in
                        ClusterController(context: host.makeContext(info.endpoint), config: config,
                                          leader: info, coordinators: coordinators)
                    })
                host.add(candidate)
            }
        }
        return host
    }

    public static func coordinatorEndpoint(_ address: Endpoint) -> Endpoint { "\(address)#coord" }
    public static func workerEndpoint(_ address: Endpoint) -> Endpoint { "\(address)#worker" }
}
