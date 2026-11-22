import ArgumentParser
import Foundation
import SwiftCacheCore
import SwiftCacheNet
import SwiftCacheServer

@main
struct Server: ParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "swiftcache-server",
        abstract: "A SwiftCache process. Its class decides which roles the cluster may give it.",
        discussion: """
        Classes:
          stateless  cluster controller candidate, master, commit proxies, resolvers
          log        transaction logs
          storage    one storage server
        Any process can also be a coordinator (--coordinator); every process must be given
        the same --coordinators list (the cluster file).
        """)

    @Option(help: "Process class: stateless, log or storage.") var `class`: String
    @Option(help: "Address to bind.") var listen = "0.0.0.0:4500"
    @Option(help: "Address other processes use to reach this one (host:port).") var advertise: String
    @Option(help: "Comma-separated coordinator addresses (host:port,...).") var coordinators: String
    @Flag(help: "This process is one of the coordinators (needs --data-dir).") var coordinator = false
    @Option(help: "Directory for durable coordinator state.") var dataDir: String?
    @Option(help: "Port for HTTP health probes (/ready, /live); 0 disables.") var healthPort: Int = 8080

    @Option(help: "Storage replicas per shard.") var replication: Int = 2
    @Option(help: "Transaction logs per generation.") var logs: Int = 3
    @Option(help: "Commit proxies per generation.") var proxies: Int = 2
    @Option(help: "MVCC window in seconds (how far back reads are served).") var mvccWindow: Double = 10
    @Option(help: "Seconds without a registration before a process is considered dead.") var failureTimeout: Double = 3
    @Option(help: "Seconds the last replica of a shard may be gone before its data is given up.") var shardLossTimeout: Double = 120
    @Option(help: "Log level: debug, info, warning, error.") var logLevel: String = "info"

    func run() throws {
        guard let cls = ProcessClass(name: self.class) else { throw ValidationError("--class must be stateless, log or storage") }
        if coordinator && dataDir == nil { throw ValidationError("coordinators need --data-dir for their durable state") }
        guard let (host, port) = parseEndpoint(listen) else { throw ValidationError("--listen must be host:port") }
        var config = ClusterConfig()
        config.replicationFactor = replication
        config.logReplicas = logs
        config.commitProxies = proxies
        config.mvccWindow = UInt64(mvccWindow * 1_000_000)
        config.failureTimeout = UInt64(failureTimeout * 1_000_000)
        config.shardLossTimeout = UInt64(shardLossTimeout * 1_000_000)
        // Real networks and schedulers are slower and noisier than the simulator.
        config.leaseTimeout = 3_000_000
        config.leaderHeartbeatInterval = 300_000
        config.leaderStepDownTimeout = 1_800_000
        config.registrationInterval = 300_000
        config.controllerWarmup = 2_000_000
        config.recoveryTimeout = 15_000_000
        config.requestTimeout = 2_000_000
        config.readWaitTimeout = 2_000_000
        config.snapshotTimeout = 15_000_000
        config.moveTimeout = 120_000_000
        let level: LogLevel = ["debug": .debug, "warning": .warning, "error": .error][logLevel.lowercased()] ?? .info

        let coordinatorEndpoints = coordinators.split(separator: ",").map {
            ServerProcess.coordinatorEndpoint(String($0).trimmingCharacters(in: .whitespaces))
        }
        let isCoordinator = coordinator
        let finalConfig = config
        let nodeHost = try NodeHost(address: advertise, dataDirectory: dataDir, logLevel: level)
        nodeHost.log(.info, "\(advertise): class=\(cls) coordinator=\(isCoordinator) coordinators=\(coordinators)")
        let holder = ProcessHolder()
        try nodeHost.run(bindHost: host, port: port, healthPort: healthPort == 0 ? nil : healthPort,
                         isReady: { holder.process?.isReady ?? false }) { env in
            let p = ServerProcess.make(env: env, processClass: cls, isCoordinator: isCoordinator,
                                       coordinators: coordinatorEndpoints, config: finalConfig)
            holder.process = p
            return p
        }
    }
}

/// Lets the health endpoint reach the process (only touched on the process's event loop).
final class ProcessHolder: @unchecked Sendable {
    var process: ProcessHost?
}
