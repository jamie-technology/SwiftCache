import ArgumentParser
import Foundation
import SwiftCacheCore
import SwiftCacheNet
import SwiftCacheServer

struct ClusterOptions: ParsableArguments {
    @Option(help: "Replicas per shard (coordinator).") var replication: Int = 2
    @Option(help: "MVCC window in seconds: how far back historical reads are served.") var mvccWindow: Double = 10
    @Option(help: "Seconds without acknowledgements before a replica is declared failed.") var failureTimeout: Double = 3
    @Option(help: "Seconds the last live replica of a shard may be silent before its data is given up.") var shardLossTimeout: Double = 120
    @Option(help: "Seconds a restarting coordinator waits for storage reports.") var recoveryWindow: Double = 3
    @Option(help: "Seconds a storage server waits before rejoining a silent coordinator.") var coordinatorTimeout: Double = 2
    @Option(help: "Log level: debug, info, warning, error.") var logLevel: String = "info"

    var config: ClusterConfig {
        var c = ClusterConfig()
        c.replicationFactor = replication
        c.mvccWindow = UInt64(mvccWindow * 1_000_000)
        c.failureTimeout = UInt64(failureTimeout * 1_000_000)
        c.shardLossTimeout = UInt64(shardLossTimeout * 1_000_000)
        c.recoveryWindow = UInt64(recoveryWindow * 1_000_000)
        c.coordinatorTimeout = UInt64(coordinatorTimeout * 1_000_000)
        // Real networks: allow more time for snapshots and slow reads than the simulator.
        c.readWaitTimeout = 2_000_000
        c.snapshotTimeout = 15_000_000
        c.moveTimeout = 120_000_000
        return c
    }

    var level: LogLevel {
        switch logLevel.lowercased() {
        case "debug": .debug
        case "warning", "warn": .warning
        case "error": .error
        default: .info
        }
    }
}

func splitListen(_ s: String) throws -> (String, Int) {
    guard let (h, p) = parseEndpoint(s) else { throw ValidationError("expected host:port, got \(s)") }
    return (h, p)
}

@main
struct Server: ParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "swiftcache-server",
        abstract: "SwiftCache server processes.",
        subcommands: [CoordinatorCommand.self, StorageCommand.self])
}

struct CoordinatorCommand: ParsableCommand {
    static let configuration = CommandConfiguration(commandName: "coordinator", abstract: "Run the coordinator (sequencer, resolver, cluster controller).")
    @Option(help: "Address to bind.") var listen = "0.0.0.0:7400"
    @Option(help: "Endpoint other processes use to reach this one (host:port).") var advertise: String
    @OptionGroup var cluster: ClusterOptions

    func run() throws {
        let (host, port) = try splitListen(listen)
        let config = cluster.config
        let advertise = self.advertise
        let nodeHost = NodeHost(endpoint: advertise, logLevel: cluster.level)
        nodeHost.log(.info, "coordinator \(advertise) replication=\(config.replicationFactor) mvccWindow=\(config.mvccWindow / 1_000_000)s")
        try nodeHost.run(bindHost: host, port: port) { env in
            Coordinator(endpoint: advertise, config: config, env: env)
        }
    }
}

struct StorageCommand: ParsableCommand {
    static let configuration = CommandConfiguration(commandName: "storage", abstract: "Run a storage server (one replica of one shard).")
    @Option(help: "Address to bind.") var listen = "0.0.0.0:7500"
    @Option(help: "Endpoint other processes use to reach this one (host:port).") var advertise: String
    @Option(help: "Coordinator endpoint (host:port).") var coordinator: String
    @OptionGroup var cluster: ClusterOptions

    func run() throws {
        let (host, port) = try splitListen(listen)
        let config = cluster.config
        let advertise = self.advertise, coordinator = self.coordinator
        let nodeHost = NodeHost(endpoint: advertise, logLevel: cluster.level)
        nodeHost.log(.info, "storage \(advertise) coordinator=\(coordinator)")
        try nodeHost.run(bindHost: host, port: port) { env in
            StorageServer(endpoint: advertise, coordinator: coordinator, config: config, env: env)
        }
    }
}
