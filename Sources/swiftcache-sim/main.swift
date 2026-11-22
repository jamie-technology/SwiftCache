import ArgumentParser
import SwiftCacheSim

@main
struct SimCommand: ParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "swiftcache-sim",
        abstract: "Run SwiftCache's deterministic cluster simulation with fault injection.")

    @Option(help: "First seed to run.") var seed: UInt64 = 1
    @Option(help: "Number of consecutive seeds to run.") var count: Int = 1
    @Option(help: "Virtual seconds of workload per seed.") var seconds: UInt64 = 20
    @Option(help: "Initial storage servers.") var storage: Int = 6
    @Option(help: "Replication factor.") var replicas: Int = 3
    @Option(help: "Storage servers added mid-run (scale-out).") var scaleOut: Int = 3
    @Option(help: "Client tasks.") var clients: Int = 10
    @Flag(help: "Disable fault injection.") var noChaos = false
    @Flag(help: "Only storage faults (no faults in the transaction system).") var storageFaultsOnly = false
    @Flag(help: "Print every log line.") var verbose = false
    @Option(help: "Run a targeted scenario instead of random chaos (or 'all'): \(Scenario.allCases.map(\.rawValue).joined(separator: ", ")).")
    var scenario: String?
    @Flag(help: "Run each seed twice and verify identical traces.") var checkDeterminism = false

    func run() throws {
        var options = SimulationOptions()
        options.duration = seconds * 1_000_000
        options.storageServers = storage
        options.replicationFactor = replicas
        options.scaleOutServers = scaleOut
        options.clients = clients
        options.chaos = !noChaos
        options.transactionSystemFaults = !storageFaultsOnly
        options.verbose = verbose
        var failed = 0
        let scenarios: [Scenario?]
        if scenario == "all" { scenarios = Scenario.allCases } else if let name = scenario {
            guard let sc = Scenario(rawValue: name) else { throw ValidationError("unknown scenario \(name)") }
            scenarios = [sc]
        } else { scenarios = [nil] }
        var runs = 0
        for sc in scenarios {
        options.scenario = sc
        if let sc { print("== scenario \(sc.rawValue)") }
        for s in seed..<(seed + UInt64(count)) {
            runs += 1
            let r = runSimulation(seed: s, options: options)
            var line = "seed \(s): \(r.passed ? "PASS" : "FAIL") trace=\(String(r.traceHash, radix: 16)) msgs=\(r.messages)"
            let interesting = ["recoveries", "bank.commits", "cycle.commits", "cas.success", "add.commits", "orders.commits", "history.commits", "history.historicalReads"]
            line += " " + interesting.map { "\($0)=\(r.stats[$0] ?? 0)" }.joined(separator: " ")
            if checkDeterminism {
                let again = runSimulation(seed: s, options: options)
                if again.traceHash != r.traceHash {
                    line += " NONDETERMINISTIC (second run trace=\(String(again.traceHash, radix: 16)))"
                    failed += 1
                }
            }
            print(line)
            if !r.passed {
                failed += 1
                for f in r.failures.prefix(10) { print("  ✗ \(f)") }
                print("  --- log tail ---")
                for l in r.logTail.suffix(40) { print("  \(l)") }
            }
        }
        }
        print("\(runs - failed)/\(runs) runs passed")
        if failed > 0 { throw ExitCode.failure }
    }
}
