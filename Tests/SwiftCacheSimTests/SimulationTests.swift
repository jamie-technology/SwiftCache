import Testing
@testable import SwiftCacheSim

/// Full-cluster deterministic simulations with fault injection. `swiftcache-sim` runs
/// many more seeds (in release mode); these keep CI fast.
@Suite(.serialized) struct SimulationTests {
    static var quick: SimulationOptions {
        var o = SimulationOptions()
        o.duration = 8_000_000
        return o
    }

    @Test(arguments: [1, 2, 3, 4] as [UInt64])
    func chaosSeedPasses(seed: UInt64) {
        let r = runSimulation(seed: seed, options: Self.quick)
        #expect(r.failures.isEmpty, "seed \(seed):\n\(r.failures.prefix(5).joined(separator: "\n"))\n\(r.logTail.suffix(20).joined(separator: "\n"))")
        #expect((r.stats["final.checked"] ?? 0) == 1)
    }

    @Test func sameSeedSameExecution() {
        let a = runSimulation(seed: 42, options: Self.quick)
        let b = runSimulation(seed: 42, options: Self.quick)
        #expect(a.traceHash == b.traceHash)
        #expect(a.messages == b.messages)
        let c = runSimulation(seed: 43, options: Self.quick)
        #expect(a.traceHash != c.traceHash)
    }
}
