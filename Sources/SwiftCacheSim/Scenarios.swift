import SwiftCache
import SwiftCacheCore
import SwiftCacheServer

/// Targeted fault scenarios for the distributed transaction system. Each runs the full
/// workload mix (and its invariants) plus scenario-specific assertions.
public enum Scenario: String, CaseIterable, Sendable {
    /// The controller's process is partitioned: it must step down, a new controller must
    /// take over, and after healing exactly one controller remains.
    case staleController
    /// Repeated controller crashes with coordinator partitions: never two leaders at once.
    case dualElection
    /// The master (sequencer) crashes repeatedly after handing out versions: versions are
    /// never reused.
    case sequencerDies
    /// Commit proxies crash under load, some after logging but before replying: idempotent
    /// operations still apply exactly once.
    case proxyDiesAfterLogging
    /// Log processes die mid-replication: no acknowledged commit is lost.
    case logDiesDuringReplication
    /// A storage replica is cut off briefly, repeatedly: it catches up from the logs (it
    /// isn't replaced) and ends identical to its peers.
    case storageFallsBehind
    /// The master's and proxies' processes are partitioned and come back after a new
    /// generation formed: the old generation's roles must stop and commit nothing.
    case oldGenerationReconnects
    /// A coordinator majority disappears: the running generation keeps committing, but no
    /// new generation can form; when they return, the cluster recovers.
    case coordinatorQuorumLoss
}

/// Invariants checked continuously in every simulation.
final class Monitor: @unchecked Sendable {
    /// generation -> master that brought it to life.
    var liveMasters: [UInt64: Endpoint] = [:]
    /// generation -> (first version allocated, highest version committed).
    var versions: [UInt64: (first: Version, committed: Version)] = [:]
    /// Simulated time at which each generation was first seen live.
    var liveAt: [UInt64: UInt64] = [:]

    func check(_ sim: Simulator) {
        // At most one master may ever complete recovery of a given generation.
        for m in sim.roles(MasterRole.self) where m.phase == .running || m.phase == .recruitingProxies {
            if let other = liveMasters[m.generation], other != m.context.endpoint {
                sim.fail("two masters recovered generation \(m.generation): \(other) and \(m.context.endpoint)")
            }
            if liveMasters[m.generation] == nil { liveAt[m.generation] = sim.now }
            liveMasters[m.generation] = m.context.endpoint
            if m.firstVersion != 0 {
                let cur = versions[m.generation]
                versions[m.generation] = (cur?.first ?? m.firstVersion, max(cur?.committed ?? 0, m.committed))
            }
        }
        // Versions never go backwards across generations.
        let gens = versions.keys.sorted()
        for (a, b) in zip(gens, gens.dropFirst()) where versions[b]!.first <= versions[a]!.committed {
            sim.fail("generation \(b) starts at \(versions[b]!.first), not above generation \(a)'s committed \(versions[a]!.committed)")
        }
        // Leases: never two processes that believe they are the controller.
        let leaders = sim.roles(CandidateRole.self).filter(\.isLeader)
        if leaders.count > 1 {
            sim.fail("two controllers at once: \(leaders.map(\.context.endpoint))")
        }
    }

    func install(_ sim: Simulator) {
        func tick() {
            check(sim)
            sim.after(20_000) { tick() }
        }
        sim.after(20_000) { tick() }
    }
}

/// After the run: every active replica of a shard must hold identical data.
func checkReplicaConsistency(_ sim: Simulator) {
    guard let cc = sim.controller else { return }
    let storage = sim.roles(StorageServer.self)
    for shard in cc.status.topology.shards {
        let replicas = shard.replicas.compactMap { ep in storage.first { $0.context.endpoint == ep } }
        guard replicas.count > 1 else { continue }
        let v = replicas.map(\.applied).min()!
        let first = replicas[0].debugContents(at: v)
        for r in replicas.dropFirst() {
            let other = r.debugContents(at: v)
            if other != first {
                let diff = Set(first.keys).symmetricDifference(other.keys).count
                sim.fail("shard \(shard.id): replicas \(replicas[0].context.endpoint) and \(r.context.endpoint) differ at version \(v) (\(first.count) vs \(other.count) keys, \(diff) differing keys)")
            }
        }
    }
}

func scheduleScenario(_ sim: Simulator, _ scenario: Scenario, monitor: Monitor, counters: Counters) {
    let t0 = sim.now
    func at(_ offset: UInt64, _ action: @escaping () -> Void) { sim.at(t0 + offset, action) }
    func pick(_ xs: [Endpoint]) -> Endpoint? { xs.isEmpty ? nil : xs[Int(sim.random.below(UInt64(xs.count)))] }

    switch scenario {
    case .staleController:
        at(2_000_000) {
            guard let cc = sim.controller else { return sim.fail("no controller to partition") }
            let addr = cc.context.endpoint.processAddress
            sim.trace("scenario: partitioning controller process \(addr)")
            sim.partition(addr, for: 5_000_000)
            sim.after(1_000_000) {
                if sim.host(addr)?.role(CandidateRole.self)?.isLeader == true {
                    sim.fail("partitioned controller still believes it leads after 1s")
                }
            }
            sim.after(4_500_000) {
                guard let new = sim.controller, new.context.endpoint.processAddress != addr else {
                    return sim.fail("no new controller elected while the old one was partitioned")
                }
            }
            sim.after(8_000_000) {
                let live = sim.roles(ClusterController.self).filter { !$0.context.stopped }
                if live.count != 1 { sim.fail("expected exactly one controller after healing, found \(live.count)") }
            }
        }

    case .dualElection:
        for round in 0..<6 {
            at(1_500_000 + UInt64(round) * 1_500_000) {
                if let cc = sim.controller {
                    let addr = cc.context.endpoint.processAddress
                    sim.kill(addr)
                    sim.after(900_000) { sim.restart(addr) }
                }
                if let c = pick(sim.coordinatorAddresses), sim.isAlive(c) {
                    sim.partition(c, for: 700_000)
                }
            }
        }

    case .sequencerDies:
        for round in 0..<5 {
            at(1_500_000 + UInt64(round) * 1_500_000) {
                if let m = sim.roles(MasterRole.self).first(where: { $0.phase == .running }) {
                    sim.killRole(m.context.endpoint)
                }
            }
        }

    case .proxyDiesAfterLogging:
        for round in 0..<14 {
            at(1_500_000 + UInt64(round) * 300_000) {
                let proxies = sim.roles(CommitProxy.self).filter { !$0.dead }
                if let p = pick(proxies.map(\.context.endpoint)) { sim.killRole(p) }
            }
        }

    case .logDiesDuringReplication:
        for round in 0..<4 {
            at(1_500_000 + UInt64(round) * 1_500_000) {
                if let l = pick(sim.logAddresses) {
                    sim.kill(l)
                    sim.after(600_000) { sim.restart(l) }
                }
            }
        }

    case .storageFallsBehind:
        at(1_500_000) {
            let members = sim.roles(StorageServer.self).filter { $0.tag != 0 && $0.isActive }
            guard let victim = members.first else { return sim.fail("no storage member") }
            let addr = victim.context.processAddress, tag = victim.tag
            sim.trace("scenario: storage \(addr) (tag \(tag)) will fall behind")
            for i in 0..<3 {
                sim.after(UInt64(i) * 1_500_000) { sim.partition(addr, for: 900_000) }
            }
            sim.after(7_000_000) {
                guard let s = sim.host(addr)?.role(StorageServer.self) else { return sim.fail("storage process gone") }
                if s.tag != tag { sim.fail("lagging storage was replaced (tag \(tag) -> \(s.tag)) instead of catching up from the logs") }
                let peersApplied = sim.roles(StorageServer.self).filter { $0.tag != 0 }.map(\.applied).max() ?? 0
                if peersApplied > s.applied + 2_000_000 { sim.fail("lagging storage did not catch up (\(s.applied) vs \(peersApplied))") }
            }
        }

    case .oldGenerationReconnects:
        at(2_000_000) {
            guard let master = sim.roles(MasterRole.self).first(where: { $0.phase == .running }) else { return }
            let gen = master.generation
            var addrs = Set([master.context.processAddress])
            for p in sim.roles(CommitProxy.self) where p.generation == gen { addrs.insert(p.context.processAddress) }
            // Keep a coordinator majority and a stateless process reachable, so a new
            // generation can form while the old one is isolated.
            let coords = Set(sim.coordinatorAddresses)
            while addrs.intersection(coords).count >= majority(coords.count), let drop = addrs.intersection(coords).sorted().last {
                addrs.remove(drop)
            }
            if Set(sim.statelessAddresses).subtracting(addrs).isEmpty, let keep = addrs.sorted().last { addrs.remove(keep) }
            let partitionedNewer = { monitor.liveMasters.keys.filter { $0 > gen } }
            sim.trace("scenario: partitioning generation \(gen)'s master/proxies: \(addrs.sorted())")
            for a in addrs.sorted() { sim.partition(a, for: 4_000_000) }
            sim.after(3_900_000) {
                if partitionedNewer().isEmpty { sim.fail("no new generation formed while the old one was partitioned") }
            }
            sim.after(8_000_000) {
                let stale = sim.roles(CommitProxy.self).filter { $0.generation <= gen && !$0.dead }
                if !stale.isEmpty { sim.fail("old-generation proxies still running after reconnecting: \(stale.map(\.context.endpoint))") }
            }
        }

    case .coordinatorQuorumLoss:
        at(2_000_000) {
            let victims = Array(sim.coordinatorAddresses.prefix(majority(sim.coordinatorAddresses.count)))
            let genBefore = monitor.liveMasters.keys.max() ?? 0
            let commitsBefore = counters["bank.commits"] + counters["add.commits"]
            sim.trace("scenario: killing coordinator quorum \(victims)")
            for v in victims { sim.kill(v) }
            sim.after(5_000_000) {
                let formed = monitor.liveMasters.keys.filter { $0 > genBefore && (monitor.liveAt[$0] ?? 0) < sim.now }
                if !formed.isEmpty { sim.fail("generation(s) \(formed) formed without a coordinator quorum") }
                let progress = counters["bank.commits"] + counters["add.commits"] - commitsBefore
                sim.trace("scenario: \(progress) commits while coordinators were down")
                for v in victims { sim.restart(v) }
            }
            sim.after(15_000_000) {
                guard let cc = sim.controller, cc.phase == .running else {
                    return sim.fail("cluster did not recover after the coordinator quorum returned")
                }
            }
        }
    }
}
