# Deterministic simulation

SwiftCache follows FoundationDB's approach: production code is written to run inside a
deterministic simulator, and most correctness testing happens there. A simulation runs a
whole cluster on **one thread**, with a **virtual clock**, a **seeded RNG**, a
**simulated network** and **simulated disks**, while injecting faults:

- coordinators, stateless processes (controller, master, proxies, resolver), log processes,
  storage processes and clients;
- the real server roles and the real Swift client library.

The same seed always produces exactly the same execution, so any failure can be replayed
and debugged.

```bash
swift run -c release swiftcache-sim --count 500                      # random chaos, seeds 1..500
swift run -c release swiftcache-sim --scenario all --count 25        # every targeted scenario × 25 seeds
swift run -c release swiftcache-sim --scenario staleController --seed 3 --verbose
swift run -c release swiftcache-sim --count 20 --check-determinism
swift run -c release swiftcache-sim --storage-faults-only            # leave the transaction system alone
```

`swift test` runs a few chaos seeds, every scenario once, and a determinism check.

## What makes it deterministic

| Source of nondeterminism | How it's controlled |
| --- | --- |
| Time | Roles only see `ProcessEnvironment.now`; the simulator advances a virtual clock event by event. |
| Randomness | Role ids, election jitter, workload choices and network latency all come from one seeded xoshiro256** generator. |
| Network | `Simulator.send` decides latency from the RNG and delivers through a time-ordered event queue. |
| Disk | `SimDisk` per process, which survives kill and restart (coordinator state). |
| Threads (server) | Roles are synchronous state machines called one event at a time. |
| Threads (client) | The real async client library runs on `SimExecutor`, a custom Swift `TaskExecutor` that runs queued jobs on the simulator thread, in FIFO order, between events. Task interleavings are a pure function of the seed. |
| Hash ordering | Swift's `Dictionary` order is randomized per process, so code always sorts keys before iterating where order matters. |

Each run hashes every delivered message and fault into a trace hash. The determinism check
runs a seed twice and compares the hashes.

## Network model

- Per-link FIFO delivery (like TCP) between processes, with random latency (100 µs–2 ms)
  and occasional 200 ms spikes.
- **Connections break.** Killing, restarting or partitioning a process drops everything in
  flight to or from it, as a TCP reset does.
- Every message is round-tripped through the binary codec.

## Random chaos

Faults are injected every 0.2–0.8 s, but only while the cluster is fully healthy (judged by
both the simulator's ground truth and the controller's view). So, by design, no shard ever
loses every replica and no generation loses every log:

| Fault | What it exercises |
| --- | --- |
| Kill a storage process, restart it 0.2–3.2 s later (empty) | Replica removal and re-replication (`addReplica` snapshot copy) |
| Partition a storage process | Catch-up from the logs, or removal and re-add |
| Kill a log process | Recovery onto the surviving logs, then restoring 3 logs |
| Kill a stateless / coordinator process | Losing the controller, master, proxies or resolver |
| Crash a single role (master, proxy, resolver, log, controller) inside a live process | Role-level failure detection and recovery |
| Partition the controller's process | Lease step-down, re-election, fencing |
| Kill a coordinator majority for 1–4 s | No progress without quorum, then recovery |
| Partition a coordinator or log | Lock and recovery with unreachable participants |
| Latency storms (20 ms) | Timeouts, retries, duplicate requests |
| Scale-out: 3 storage servers join mid-run | Shard formation and live migration under faults |

## Targeted scenarios

`--scenario <name>`, each also running the full workload mix:

| Scenario | Faults | Asserts |
| --- | --- | --- |
| `staleController` | Partition the controller's process for 5 s | It steps down within 1 s; a new controller is elected elsewhere; exactly one remains after healing |
| `dualElection` | Kill the controller's process 6 times while partitioning coordinators | Never two leaders at once (continuous check) |
| `sequencerDies` | Crash the running master 5 times | Versions never reused or regressing across generations |
| `proxyDiesAfterLogging` | Crash commit proxies every 300 ms | Idempotent operations apply exactly once; no acknowledged commit lost |
| `logDiesDuringReplication` | Kill log processes under load | No acknowledged commit lost; durability policy restored |
| `storageFallsBehind` | Cut one replica off for 0.9 s, three times | It catches up from the logs (not replaced) and ends identical to its peers |
| `oldGenerationReconnects` | Partition the running generation's master and proxies (keeping a coordinator majority reachable) | A new generation forms meanwhile; after healing the old roles stop without committing anything |
| `coordinatorQuorumLoss` | Kill 2 of 3 coordinators for 5 s | No new generation forms without quorum; the running one keeps committing; recovery after they return |

## Invariants checked in every run

**Continuously (every 20 ms):**

- At most one master ever completes recovery of a given generation.
- Each generation's first version is above everything the previous generation committed.
- Never two processes that both believe they are the controller.

**By workloads:**

| Workload | Invariant |
| --- | --- |
| **bank** | Random transfers between 20 accounts. Audits (point reads or a cross-shard range read) always total 2,000 (serializability). |
| **cycle** | 16 keys form a permutation cycle mutated by 3-key swaps (FoundationDB's classic test). It stays one cycle. |
| **cas** | `getVersioned` + `compareAndSwap` increments, in one transaction and across two (the `versionMismatch` path), each writing `cas/log/<n>` atomically. The log is exactly `1...counter`. |
| **add** | Idempotent atomic adds. The total equals the acknowledged adds exactly (plus at most one per add that gave up with an unknown outcome). |
| **orders** | Idempotent transactions that generate a **new id per attempt**. Every acknowledged id exists, no id is returned twice, and the count equals the number of orders. |
| **history** | Writes, then re-reads earlier writes at their commit version (`read(atVersion:)`). Exact match; reads before a write don't see it. |
| **global** | No versionstamp is ever handed to two commits. |

**At the end**, after healing and quiescing: every invariant is re-checked by a fresh client;
all active replicas of each shard hold identical data at a common version; every workload
made progress; and the cluster has a controller and fully replicated shards.

## Bugs the simulator found

**Milestone 1 (single coordinator):**

1. Replicas wiped themselves after a brief coordinator partition (version gap).
2. A stalled commit pipeline made healthy replicas look dead.
3. A partition during recovery made the coordinator bootstrap an empty cluster.

**Milestone 2 (distributed transaction system):**

4. **A lost version-request reply wedged a generation.** A proxy with two outstanding version
   requests retried the first after its reply was lost. The master only remembered its last
   reply per proxy, so it allocated a *new* version. The original version was never pushed,
   and every log waited for it forever. Fix: the master (and resolver) remember recent
   replies per request. Backstop: a master whose generation stops committing ends it.
5. Proxies recruited during recovery asked for versions before the master considered itself
   running, and shut themselves down.
6. A controller role killed on its own left its candidate renewing the lease with no
   controller behind it.
7. Leases measured from heartbeat *replies* could outlive the coordinators' view under
   latency spikes. They are now measured from heartbeat *sends*.
8. Roles from generations run by a previous controller were never halted. They were harmless
   (fenced), but lingered.
9. A generation formed while a log process was down stayed at 2 of 3 logs indefinitely.
10. **Acknowledged write lost during a migration.** After a partition, a proxy's timed-out
    version requests were retried on independent timers and reached the master out of
    order. The master handed out versions in arrival order, so the proxy's batches were no
    longer in version order. The proxy then skipped a relayed `createShard` as "already
    applied" and routed a mid-migration write without the destination shard. Fix: the
    master serves each proxy's requests strictly in request-number order (as FoundationDB
    does), and a proxy that ever sees its batch versions go backwards ends its generation.
11. **Shard map lost in recovery.** A log compacted the metadata tag as soon as it stored a
    new shard-map snapshot. That snapshot's version was above the recovery version (it wasn't
    on every log yet), so recovery discarded it, and the history it had replaced was already
    gone. The cluster came back with an empty map. Fix: compact only once the snapshot is
    known committed on every log.

## Writing new workloads

A workload is an `async` function taking a `Database` built on
`sim.makeClientTransport()`. Use `sim.random` for every random choice, `sim.now` for time,
and `sim.fail(...)` to record a violation. Never use `Date()`, `Task.sleep`,
`UInt64.random` or unstructured `Task {}` in simulated code: they escape the deterministic
executor or the virtual clock.
