# Deterministic simulation

SwiftCache follows FoundationDB's approach: the production code is written to run inside a
deterministic simulator, and most correctness testing happens there. A simulation runs a whole
cluster (coordinator, storage servers and clients) on **one thread**, with a **virtual clock**,
a **seeded RNG** and a **simulated network**, while injecting faults. The same seed always
produces exactly the same execution, so any failure can be replayed and debugged.

```bash
swift run -c release swiftcache-sim --count 500                 # seeds 1..500
swift run -c release swiftcache-sim --seed 57 --verbose         # replay a single seed with full logs
swift run -c release swiftcache-sim --count 20 --check-determinism
swift run -c release swiftcache-sim --replicas 2 --storage 4 --scale-out 4 --seconds 60
```

`swift test` runs a handful of seeds plus a determinism check.

## What makes it deterministic

| Source of nondeterminism | How it's controlled |
| --- | --- |
| Time | Nodes only see `NodeEnvironment.now`; the simulator advances a virtual clock event by event. |
| Randomness | `NodeEnvironment.random` and the client transport draw from one seeded xoshiro256** generator. |
| Network | `Simulator.send` decides latency from the RNG and delivers through a time-ordered event queue. |
| Threads (server) | Server nodes are synchronous state machines called one event at a time. |
| Threads (client) | The **real async client library** runs on `SimExecutor`, a custom Swift `TaskExecutor` that queues jobs and runs them on the simulator thread, in FIFO order, between events. All client tasks are created with that executor preference (child tasks inherit it), and the transport resumes continuations from simulator events. Task interleavings are a pure function of the seed. |
| Hash ordering | Swift's `Dictionary` iteration order is randomized per process, so the code always sorts keys before iterating when order affects behaviour. |

Each run computes a trace hash over every delivered message and fault. The determinism test runs
a seed twice and compares hashes.

## Network model

- Per-directed-link FIFO delivery (like TCP), with random latency (100µs–2ms by default) plus
  occasional spikes.
- **Connections break.** Killing, restarting or partitioning a process bumps the epoch of all its
  links, and in-flight messages on old links are dropped, as when a TCP connection resets.
- Every message is round-tripped through the binary codec, so encoding bugs show up in
  simulation.

## Faults injected (`scheduleChaos`)

- Kill a storage server, then restart it with empty memory 0.2–3.2s later.
- Partition a storage server for 0.1–2.6s.
- Kill the coordinator and restart it 0.1–1.6s later (full recovery from storage reports).
- Partition the coordinator for 50–850ms.
- Latency storms: up to 20ms one-way for a second.
- Scale-out: three new storage servers join a quarter of the way through, forcing shard
  formation and live slot migration under load and faults.

Storage faults are only injected when the cluster is fully healthy, judged by both the
simulator's ground truth and the coordinator's view. So at most one replica of any shard is down
at once, and the data is always recoverable. (Killing every replica of an in-memory shard *does*
lose its data. That is correct behaviour, not a bug.)

## Workloads and invariants

| Workload | What it checks |
| --- | --- |
| **bank** | Random transfers between 20 accounts. Audits (point reads or a cross-shard range read) must always total 2,000. Checks serializability and range-read consistency. |
| **cycle** | 16 keys form a permutation cycle mutated by 3-key swaps (FoundationDB's classic test). It must remain one cycle of length 16. |
| **cas** | `getVersioned` + `compareAndSwap` increments, both inside one transaction and across two transactions (the `versionMismatch` path), each writing `cas/log/<n>` atomically with the counter. The log must be exactly `1...counter`: no lost or duplicated updates. Versionstamps in mismatches must move forward. |
| **add** | Blind atomic adds. The final total must lie within [acknowledged, attempted]. |
| **history** | Writes with versionstamps, then re-reads earlier writes at their commit version (`read(atVersion:)`). Must see exactly that value and versionstamp; reads just before a write must not see it. Versionstamps must increase. Checks MVCC and historical reads across crashes, migrations and recoveries. |

After the fault period every process is healed, the cluster quiesces, and a fresh client re-checks
all invariants. The run also fails if a workload made no progress at all, or if scale-out didn't
produce new shards.

## Bugs the simulator found during development

These all passed ordinary local testing:

1. **Replica self-wipe on version gaps.** A brief coordinator partition dropped batches. Every
   replica saw a gap and wiped itself, losing whole shards. Fix: the coordinator retains
   unacknowledged batches and resends them.
2. **False failure detection.** One dead replica stalled the commit pipeline, healthy replicas
   stopped receiving batches (so stopped acknowledging), and all were declared failed. Fix:
   only count time spent owing an acknowledgement.
3. **Recovery declaring an empty cluster.** A coordinator partitioned during recovery lost all its
   `recover` replies and bootstrapped a fresh, empty cluster. Fix: resend, and require every
   shard to confirm, or restart recovery.
4. **Double-applied adds** after commits with unknown results (see transactions.md).

## Writing new workloads

A workload is an `async` function taking a `Database` built on `sim.makeClientTransport()`. Use
`sim.random` for every random choice, `sim.now` for time, and `sim.fail(...)` to record a
violation. Never use `Date()`, `Task.sleep`, `UInt64.random` or unstructured `Task {}` inside
simulated code; they escape the deterministic executor or the virtual clock.
