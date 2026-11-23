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
| Pause any process for 50–950 ms (GC / scheduler stall: nothing delivered, no timers, then everything released in order) | Leases outliving their holder, stale decisions surfacing late |
| Latency storms (20 ms) | Timeouts, retries, duplicate requests |
| Clock-rate skew: every process's clock runs at 0.92–1.08× real time | Lease margins under relative drift (see below) |
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
| `masterPause` | Freeze the running master for 0.45 s or 2 s, four times | A master that wakes with an expired lease never hands out a stale read version |
| `delayedOldGeneration` | Hold everything the running master sends (lease renewals, read versions, forwarded resolutions) for 3 s while it keeps working, then release it all | A new generation forms meanwhile; the released messages can't extend old authority, change new state or give a client a stale version |
| `lostPushes` | Lose every push to one log for 400 ms, three times (well under failure detection) | Proxies re-push uncommitted batches: the same generation resumes committing without a recovery; no acknowledged commit lost |
| `logPauseDuringRecovery` | Crash the master while freezing a log (holding back its lock reply) | Recovery waits out the lease on the frozen log or proceeds with the others, correctly either way |

### Timing assumptions

Read versions are served by the master under leases granted by every log: 240 ms on the
master's clock against 300 ms on the log's, the log's lease starting when it *receives* the
renewal. That is safe if processes' monotonic clocks advance at rates within a factor of
1.25 of each other (r_log / r_master ≤ 300/240). The simulator runs every process at a
random rate in [0.92, 1.08] (worst relative ratio ≈ 1.17), pauses processes, and delays
outgoing messages, so the protocol is tested inside its assumptions. Production uses
`CLOCK_MONOTONIC` (NTP slews it by at most ±500 ppm). The simulator validates the protocol
under these assumptions; it can't prove the real runtime meets them.

## Invariants checked in every run

**Continuously (every 20 ms):**

- After any acknowledged commit, a freshly obtained read version includes it (checked by the
  history workload on every write).

- At most one master ever completes recovery of a given generation.
- Each generation's first version is above everything the previous generation committed.
- Never two processes that both believe they are the controller (a leader believes so only
  while its lease is valid by its own clock).
- While a candidate believes it leads, no majority could nominate anyone else: a majority
  of coordinators hold a live lease for it, are down, or are abstaining after a restart.

**By workloads:**

| Workload | Invariant |
| --- | --- |
| **bank** | Random transfers between 20 accounts. Audits (point reads or a cross-shard range read) always total 2,000 (serializability). |
| **cycle** | 16 keys form a permutation cycle mutated by 3-key swaps (FoundationDB's classic test). It stays one cycle. |
| **cas** | `getVersioned` + `compareAndSwap` increments, in one transaction and across two (the `versionMismatch` path), each writing `cas/log/<n>` atomically. The log is exactly `1...counter`. |
| **add** | Idempotent atomic adds. The total equals the acknowledged adds exactly (plus at most one per add that gave up with an unknown outcome). |
| **orders** | Idempotent transactions that generate a **new id per attempt**. Every acknowledged id exists, no id is returned twice, and the count equals the number of orders. |
| **history** | Writes, then re-reads earlier writes at their commit version (`read(atVersion:)`). Exact match; reads before a write don't see it. |
| **realtime** | One client increments a counter (read-modify-write); an independent client checks that transactions it starts afterwards reflect every acknowledged increment: read-only results, values a body acts on (an error thrown on seeing a stale value must never escape), and committed read-modify-writes. |
| **global** | No versionstamp is ever handed to two commits. |

**At the end**, after healing and quiescing: every invariant is re-checked by a fresh client;
all active replicas of each shard hold identical data at a common version; the running
generation's logs have released what storage popped (no unbounded retention); every live
coordinator names the actual controller; every
workload made progress; and the cluster has a controller and fully replicated shards.

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

**Performance III (November 2026):**

12. **Logs never released memory.** Garbage collection used the minimum popped version over
    *all* tags, including the metadata tag, which only advances when a shard-map snapshot
    commits (once per generation). Logs therefore kept every entry they ever received
    (hundreds of MB after minutes of load, against 1 GiB pod limits), and recovery copied
    all of it. Fix: release data by the storage tags' floor, keeping only unsuperseded
    shard-map metadata. The end-of-run retention check fails on the old code.
13. **A newly elected controller's lease could outlive the coordinators'.** A candidate
    counted nominations as a lease starting at election time, but a coordinator's
    nomination lease runs from when it was granted, possibly long before. Another
    candidate could be nominated by a majority while the first still believed it led
    (two controllers with valid leases). Fix: an elected candidate leads only once a
    majority acknowledges a heartbeat, its lease measured from that heartbeat's send; and
    it checks the lease before every event, so a leader resuming from a stall steps down
    before acting. The "no majority could nominate anyone else" check fails on 3 of 160
    seeds with the old logic and on none with the fix.
14. **A coordinator outside the electing majority pointed clients at a non-controller.**
    It kept nominating a candidate that never won (leaders only heartbeat, which it
    ignored), so roughly one client connection in nine failed with "controller
    unreachable". Found by the k3s test; now caught by an end-of-run check that every
    coordinator names the controller. Fix: a coordinator free to nominate prefers (and
    adopts) a candidate heartbeating as leader, and clients try the other coordinators.

Found outside the simulator in the same round: the server ignored SIGTERM (it runs as PID 1
in its container, where unhandled signals are ignored), so a deleted pod kept running every
role, and could even run a recovery, until SIGKILL at the end of its grace period. It now
exits on SIGTERM.

## Writing new workloads

A workload is an `async` function taking a `Database` built on
`sim.makeClientTransport()`. Use `sim.random` for every random choice, `sim.now` for time,
and `sim.fail(...)` to record a violation. Never use `Date()`, `Task.sleep`,
`UInt64.random` or unstructured `Task {}` in simulated code: they escape the deterministic
executor or the virtual clock.
