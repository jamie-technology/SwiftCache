# Roadmap and known limitations

## Done

- **Milestone 1:** sharded, replicated in-memory storage; serializable transactions;
  versionstamps; CAS; historical reads; deterministic simulation; SwiftNIO runtime; k3s.
- **Milestone 2:** FoundationDB-style distributed transaction system: coordinator quorum
  with leader election and a Paxos generation register; elected cluster controller; master
  (sequencer); commit proxies; resolver; replicated transaction logs that storage servers
  pull from; generation-based recovery with fencing; data distribution through metadata
  commits; exactly-once idempotent transactions; targeted fault scenarios.
- **Milestone 3:** performance: request coalescing (read versions, reads, commits),
  demand-driven storage peeks, a lighter read path, incremental resolver GC, a leaner
  network layer; rollout-safe readiness probes.

## Performance

Benchmarks against Redis, what was optimized, and what's next are in
[performance.md](performance.md). Strictly serializable reads now match Redis at high
concurrency in k3s. Replicated writes are about 2.7× behind.

## Known limitations

- **In-memory durability.** Logs and storage keep data in memory, replicated. Losing every
  log of a generation at once loses commits not yet applied by storage. Losing every replica
  of a shard loses that shard. Coordinators *are* durable (fsync).
- **Static coordinator set.** Changing coordinators requires redeploying with a new cluster
  file (FoundationDB supports online coordinator changes).
- **One resolver.** The resolver role is a single instance per generation (multiple proxies
  are supported). Partitioning resolvers by key range is a later step.
- **Full replication of logs.** Every log stores every mutation. FoundationDB assigns tags to
  a subset of logs.
- **No scale-down / exclusion** of storage servers.
- **No eviction / max-memory policy.**
- **No TLS or authentication** on the wire.
- **Clients need pod-network access**, since they route directly to proxies and storage pods.
- **Range reads scatter to every shard** (hash partitioning).
- **Clock assumptions.** Leases assume clocks advance at similar rates (no huge skew in
  *rate*; absolute skew doesn't matter). Safety never depends on this: fencing via the
  generation register does.

## Later

- Optional persistence (log and storage on disk) for warm restarts.
- Watches, eviction policies, metrics (Prometheus), tracing.
- A RESP (Redis protocol) gateway for drop-in compatibility.
