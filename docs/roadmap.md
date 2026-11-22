# Roadmap and known limitations

## Done

- **Milestone 1:** sharded, replicated in-memory storage; serializable transactions;
  versionstamps; CAS; historical reads; deterministic simulation; SwiftNIO runtime; k3s.
- **Milestone 2:** FoundationDB-style distributed transaction system: coordinator quorum
  with leader election and a Paxos generation register; elected cluster controller; master
  (sequencer); commit proxies; resolver; replicated transaction logs that storage servers
  pull from; generation-based recovery with fencing; data distribution through metadata
  commits; exactly-once idempotent transactions; targeted fault scenarios.

## Next: performance (vs Redis)

Target: Redis-class single-key throughput and latency, measured side by side in the same
k3s cluster (`redis-benchmark` against Redis 7 vs. an equivalent SwiftCache harness with
matching key and value sizes and connection counts). Report GET/SET ops/s and
p50/p99/p999, plus transactional workloads (bank transfers vs Redis `WATCH`/`MULTI`).

Current laptop baseline (k3s on colima, 4 vCPU): ~7k ops/s at 80% reads, p50 4 ms, p99 12 ms.
Every single-key operation is currently a full transaction. Likely work, roughly in order
of payoff:

- **Read versions:** batch GRV requests per proxy and confirm generation liveness once per
  batch (FoundationDB does this); let clients reuse a recent read version for read-only
  single-key gets (bounded staleness, opt-in) or use causal read-your-writes tokens.
- **Commit path:** pipeline the master's committed-version acknowledgement instead of
  waiting per batch; tune batching for latency versus throughput.
- **Zero-copy encoding:** `ByteBuffer` end to end instead of `[UInt8]` copies.
- **Storage:** multiple storage servers per process (one per core); a per-tag index in the
  logs (peek currently scans entries).
- **Clients:** connection pooling and request pipelining.

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
