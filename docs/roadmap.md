# Roadmap and known limitations

## Milestone 1 (current)

A single coordinator, sharded and replicated in-memory storage, serializable transactions,
versionstamps, CAS, historical reads, deterministic simulation, a SwiftNIO runtime and k3s
deployment.

### Known limitations

- **The coordinator is a single process** that combines sequencer, resolver, commit proxy and
  cluster controller. It recovers from storage reports after a crash, but writes are unavailable
  until a replacement pod is up, and every commit passes through one event loop.
- **No durable log.** An acknowledged commit lives only in storage replicas' memory. Losing every
  replica of a shard loses that shard's data.
- **Unknown commit results** can double-apply non-idempotent transactions on retry.
- **No scale-down.** Removing shards (draining slots off a shard) isn't implemented.
- **No eviction.** Memory is bounded only by the pod limit. There is no LRU or max-memory policy.
- **No auth or TLS** on the wire protocol.
- **Clients need pod-network access** (they route directly to storage pods).
- **Range reads** scatter to every shard (hash partitioning).

## Next: distributed, role-split transaction system (FoundationDB-style)

Separate authority and failure domains first, and processes only where that adds resilience.

- **Coordinators (Paxos/quorum, 3 or 5).** Store only the cluster's generation and configuration,
  and elect a single **cluster controller**. They replace "the" coordinator as the source of
  truth for "who is in charge", so a partitioned controller is fenced by generation number.
- **Cluster controller.** Membership, failure detection (by quorum rather than one observer),
  and recruiting the roles below.
- **Sequencer.** Hands out commit versions and read versions. Small and fast.
- **Commit proxies (N).** Accept commits, batch them, get a version from the sequencer, send
  conflict ranges to resolvers and mutations to the log. Clients spread over proxies.
  - **Idempotency IDs.** Each commit carries a client-generated ID that is written into the log
    alongside its mutations. After `commitUnknownResult`, the client (or a proxy after recovery)
    can ask whether that ID committed, at what versionstamp, so retries never double-apply.
- **Resolvers (N, partitioned by key range or slot).** Conflict detection scales out.
- **Transaction logs (replicated).** Commits become durable once a quorum of log replicas has
  them. Storage servers *pull* from the logs by version, so a lagging or restarted replica just
  resumes from its version, with no coordinator-held resend buffers and no truncation-based
  recovery. Recovery then becomes: lock the old log generation, find the durable end, start a
  new generation.
- Simulation grows with it: log and coordinator faults, quorum partitions, clock skew.

## Then: performance

Target: **Redis-class single-key throughput and latency**, measured side by side in the same
k3s cluster:

- `redis-benchmark` against Redis 7 vs. an equivalent SwiftCache harness (`swiftcache-cli bench`
  extended with matching key and value sizes, pipelining and connection counts).
- Report GET/SET ops/s, p50/p99/p999 latency, plus transactional workloads (bank transfers vs
  Redis `WATCH`/`MULTI`).
- Likely work: multi-threaded storage servers (shard-per-core), read fast paths that skip the
  version wait when the requested version is already known committed, request pipelining,
  cheaper wire encoding (avoid `[UInt8]` copies, using `ByteBuffer` end to end), batching tuned
  for latency vs throughput, and GRV batching/caching.

## Later

- Scale-down / shard draining and an admin API to exclude servers.
- Optional persistence (snapshot + log replay) for warm restarts.
- Max-memory and eviction policies.
- Watches (notify on key change, as in FoundationDB watches).
- TLS and auth.
- Metrics (Prometheus) and tracing.
- A RESP (Redis protocol) gateway for drop-in compatibility.
