# Performance

## Results

### Side by side with Redis (k3s)

`scripts/bench-vs-redis.sh` runs Redis 7 (persistence off) and SwiftCache in the same k3s
cluster (colima, 4 vCPU VM on a laptop). Both clients run as in-cluster pods with the same
concurrency, 100-byte values, 10k random keys and no pipelining: `redis-benchmark` for
Redis, `swiftcache-cli bench` for SwiftCache.

| | Redis 7, 32 clients | SwiftCache, 32 | Redis 7, 256 clients | SwiftCache, 256 |
| --- | --- | --- | --- | --- |
| get | 175k ops/s, p50 0.10 ms | 85k, p50 0.34 ms | 141k, p50 0.84 ms | **197k**, p50 1.10 ms |
| set | 155k ops/s, p50 0.10 ms | 24k, p50 1.19 ms | 144k, p50 0.83 ms | 67k, p50 3.2 ms |
| 2-key read-modify-write txn | — | 10.6k, p50 2.8 ms | — | 26k, p50 6.8 ms (p99.9 1.1 s, under investigation) |

What is being compared: Redis is one process with no replication on the write path and no
cross-key isolation. SwiftCache here is 3 shards × 2 replicas and 3 transaction logs, and
every operation is strictly serializable:

- a `get` is one round trip: a leased read version from the master in parallel with a
  replica's latest committed value;
- a `set` is a replicated transaction on 3 logs, plus an idempotency record.

Open issue: the 2-key transaction benchmark at 256 clients regressed from the previous round
(33k → 26k), with a 1.1 s p99.9. No recovery or failure happened during the run; the
suspected cause is long conflict-retry chains with exponential backoff.

### Local cluster (no Docker networking)

`scripts/local-cluster.sh` (3 coordinators, 3 logs, 6 storage processes on one MacBook):

| | 32 clients | 128 | 512 |
| --- | --- | --- | --- |
| get | 114–125k ops/s | 209k | **427k** (p99 2.3 ms) |
| set | 43–58k | 97–101k | 159k (p99 7.5 ms) |
| 2-key txn | 20–22k | 45–53k | 92–97k (p99 13.6 ms) |

Single-client latency: get 0.05 ms, set 0.17–0.21 ms, 2-key txn 0.42 ms.

Throughput rises with concurrency because the system batches at every stage. Redis's
single thread doesn't get that benefit.

## Methodology

- **Throughput and latency:** `swiftcache-cli bench --mode get|set|mixed|txn|grv|read-at`.
  It reports ops/s, p50, p99 and p99.9. `grv` (read versions only) and `read-at` (reads at a
  fixed version) isolate the two halves of a `get`.
- **CPU per operation:** `scripts/cpu-per-op.sh` divides server CPU (all processes) and
  client CPU by completed operations. It's much less sensitive to background load than
  throughput, so use it to judge optimizations on a shared machine.
- Every optimization was re-validated by the deterministic simulator (random chaos plus
  targeted scenarios) and the k3s end-to-end test before being kept.

## What made it faster

Starting point: 4.6k gets/s and 11.8k sets/s at 32 clients, with gets slower than sets.

| Change | Why it helped | Effect |
| --- | --- | --- |
| Proxies tell logs about newly committed versions immediately (`tlogCommitted`) | Storage previously learned a version was readable only from the next push, up to a 10 ms idle tick, so reads waited | get 4.6k → 23k |
| Read-version rounds start on arrival rather than on the 1 ms tick | Removes up to 1 ms of queueing | small |
| Write-only transactions reuse a recent version instead of fetching one | A transaction that read nothing has no read conflicts; an older version is only more conservative for its idempotency key | set +15% |
| Coalesced socket writes (one flush per event-loop pass per connection) | The system was syscall-bound (~7 µs CPU per message) | fewer syscalls |
| Client: per-connection locks, atomic request ids, cached channel, timeout sweep instead of a timer per request | The benchmark client itself was the bottleneck (200% CPU) | client CPU −40% |
| Server roles: one timeout sweep per role instead of a timer per request | Timer-heap churn showed up in profiles | |
| Demand-driven storage peeks | Every committed version used to wake every storage server. Now a log answers only when the tag has data, a waiting read needs a newer version, or after 200 ms | server CPU per get −20% |
| Peeks spread across logs | All storage servers used to pull from the first log | |
| Lighter read-only `get` path | No transaction object or commit machinery for a single read | get +25% |
| **Client-side read-version coalescing** | Concurrent operations in one process share one read version (each still requested after its caller started, so strictness holds) | read-version server CPU 22 → 4 µs/op; gets ×2–5 at high concurrency |
| **Batched storage reads** (`readBatch`) | Concurrent reads to one storage server share a message | |
| **Batched commits** (`commitBatch`) | Concurrent commits to one proxy share a message; each is still resolved and stamped individually | sets ×1.6, txns ×2+ at high concurrency |
| Incremental resolver GC | The resolver swept its whole per-key map once a second, stalling commits | set p99.9 at 128 clients 66 ms → 6 ms |
| Logs report durability straight to the master | The master's committed version is the minimum durable version across logs; proxies reply when the master says so (no proxy → master report round trip) | fewer hops per commit |
| Version + resolution in one round trip | The master allocates the version and forwards the batch to a resolver on its own process, which replies to the proxy | −1 network round trip per commit |
| Read versions from the master under log leases | No per-request log confirmation; one round trip; fenced by recovery locks that wait out the lease | read-version server CPU 4.4 → 0.7 µs/op |
| One-round-trip single-key gets | Read version and a replica's latest committed value in parallel; used if the replica is at least as new | get p50 0.09 → 0.05 ms |
| Continuous commit pipelining | A new batch starts when the previous one is resolved (not on a 1 ms timer) | set p50 and p99 down, batches 8 → 50–120 txns |
| Multiple in-flight batches per client coalescer | Commits/reads no longer wait for the previous batch's full round trip | set at 512 clients 114k → 159k |
| Immediate first retry after a conflict, 0.5 ms initial backoff | The conflicting writer already committed; a fresh read version usually succeeds | txn p99 at 512 clients 21 → 14 ms |
| In-place frame decoding, cheap client RNG, one topology lookup per get | Fewer copies, syscalls and lock round trips | client CPU down |
