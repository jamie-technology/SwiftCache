# Performance

## Results

### Side by side with Redis (k3s)

`scripts/bench-vs-redis.sh` runs Redis 7 (persistence off) and SwiftCache in the same k3s
cluster (colima, 4 vCPU VM on a laptop). Both clients run as in-cluster pods with the same
concurrency, 100-byte values, 10k random keys and no pipelining: `redis-benchmark` for
Redis, `swiftcache-cli bench` for SwiftCache.

| | Redis 7, 32 clients | SwiftCache, 32 | Redis 7, 256 clients | SwiftCache, 256 |
| --- | --- | --- | --- | --- |
| get | 147k ops/s, p50 0.10 ms | 44k, p50 0.68 ms | 165k, p50 0.77 ms | **178k**, p50 1.25 ms |
| set | 69k ops/s | 22k, p50 1.30 ms | 151k, p50 0.79 ms | 55k, p50 3.9 ms |
| 2-key read-modify-write txn | — | 9k | — | 33k |

What is being compared: Redis is one process with no replication on the write path and no
cross-key isolation. SwiftCache here is 3 shards × 2 replicas and 3 transaction logs, and
every operation is strictly serializable:

- a `get` obtains a read version confirmed by the master and every log;
- a `set` is a replicated transaction on 3 logs, plus an idempotency record.

### Local cluster (no Docker networking)

`scripts/local-cluster.sh` (3 coordinators, 3 logs, 6 storage processes on one MacBook):

| | 32 clients | 128 | 512 |
| --- | --- | --- | --- |
| get | 53–76k ops/s | 201k | **387k** (p99 2.5 ms) |
| set | 44k | 91k | 117k (p99 10 ms) |
| 2-key txn | 20k | 37–40k | 73–86k |

Single-client latency: get 0.15 ms, set 0.22 ms, 2-key txn 0.5 ms.

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

Correctness bugs caught while optimizing (both by the simulator, both fixed before
keeping the change):

- **Seeded logs weren't indexed for the demand-driven peek.** A long-poll timeout could have
  told storage to skip data. Logs now always scan on timeout; the index only decides *when*
  to answer.
- A timed-out peek with no progress was re-held without a timer.

## Where the remaining cost is

Profiles show server time dominated by syscalls and message framing and decoding
(`FrameDecoder` ≈ 12–15%), then the resolver and proxy finalization. A `set` touches the
master twice, the resolver, all 3 logs, and 2 storage replicas (4 with its idempotency
record).

Next steps, roughly in order of payoff:

1. **Zero-copy decode:** parse messages directly from NIO buffers (no `[UInt8]` copies); shorter endpoint encoding in frames.
2. **Idempotency record co-location:** store the record with the transaction's first key (same shard), so a single-key `set` touches one shard instead of two.
3. **More, smaller processes:** storage servers per core, more proxies, and partitioned resolvers; scaling out is how a cluster goes from hundreds of thousands to millions.
4. **Pipelining the master's committed-version report** with the next batch.
5. **Optional bounded-staleness reads** (opt-in, documented as not strictly serializable) for workloads that want Redis-like read latency at low concurrency.
