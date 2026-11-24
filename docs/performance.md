# Performance

## Results

### Side by side with Redis (k3s)

`scripts/bench-vs-redis.sh` runs Redis 7 (persistence off) and SwiftCache in the same k3s
cluster (colima, 4 vCPU VM on a laptop). Both clients run as in-cluster pods with the same
concurrency, 100-byte values, 10k random keys and no pipelining: `redis-benchmark` for
Redis, `swiftcache-cli bench` for SwiftCache.

| | Redis 7, 32 clients | SwiftCache, 32 | Redis 7, 256 clients | SwiftCache, 256 |
| --- | --- | --- | --- | --- |
| get | 172k ops/s, p50 0.10 ms | 120k, p50 0.26 ms | 163k, p50 0.78 ms | **384k**, p50 0.57 ms |
| set | 163k ops/s, p50 0.10 ms | 54k, p50 0.56 ms | 159k, p50 0.78 ms | 122k, p50 1.68 ms |
| 2-key read-modify-write txn | — | 22.6k, p50 1.31 ms | — | 61k, p50 3.6 ms, p99.9 27 ms |

One client: Redis 36k gets/s and 48k sets/s (p50 0.015 ms) in this run (25–38k and
22–86k in others: Redis's single-client numbers swing widely on this VM); SwiftCache 13k
gets/s (p50 0.07 ms), 4.3k sets/s (p50 0.22 ms), 1.9k transactions/s (p50 0.51 ms).

Streaming delivery, A/B on the same quiet machine, back to back (`main` 3237be7 → streaming):
set 4.3k → 4.7k (1 client), 36k → 44k (32), 107k → 111k (256); txn 18.5k → 19.8k (32),
52k → 55k (256), p99.9 44 → 31 ms; gets unchanged within noise (296k vs 290k at 256; an
earlier run of `main` measured 322k).

Previous round (same VM, November 2026, before Performance III): get 85k / 197k, set
24k / 67k, txn 10.6k / 26k (p99.9 1.1 s) at 32 / 256 clients. Redis measured 175k / 141k
gets and 155k / 144k sets then; Redis varies by about ±10% between runs on this VM.

Configuration: one commit proxy (on the master's process, the shipped default) and one
client I/O thread with benchmark tasks running on it (`--threads 1`, the script's default).
With two proxies and four client threads (the previous settings) the same build measured
get 98k / 245k, set 24k / 84k, txn 11.4k / 34k.

What is being compared: Redis is one process with no replication on the write path and no
cross-key isolation. SwiftCache here is 3 shards × 2 replicas and 3 transaction logs, and
every operation is strictly serializable:

- a `get` is one round trip: a leased read version from the master in parallel with a
  replica's latest committed value;
- a `set` is a replicated transaction on 3 logs (its idempotency entry rides in one record
  per commit batch).

**The 256-client transaction regression (26k/s, p99.9 1.1 s) is gone** (49k/s, p99.9
36 ms). It was not conflict-retry chains: `bench --mode txn` now prints attempts, backoff
and per-phase time for all transactions, the slowest 1% and the slowest 0.1%, and the
slowest 0.1% averaged 1.08 attempts and ~0 ms of backoff (locally), with their time in
reads and commits. At 256 clients in k3s they now average 1.15 attempts, 0.03 ms of
backoff and 54 ms in total. The exact source of the old build's 1.1 s tail in k3s wasn't
isolated; candidates fixed since include logs never releasing memory (OOM risk at the
1 GiB pod limit) and a terminating pod's roles running for its whole grace period.

### Local cluster (no Docker networking)

`scripts/local-cluster.sh` (3 coordinators, 3 logs, 6 storage processes on one MacBook):

| | 1 client | 32 clients | 128 | 512 |
| --- | --- | --- | --- | --- |
| get | 23k, p50 0.04 ms | 152k | 288k | **440k** (p99 2.7 ms) |
| set | 8.4k, p50 0.12 ms | 86k | 152k | 162k (p99 7.3 ms) |
| 2-key txn | 3.5k, p50 0.28 ms | 34.5k | 71k | 99k (p99 11.9 ms) |

One proxy, one client I/O thread. Previous round: get 114–125k / 209k / 427k, set
43–58k / 97–101k / 159k, txn 20–22k / 45–53k / 92–97k at 32 / 128 / 512 clients; single
client get 0.05 ms, set 0.17–0.21 ms, txn 0.42 ms.

Throughput rises with concurrency because the system batches at every stage. Redis's
single thread doesn't get that benefit.

## Methodology

- **Throughput and latency:** `swiftcache-cli bench --mode get|set|mixed|txn|grv|read-at`.
  It reports ops/s, p50, p99 and p99.9. `grv` (read versions only) and `read-at` (reads at a
  fixed version) isolate the two halves of a `get`.
- **Transactions:** `--mode txn` also reports attempts per transaction and where the time
  went (backoff, read version, reads, commit), for all, the slowest 1% and slowest 0.1%.
- **Where commits spend time:** each proxy logs, every 5 s, its batches, transactions per
  batch, and per-batch averages/maxima of time queued at the proxy, time to resolution and
  time until the logs and master declare the batch committed. `bench` prints the client
  side: transactions per commit batch, time queued in the client and batch round trip.
- **Client threads:** benchmark tasks run on the client's I/O threads (`--executor loop`,
  the default; `global` runs them on Swift's default executor). `--threads` sets how many.
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

### Performance III (November 2026)

Measured locally at 32 clients unless noted (`scripts/cpu-per-op.sh`, per-process CPU).

| Change | Why it helped | Effect |
| --- | --- | --- |
| Callback-driven client batching | Coalescers sent the next batch only after waking a queued task to drive it, and every get spawned an `async let` child task; now the next batch goes out from the previous one's completion and a get awaits two callbacks | get 123k → 173k ops/s; client 25.9 → 18.3 µs/op |
| Client lanes: per-event-loop connections, batching and task executors | Each reply crossed threads twice (NIO thread → Swift's pool); tasks can now run on an I/O thread whose own connections serve them, end to end on one thread | client 18.3 → 6.7 µs/get with one lane |
| Byte keys hashed as buffers (`ByteKey`) | `[UInt8]`'s `Hashable` feeds the hasher one byte at a time through a protocol witness; it dominated the resolver (two-thirds of the master process in one profile) | less CPU in resolver, storage and client |
| No empty "committed advanced" batches | Logs already learn the committed version from the master; the proxy still ran an extra empty batch (≈25 messages) after every data batch | proxy batches halved at low load |
| First commit proxy on the master's process | Version/resolution and committed-version notices are local, so a commit through it is client → proxy → logs → master's process → client | set 62k → 79k; 1-client set p50 0.17 → 0.11 ms; with one proxy (now the default) server CPU 83 → 65 µs/set |
| One-way pushes | Logs acknowledged every push to the proxy, which never used the reply (completion comes from the master); now they answer only to refuse, and unacknowledged batches are re-pushed | −3 messages per batch |
| One idempotency record per commit batch | Every idempotent op stored its own record for 10 minutes (tens of millions of keys at sustained write rates); now one record lists a batch's operations | storage writes per set halved; idempotency memory ÷ ~10 |
| `tlogNeed` instead of superseding peeks | A read arriving just before its version's data cancelled the storage server's peek and started another (3 messages); now it raises the held peek's need (1 message) | txn 31k → 35k ops/s, p99 2.35 → 1.31 ms |
| Streaming log → storage delivery | A peek is a subscription: the log keeps sending replies (with the version each continues from) instead of each reply being followed by a new peek; storage applies only gap-free continuations | server CPU per set 46 → 36 µs (logs −23%, storage −39%), per txn −13% |
| Logs release popped data | GC was held back by the metadata tag (advances once per generation), so logs kept everything | bounded log memory (was 0.5–0.9 GB after minutes) |

In k3s, these took 32-client get/set/txn from 85k/24k/10.6k to 115k/40k/17.4k ops/s and
256-client from 197k/67k/26k to 322k/112k/49k.

### Performance V (November 2026)

Profiles (`xctrace` Time Profiler on the local cluster, 32-client sets) showed both the
master/proxy process and each log spending 30–42% of their CPU in `write`/`read`/`kevent`
syscalls; frame encoding and decoding was ~10%. Syscalls scale with messages, and messages
with commit batches: locally the proxy was running ~2,400 batches/s of only 2–4
transactions, because it started a new batch as soon as the previous one was resolved
(~50 µs) while logs and master took ~200 µs, keeping ~4 tiny batches in flight.

| Change | Why it helped | Effect |
| --- | --- | --- |
| One commit batch in flight per proxy (`maxInflightBatches` 8 → 1) | While logs and master work on a batch, arrivals accumulate into the next, so messages and syscalls per transaction drop. A scheduling change only: batch contents and ordering are unchanged | local, 32 clients: server CPU per set 51 → 35 µs, sets +12%, txns +14% (CPU per txn 137 → 109 µs); 256 clients: sets 170k → 199k/s; k3s: neutral (batches there were already 20–70 transactions, since each hop takes 200–400 µs) |
| One commit batch and one read batch in flight per client lane (was 4 each) | Same effect on the client side: requests accumulate while a batch is out, so batches are larger and messages fewer. With the proxy holding one batch, extra client slots only queued at the proxy | k3s, 32 clients: sets 45.9k → 52.6k/s (+15%, p50 0.65 → 0.57 ms; commits per batch 5 → 16, batch round trip 483 → 302 µs); 256 clients: gets 276k → 303–314k (+12%), txns 57k → 61k; single-client unchanged (one request is never held back); mixed 80% reads at 32 clients: 51k → 68k ops/s, p50 0.56 → 0.42 ms |
| Per-connection endpoint interning | Frames named `from`/`to` with 30–60-byte strings, decoded into new heap strings each time; now each endpoint is defined once per connection and referenced by a 2-byte number | ~70 fewer bytes per frame. No measurable CPU or throughput change (locally or in k3s); kept for multi-node networks, where bytes per message matter at 100k+ ops/s |

| One suspension per get | A get awaited its read version and its replica answer separately (two task suspensions and resumptions, and a generic `ResultBox` instantiated per call); now a non-generic join resumes the caller once, when both answers are in | 32 concurrent gets on one client thread: 131k → 160k/s, client CPU 7.5 → 6.1 µs/get (in k3s the single-threaded benchmark client was the get bottleneck at 986m) |
| Resolver keyed by 64-bit key hashes in a flat table | Every idempotent operation adds a unique conflict key (its idempotency record) for the 10 s window: ~1.4M heap-allocated byte keys, SipHashed on insert, lookup and expiry. Now a seeded 64-bit hash in an open-addressing table (16 bytes/entry). A collision can only cause a spurious conflict (abort and retry), never a missed one: an entry holds at least the newest write of every key with its hash, and expires only with the write that set it. Range checks still use exact keys | resolver 29% → 11% of the master/proxy process at 256 clients; master/proxy memory 255 → 207 MB; sets at 256 clients 144k → 155k/s |
| One encoding per push | A batch's push is identical for every log; it is encoded once and the bytes shared by each log's frame | ~6% of master/proxy CPU at 256 clients |

| Client replies flushed first; byte-level endpoint parsing | A `perf` profile of the master in k3s (Linux) showed ~40% of its CPU inside `write`: on the bridged pod network the receiver's TCP path, netfilter included, runs in the sender's syscall (~9 µs per flush). The end-of-pass flush went through connections in dictionary order, so a commit's client reply often waited behind the master's three log notices. Client connections are now flushed first. Endpoint parsing (`processAddress`, the `client:` test) used `Character` search, running grapheme breaking on every message (4.5% of the master); it now searches bytes | k3s, back to back (2 rounds): 1-client set 4.9k → 5.3k/s (p50 0.20 → 0.17 ms), 32-client gets 132k → 146k, 32-client sets 58k → 61k |

Measured, then: in-flight cap 1 vs 2 vs 4 vs 8 (32 clients, local): sets 66.9k / 67.7k /
62.1k / 59.5k, server µs per set 34.7 / 44.3 / 50.2 / 51.4.

Benchmarking hazard: a transaction run started right after heavy set runs measured about
half its steady rate (36–40k vs 72k at 256 clients, locally) while storage caught up. Run
each mode on a freshly started cluster, or let it settle, before comparing.

### What's next

- **Writes at low and medium concurrency** are latency-bound: a set is
  client → proxy → 3 logs → master → client, each hop an event-loop wakeup and syscalls on
  a shared 4-vCPU VM. Streaming log → storage delivery is done (+23% sets at 32
  clients). In k3s, commit batches are already 20–70
  transactions and per-message CPU isn't the limit (Performance V): a 32-client set is
  latency-bound by Little's law (32 / 0.66 ms ≈ 48k/s), about 150 µs per network hop on
  this VM. The remaining lever is fewer or cheaper hops on the write path.
- **Tried and rejected (measured locally, Performance III):**
  - *Overlapping a transaction's read version with its first read* (adopting a replica's
    latest version). Without waiting for the read version: +30% transactions/s with one
    client, but −60% at 256 clients (an older read version widens the conflict window), and
    a transaction body could observe a snapshot older than commits acknowledged before it
    started (caught by the simulator's cycle invariant). Waiting for the read version
    first (strict): slower at every concurrency, since the replica was behind it about
    half the time.
  - *More read-version requests in flight per client lane* (1 → 2 or 4): no measurable
    change.
  - *More commit batches in flight per client lane* (4 → 8 or 16), to trade the spare
    CPU below for latency: slightly worse in k3s (32-client sets 38k → 34–36k/s,
    transactions 17–18k → 16–17k/s, two runs each) and locally. Fewer turned out better
    (Performance V: 4 → 1, +15% sets).
- **32-client writes in k3s are latency-bound, not CPU-bound**: during a 32-client set run
  the node used 2.2 of 4 cores and no pod was saturated (master+proxy 0.47 core, each log
  0.26). A set's p50 is 0.22 ms with one client but 0.78 ms with 32, so most of it is
  queueing in the single-threaded master/proxy and log event loops, not network hops.
  Measured split of a 32-client set (p50 0.79 ms): 162 µs waiting in the client for one of
  its 4 commit-batch slots, and a 581 µs batch round trip, of which 365 µs inside the
  proxy, logs and master (logs → master 301 µs, vs 110 µs with one client). Flushing
  commit-path messages immediately instead of at the end of each event-loop pass made it
  worse (sets 37k → 29k/s): on this VM a syscall or packet costs more than the wait, so
  the lever is fewer messages, hence streaming storage delivery.
- **Single-client latency** (get 0.07 ms vs Redis 0.015 ms) is dominated by fixed per-hop
  costs in the server event loops and the client.
