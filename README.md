# SwiftCache

In-memory, Redis-style key-value store with strictly serializable multi-key ACID transactions
on a FoundationDB-style distributed transaction system: MVCC snapshot reads, conflict
detection, versionstamps, CAS and exactly-once commits. Sharded and replicated, consistent
through failures and partitions, tested by deterministic simulation.

```swift
import SwiftCache

let db = try await Database.connect(cluster: "coord-0:4500,coord-1:4500,coord-2:4500")

// Optimistic update on a key's version.
let entry = try await db.getVersioned("users/123")!
var user = try entry.decode(User.self)
user.visits += 1
try await db.compareAndSwap(key: "users/123", expectedVersion: entry.versionstamp, newValue: user)

// Multi-key, strictly serializable transaction; retried automatically on conflict.
try await db.transaction { tr in
    let from = try await tr.get("acct/alice", as: Int.self) ?? 0
    let to = try await tr.get("acct/bob", as: Int.self) ?? 0
    try tr.set("acct/alice", from - 10)
    try tr.set("acct/bob", to + 10)
}

// Versions and CAS inside a transaction: all or nothing.
try await db.transaction { tr in
    let lock = try await tr.getVersioned("locks/job-7")
    try await tr.compareAndSwap(key: "locks/job-7", expectedVersion: lock?.versionstamp, newValue: "worker-3")
    try tr.set("jobs/7/state", "running")
}

// Exactly once, even if the commit's outcome is unknown and it is retried.
let orderID = try await db.transaction(idempotent: true) { tr -> String in
    let id = UUID().uuidString
    try tr.set("orders/\(id)", order)
    tr.add("orders/count", 1)
    return id
}

// Read the past (within the MVCC window).
let v = try await db.readVersion()
let then = try await db.read(atVersion: v) { tr in try await tr.get("acct/alice", as: Int.self) }
```

## Features

- **Strictly serializable transactions.** Optimistic concurrency with read/write conflict
  detection on keys and ranges. Every read in a transaction sees one MVCC snapshot. A
  transaction sees every commit acknowledged before it began.
- **Versionstamps.** Every commit gets a 10-byte, totally ordered versionstamp, recorded on
  each key it writes.
- **Compare-and-swap on key versions**, standalone or composed with other writes in a
  transaction.
- **Exactly-once commits.** `idempotent: true` records an operation id atomically with the
  commit, so a retry after an unknown result never applies it twice. The single-key
  conveniences are always idempotent.
- **Historical reads** at any version within the MVCC window, plus `readVersion()` as a
  consistency token.
- **Atomic adds, TTLs, range reads** and Redis Cluster–compatible hash slots (`{hash tags}`
  co-locate keys).
- **Sharded and replicated.** Add storage pods to add shards. Slots migrate live, failed
  replicas are re-replicated, and rolling restarts are gated on re-replication.
- **FoundationDB-style distributed transaction system.** Coordinators (Paxos register,
  durable) elect a cluster controller. A master, commit proxies, a resolver and replicated
  transaction logs run in fenced *generations*. Any failure recovers into a new generation;
  the old one is locked out.
- **Deterministic simulation.** Every role is a state machine that runs unchanged in a
  seeded, single-threaded simulator (with the real client library) under crashes,
  partitions, quorum loss and latency storms, checking invariants continuously.

**Durability model.** Data lives in memory, replicated: writes are on every transaction log
before they're acknowledged, and on every replica of their shard shortly after. Losing
*every* replica of a shard, or every log of a generation at once, loses data, like Redis
without persistence. Coordinator state is fsynced to disk.

## Performance

Same k3s cluster (colima, 4 vCPU laptop VM), in-cluster clients, 100-byte values, 10k keys,
no pipelining. Redis is a single unreplicated process. SwiftCache is 3 shards × 2 replicas,
3 transaction logs, and every operation is strictly serializable.

| | Redis 7, 32 clients | SwiftCache, 32 | Redis 7, 256 clients | SwiftCache, 256 |
| --- | --- | --- | --- | --- |
| GET / `get` | 172k ops/s | 120k ops/s | 163k ops/s | **384k ops/s** |
| SET / `set` (replicated, idempotent) | 163k ops/s | 54k ops/s | 159k ops/s | 122k ops/s |
| 2-key read-modify-write transaction | — | 23k ops/s | — | 61k ops/s (p99.9 27 ms) |

SwiftCache beats Redis on reads at high concurrency (2.4× at 256 clients); Redis's single
unreplicated process still wins on writes, and on everything at low concurrency (one
client: Redis 25–38k gets/s and 22–86k sets/s depending on the run, SwiftCache 13k and
4.3k, p50 0.07 ms and 0.22 ms).
On the same laptop without Docker networking: 440k gets/s at 512 clients, 152k sets/s and
71k two-key transactions/s at 128. See [docs/performance.md](docs/performance.md) for
methodology, the optimizations made, and what's next.

## Quick start (k3s)

```bash
./scripts/build-image.sh                        # builds swiftcache:<git-sha> (visible to colima/k3s)
kubectl apply -f deploy/k8s/swiftcache.yaml     # 3 coordinators, 3 logs, 4 storage = 2 shards × 2 replicas
kubectl -n swiftcache run cli --rm -it --image=swiftcache:latest --restart=Never \
  --env=SWIFTCACHE_CLUSTER="$(kubectl -n swiftcache get cm swiftcache-config -o jsonpath='{.data.CLUSTER}')" \
  --command -- swiftcache-cli status --stats

kubectl -n swiftcache scale statefulset swiftcache-storage --replicas=6   # add a shard

./scripts/k3s-test.sh          # end-to-end: deploy, verify, chaos, all-coordinator restart, scale-out, rolling restart
./scripts/bench-vs-redis.sh    # side-by-side with Redis 7 in the same cluster
```

## Development

```bash
swift build
swift test                                                       # unit tests, simulation seeds, scenarios, determinism
                                                                 # (needs Xcode's toolchain for swift-testing macros:
                                                                 #  DEVELOPER_DIR=/Applications/Xcode.app/Contents/Developer swift test)
./scripts/sweep.sh                                               # full validation: 330 chaos seeds + every scenario
swift run -c release swiftcache-sim --count 500                  # random chaos, 500 seeds
swift run -c release swiftcache-sim --scenario all --count 25    # targeted fault scenarios
swift run -c release swiftcache-sim --seed 57 --verbose          # replay one seed with full logs
./scripts/local-cluster.sh && ./scripts/cpu-per-op.sh            # local cluster + CPU-per-operation benchmark
```

## Documentation

| Doc | Contents |
| --- | --- |
| [docs/architecture.md](docs/architecture.md) | Roles and fencing, the commit path, recovery, leader election, sharding |
| [docs/transactions.md](docs/transactions.md) | Transaction semantics, versionstamps, CAS, historical reads, idempotency, retries |
| [docs/client.md](docs/client.md) | Swift client library API and patterns |
| [docs/simulation.md](docs/simulation.md) | Deterministic simulation, fault scenarios, invariants, bugs found |
| [docs/operations.md](docs/operations.md) | Running on k3s: deployment, scaling, failure behaviour, flags, CLI |
| [docs/performance.md](docs/performance.md) | Benchmarks vs Redis, methodology, optimizations |
| [docs/roadmap.md](docs/roadmap.md) | Known limitations and what's next |

## Layout

```
Sources/
  SwiftCacheCore/     types, versionstamps, slots, shard map, binary wire protocol
  SwiftCacheServer/   roles as pure state machines (no I/O): CoordinatorRole, CandidateRole,
                      ClusterController, MasterRole, CommitProxy, ResolverRole, TLogRole,
                      StorageServer, WorkerAgent; VersionedStore, ConflictSet, ProcessHost
  SwiftCacheSim/      deterministic simulator, workloads, fault scenarios, invariant monitors
  SwiftCacheNet/      SwiftNIO runtime: framing, NodeHost, FileDisk, health probes, client transport
  SwiftCache/         the client library (Database, Transaction, request coalescing)
  swiftcache-server/  server binary (--class stateless | log | storage [--coordinator])
  swiftcache-cli/     CLI: get/set/cas/range/status, verify (end-to-end suite), bench
  swiftcache-sim/     simulation runner
deploy/k8s/           k3s manifests
scripts/              build-image, k3s-test, bench-vs-redis, local-cluster, cpu-per-op, redeploy
```

## License

MIT. See [LICENSE](LICENSE).
