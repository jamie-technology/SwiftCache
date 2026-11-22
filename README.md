# SwiftCache

SwiftCache is an in-memory, sharded, replicated key-value store written in Swift, built on a
FoundationDB-style distributed transaction system. Think Redis, but with serializable
multi-key transactions, versionstamps, compare-and-swap on key versions, historical reads,
and exactly-once idempotent commits.

- **Serializable transactions** that retry automatically on read/write conflicts (optimistic
  concurrency, FoundationDB style).
- **Versionstamps.** Every committed write gets a 10-byte, totally ordered versionstamp, and
  every key records the versionstamp of the commit that last wrote it.
- **Compare-and-swap on key versions**, standalone or inside a multi-key transaction.
- **Historical reads.** Run a read-only transaction at any version inside the MVCC window.
- **Sharding.** 16,384 hash slots (Redis Cluster compatible hashing, `{hash tags}` supported).
  Adding storage pods adds shards, and the coordinator migrates slots onto them, so capacity
  grows with the cluster.
- **Replication.** Each shard has N in-memory replicas. Failed replicas are detected and
  re-replicated automatically.
- **Distributed, fenced transaction system.** A coordinator quorum elects a cluster
  controller and holds the transaction system's root state in a Paxos register. Separate
  master, commit-proxy, resolver and transaction-log roles run in *generations*; a failure
  produces a new generation, and the old one is locked out.
- **Idempotent transactions** (`idempotent: true`) are applied exactly once, even when a
  commit's outcome is unknown and it gets retried.
- **Deterministic simulation.** The server is written as message-driven state machines that run
  unchanged inside a seeded, single-threaded simulator with crashes, partitions and latency
  storms. The real Swift client library runs inside the simulation too.
- **Atomic adds, TTLs and range reads**, all transactional.

```swift
import SwiftCache

let db = try await Database.connect(cluster: "coord-0:4500,coord-1:4500,coord-2:4500")

// Optimistic update on a key's version.
let entry = try await db.getVersioned("users/123")!
var user = try entry.decode(User.self)
user.visits += 1
try await db.compareAndSwap(key: "users/123", expectedVersion: entry.versionstamp, newValue: user)

// Multi-key serializable transaction; retried automatically on conflict.
try await db.transaction { tr in
    let from = try await tr.get("acct/alice", as: Int.self) ?? 0
    let to = try await tr.get("acct/bob", as: Int.self) ?? 0
    try tr.set("acct/alice", from - 10)
    try tr.set("acct/bob", to + 10)
}

// Versions and CAS inside a transaction.
try await db.transaction { tr in
    let lock = try await tr.getVersioned("locks/job-7")
    try await tr.compareAndSwap(key: "locks/job-7", expectedVersion: lock?.versionstamp, newValue: "worker-3")
    try tr.set("jobs/7/state", "running")          // committed atomically with the CAS
}

// Read the past.
let v = try await db.readVersion()
// ... later (within the MVCC window) ...
let then = try await db.read(atVersion: v) { tr in try await tr.get("acct/alice", as: Int.self) }
```

## Quick start (local k3s)

```bash
./scripts/build-image.sh                        # builds swiftcache:<git-sha> into Docker (visible to colima/k3s)
kubectl apply -f deploy/k8s/swiftcache.yaml     # 3 coordinators, 3 logs, 4 storage = 2 shards × 2 replicas
kubectl -n swiftcache run cli --rm -it --image=swiftcache:latest --restart=Never \
  --env=SWIFTCACHE_CLUSTER="$(kubectl -n swiftcache get cm swiftcache-config -o jsonpath='{.data.CLUSTER}')" \
  --command -- swiftcache-cli status --stats

kubectl -n swiftcache scale statefulset swiftcache-storage --replicas=6   # add a shard

./scripts/k3s-test.sh                           # full end-to-end test: deploy, verify, chaos, scale-out, bench
```

## Development

```bash
swift build
swift test                                                   # unit tests + simulation seeds + determinism check
swift run -c release swiftcache-sim --count 500              # 500 seeds of chaos simulation
swift run -c release swiftcache-sim --scenario all --count 25   # targeted fault scenarios
swift run -c release swiftcache-sim --seed 57 --verbose      # replay one seed, with full logs
```

## Documentation

| Doc | Contents |
| --- | --- |
| [docs/architecture.md](docs/architecture.md) | Roles and fencing, the commit path, recovery, leader election, sharding |
| [docs/transactions.md](docs/transactions.md) | Transaction semantics, versionstamps, CAS, historical reads, conflicts, retries |
| [docs/client.md](docs/client.md) | Swift client library API reference and patterns |
| [docs/simulation.md](docs/simulation.md) | Deterministic simulation: how it works, workloads, reproducing failures |
| [docs/operations.md](docs/operations.md) | Running on k3s: deployment, scaling, failure behaviour, tuning, CLI |
| [docs/roadmap.md](docs/roadmap.md) | Known limitations and what's next (performance vs Redis) |

## Layout

```
Sources/
  SwiftCacheCore/     types, versionstamps, slots, binary wire protocol, ClientTransport protocol
  SwiftCacheServer/   roles as pure state machines (no I/O): CoordinatorRole, CandidateRole,
                      ClusterController, MasterRole, CommitProxy, ResolverRole, TLogRole,
                      StorageServer, WorkerAgent; plus VersionedStore, ConflictSet, ProcessHost
  SwiftCacheSim/      deterministic simulator, workloads, fault scenarios, invariant monitors
  SwiftCacheNet/      SwiftNIO runtime: framing, NodeHost (runs a process's roles on one event loop), FileDisk, client transport
  SwiftCache/         the client library (Database, Transaction)
  swiftcache-server/  server binary (--class stateless | log | storage [--coordinator])
  swiftcache-cli/     CLI: get/set/cas/range/status, verify (end-to-end suite), bench
  swiftcache-sim/     simulation runner
deploy/k8s/           k3s manifests
scripts/              build-image.sh, k3s-test.sh
```
