# Operations (k3s)

Production deployments target k3s. Everything here is tested against the local colima k3s
cluster by `scripts/k3s-test.sh`.

## Deploy

```bash
./scripts/build-image.sh                           # → swiftcache:<sha>, also tagged swiftcache:latest
kubectl apply -f deploy/k8s/swiftcache.yaml
```

What gets created (namespace `swiftcache`):

| Object | Purpose |
| --- | --- |
| `ConfigMap swiftcache-config` | `REPLICATION`, `MVCC_WINDOW_SECONDS`, `COORDINATOR` endpoint |
| `StatefulSet swiftcache-coordinator` (1 pod) + `Service` | The coordinator, at `swiftcache-coordinator.swiftcache.svc.cluster.local:7400` |
| `StatefulSet swiftcache-storage` (4 pods) + headless `Service` | Storage servers, each advertising `<pod>.swiftcache-storage.swiftcache.svc.cluster.local:7500` |

With `REPLICATION=2` and 4 storage pods you get 2 shards × 2 replicas. On multi-node clusters
the topology spread constraint keeps replicas on different nodes.

For a production image registry, push the image and replace `image:` (and set
`imagePullPolicy: Always` or pin digests).

## Scaling capacity

Each storage pod holds its shard's slice of the data in memory (the default limit is 1Gi per
pod). Total capacity ≈ `shards × per-pod memory`. To add a shard, add `REPLICATION` pods:

```bash
kubectl -n swiftcache scale statefulset swiftcache-storage --replicas=6
```

The new pods join the pool, form a shard, and the coordinator migrates slots in 2,048-slot steps
until slot counts are balanced. Reads and writes keep working during migration. Watch it with:

```bash
swiftcache-cli status --stats
```

Scale storage pods in multiples of `REPLICATION`. A leftover pod waits in the unassigned pool
and replaces the next failed replica. Scaling **down** is not yet supported (see roadmap).

## The CLI

The image contains `swiftcache-cli`. Run it as a one-off pod:

```bash
alias sc='kubectl -n swiftcache run cli-$RANDOM --rm -i --quiet --restart=Never --image=swiftcache:latest \
  --env=SWIFTCACHE_COORDINATOR=swiftcache-coordinator.swiftcache.svc.cluster.local:7400 \
  --command -- swiftcache-cli'

sc status --stats                     # shards, slot counts, replicas, per-server keys/memory
sc set greeting hello --ttl 60
sc get greeting                       # value + versionstamp
sc get greeting --at-version 1795341970010421
sc cas greeting 000660da9fde0c9f0000 "hi"   # expected versionstamp, or 'none'
sc incr page-views --by 1
sc range user/ --limit 20
sc read-version
sc wait-ready --shards 3              # block until N fully replicated, balanced shards
sc verify --min-shards 2              # end-to-end correctness suite
sc bench --seconds 10 --concurrency 32 --read-ratio 0.8
```

## Failure behaviour

- **Storage pod dies or is deleted.** Its shard keeps serving from the other replicas. After
  `--failure-timeout` (3s) the replica leaves the commit quorum. The restarted pod rejoins
  empty, copies a snapshot from a peer and becomes active again (`catching_up` → `active` in
  `status`).
- **Coordinator pod dies.** Writes pause; reads at known versions continue. The new pod
  recovers from storage reports in about `--recovery-window` (3s) and clients retry
  transparently.
- **All replicas of a shard die.** That shard's data is lost (in-memory). Commits touching it
  stall for up to `--shard-loss-timeout` (120s), in case the replicas are only unreachable, and
  then its slots are reassigned empty.
- **Node failure** on multi-node k3s. Spread constraints keep replicas apart, so a node loss
  costs one replica per shard.

## Tuning flags (`swiftcache-server … --help`)

| Flag | Default (k8s) | Notes |
| --- | --- | --- |
| `--replication` | 2 | Coordinator only. Replicas per shard. |
| `--mvcc-window` | 10s | How far back historical reads and long transactions work. Costs memory for overwritten keys. |
| `--failure-timeout` | 3s | Unacknowledged-batch time before a replica is dropped. |
| `--shard-loss-timeout` | 120s | Silence tolerated from a shard's *last* replica before its data is given up. |
| `--recovery-window` | 3s | Coordinator start-up wait for storage reports. |
| `--coordinator-timeout` | 2s | Storage-side: silence before re-joining the coordinator. |
| `--log-level` | info | |

## Observability

Logs go to stderr (`kubectl logs`). The coordinator logs membership changes, data movements,
failures and recoveries. `swiftcache-cli status --stats` shows versions, commit and conflict
counters, the topology and per-server stats. A metrics endpoint is on the roadmap.

## Running the end-to-end test

```bash
./scripts/k3s-test.sh            # builds the image, deploys a fresh namespace, then:
                                 # verify → delete storage pod → delete coordinator → verify
                                 # → scale 2→3 shards → verify → bench
IMAGE=swiftcache:abc123 ./scripts/k3s-test.sh     # reuse an existing image
```

**The test deletes and recreates the `swiftcache` namespace.**
