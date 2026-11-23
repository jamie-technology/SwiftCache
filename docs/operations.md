# Operations (k3s)

Production deployments target k3s. Everything here is exercised against the local colima
k3s cluster by `scripts/k3s-test.sh`.

## Deploy

```bash
./scripts/build-image.sh                           # → swiftcache:<sha>, also tagged swiftcache:latest
kubectl apply -f deploy/k8s/swiftcache.yaml
```

What gets created (namespace `swiftcache`):

| Object | Purpose |
| --- | --- |
| `ConfigMap swiftcache-config` | `CLUSTER` (the coordinator list; every process and client uses it), `REPLICATION`, `LOGS`, `PROXIES`, `MVCC_WINDOW_SECONDS` |
| headless `Service swiftcache` | A stable DNS name per pod: `<pod>.swiftcache.swiftcache.svc.cluster.local:4500` |
| `StatefulSet swiftcache-coordinator` (3) + PVCs | Coordinators (durable state on a 64 Mi volume) that also host stateless roles: controller, master, commit proxies, resolver |
| `StatefulSet swiftcache-log` (3) | Transaction logs |
| `StatefulSet swiftcache-storage` (4) | Storage servers: 2 shards × 2 replicas |

Use `kubectl -n swiftcache logs` on a coordinator pod to follow elections and recoveries,
and `swiftcache-cli status` (below) to see which pod currently hosts each role.

**Coordinator membership is static:** the `CLUSTER` list is fixed at deploy time (as in
FoundationDB's cluster file). Keep coordinator pods on separate nodes in multi-node
clusters. The spread constraints try to.

## Scaling capacity

Each storage pod holds its shard's slice of data in memory (1 Gi limit per pod by default).
Total capacity ≈ `shards × per-pod memory`. To add a shard, add `REPLICATION` storage pods:

```bash
kubectl -n swiftcache scale statefulset swiftcache-storage --replicas=6
```

The new pods register, the controller forms a shard from them, and slots migrate in
2,048-slot steps until balanced. Reads and writes continue throughout. A leftover pod (not a
multiple of `REPLICATION`) waits idle and replaces the next failed replica. Scaling *down*
isn't supported yet.

**Rolling restarts are safe.** A storage pod reports ready (`/ready` on the health port)
only once its replica has re-copied its shard, so `kubectl rollout restart` never takes a
shard's second replica down before the first has recovered. Without this, a rollout with
replication 2 could restart both replicas of a shard back to back and lose it.

More commit proxies (`PROXIES`) and logs (`LOGS`) take effect at the next recovery.
Restarting one coordinator pod triggers one, or the cluster restores missing redundancy by
itself.

## The CLI

The image contains `swiftcache-cli`. Run it as a one-off pod:

```bash
CLUSTER=$(kubectl -n swiftcache get configmap swiftcache-config -o jsonpath='{.data.CLUSTER}')
alias sc="kubectl -n swiftcache run cli-\$RANDOM --rm -i --quiet --restart=Never --image=swiftcache:latest \
  --env=SWIFTCACHE_CLUSTER=$CLUSTER --command -- swiftcache-cli"

sc status --stats                    # generation, role placement, shards, storage, processes
sc set greeting hello --ttl 60
sc get greeting                      # value + versionstamp
sc get greeting --at-version 1795341970010421
sc cas greeting 000660da9fde0c9f0000 "hi"   # expected versionstamp, or 'none'
sc incr page-views --by 1
sc range user/ --limit 20
sc read-version
sc wait-ready --shards 3 --min-generation 5
sc verify --min-shards 2             # end-to-end correctness suite
sc bench --seconds 10 --concurrency 32 --read-ratio 0.8
```

## Failure behaviour

| Failure | What happens | Client-visible |
| --- | --- | --- |
| Storage pod dies | Its shard keeps serving from the other replicas. After `--failure-timeout` the replica is removed; the restarted pod is re-added and copies the shard from a peer | Nothing (reads use other replicas) |
| Log pod dies | The controller recovers a new generation on the remaining logs, then restores 3 logs when the pod is back | Commits pause for the recovery (~1–3 s); retried automatically |
| Pod hosting the controller / master / proxies dies | Re-election (if the controller died) and recovery | Commits pause; retried automatically |
| One coordinator dies | Nothing, as long as a majority remains | Nothing |
| A coordinator majority is down | The running generation keeps committing. No re-election or recovery is possible until a majority returns | Nothing, unless a transaction role also fails |
| All coordinators restart together | Their state is on PVCs; the next recovery reads it and continues | Commits pause for the recovery |
| Every replica of a shard dies at once | That shard's data is lost (in-memory). After `--shard-loss-timeout` its slots are reassigned empty | Data loss for those keys |
| Every log of a generation dies at once | Commits not yet applied by storage are lost; the cluster continues from what storage has | Loss of the most recent commits |

## Server flags (`swiftcache-server --help`)

| Flag | Default | Notes |
| --- | --- | --- |
| `--class` | — | `stateless`, `log` or `storage` |
| `--coordinators` | — | The cluster file: comma-separated `host:port` |
| `--coordinator`, `--data-dir` | — | This process is a coordinator; its durable state lives in `--data-dir` |
| `--replication` | 2 | Storage replicas per shard |
| `--logs` | 3 | Transaction logs per generation |
| `--proxies` | 2 | Commit proxies per generation (the k8s config sets 1: one proxy on the master's process is fastest on small clusters) |
| `--mvcc-window` | 10 s | How far back historical reads and long transactions work |
| `--failure-timeout` | 3 s | Missed registrations before a process counts as dead |
| `--shard-loss-timeout` | 120 s | Silence tolerated from a shard's *last* replica before its data is given up |
| `--health-port` | 8080 | HTTP probes: `/ready` (storage: caught-up member), `/live`; 0 disables |
| `--log-level` | info | |

Production timings are more conservative than the simulator's: leases 3 s, heartbeats every
300 ms, step-down after 1.8 s.

## Running the end-to-end test

```bash
./scripts/k3s-test.sh            # build, deploy a fresh namespace, then:
                                 #   verify → delete a storage pod → delete a log pod
                                 #   → delete the controller's pod → delete ALL coordinators
                                 #   → verify → scale 2→3 shards → verify → bench
IMAGE=swiftcache:abc123 ./scripts/k3s-test.sh
```

**The test deletes and recreates the `swiftcache` namespace.**
