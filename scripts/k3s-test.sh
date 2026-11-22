#!/usr/bin/env bash
# End-to-end test of SwiftCache on the local k3s cluster:
#   1. build the image and (re)deploy a fresh cluster
#      (3 coordinators/stateless, 3 logs, 4 storage = 2 shards × 2 replicas)
#   2. run the correctness suite (swiftcache-cli verify) inside the cluster
#   3. chaos: delete a storage pod, a log pod, the pod hosting the cluster controller,
#      and then all coordinators at once (their state must survive on the PVCs);
#      data must survive and verify must pass after each
#   4. scale out to 3 shards; slots must rebalance and verify must pass
#   5. run a short benchmark
set -euo pipefail
cd "$(dirname "$0")/.."
NS=swiftcache
IMAGE="${IMAGE:-}"
K="kubectl -n $NS"

step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }

# Runs swiftcache-cli inside the cluster as a one-off pod and streams its output.
cli() {
  local name="cli-$RANDOM$RANDOM"
  $K run "$name" --rm -i --quiet --restart=Never --image="$IMAGE" --image-pull-policy=IfNotPresent \
    --env=SWIFTCACHE_CLUSTER="$($K get configmap swiftcache-config -o jsonpath='{.data.CLUSTER}')" \
    --command -- /usr/local/bin/swiftcache-cli "$@"
}

if [[ -z "$IMAGE" ]]; then
  step "Building image"
  IMAGE=$(./scripts/build-image.sh | tail -1)
fi
echo "image: $IMAGE"

step "Deploying fresh cluster"
kubectl delete namespace $NS --ignore-not-found --wait=true
sed "s#image: swiftcache:latest#image: $IMAGE#" deploy/k8s/swiftcache.yaml | kubectl apply -f -
for ss in coordinator log storage; do $K rollout status statefulset/swiftcache-$ss --timeout=180s; done
cli wait-ready --shards 2 --timeout 180

step "Correctness suite"
cli verify --min-shards 2

step "Writing marker data"
cli set marker/a "before-chaos"
cli incr marker/counter --by 42

controller_pod() { cli status | awk '/^controller:/ {print $2}' | cut -d. -f1; }
generation() { cli status | awk '/^phase:/ {print $4}'; }

step "Chaos: delete a storage pod"
$K delete pod swiftcache-storage-1 --wait=false
sleep 2
cli get marker/a
$K rollout status statefulset/swiftcache-storage --timeout=120s
cli wait-ready --shards 2 --timeout 120

step "Chaos: delete a log pod (forces a new transaction-system generation)"
GEN=$(generation)
$K delete pod swiftcache-log-0 --wait=false
cli wait-ready --shards 2 --min-generation $((GEN + 1)) --timeout 120
cli get marker/a

step "Chaos: delete the pod hosting the cluster controller"
CC=$(controller_pod); GEN=$(generation); echo "controller is on $CC (generation $GEN)"
$K delete pod "$CC" --wait=false
cli wait-ready --shards 2 --min-generation $((GEN + 1)) --timeout 120
cli get marker/a
echo "new controller on $(controller_pod)"

step "Chaos: delete ALL coordinator pods at once (state must survive on their volumes)"
GEN=$(generation)
$K delete pod swiftcache-coordinator-0 swiftcache-coordinator-1 swiftcache-coordinator-2 --wait=false
sleep 5
$K rollout status statefulset/swiftcache-coordinator --timeout=180s
cli wait-ready --shards 2 --min-generation $((GEN + 1)) --timeout 180
cli get marker/a
cli get marker/counter >/dev/null
cli verify --min-shards 2

step "Scale out: 4 -> 6 storage pods (2 -> 3 shards)"
$K scale statefulset swiftcache-storage --replicas=6
$K rollout status statefulset/swiftcache-storage --timeout=180s
cli wait-ready --shards 3 --timeout 180
cli get marker/a
cli status --stats
cli verify --min-shards 3

step "Benchmark (10s, 32 concurrent, 80% reads)"
cli bench --seconds 10 --concurrency 32

step "All k3s tests passed"
