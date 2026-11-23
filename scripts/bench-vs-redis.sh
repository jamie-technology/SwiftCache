#!/usr/bin/env bash
# Side-by-side benchmark: Redis 7 vs SwiftCache, both inside the same k3s cluster, with
# the same client concurrency, key space and value size and no pipelining.
#
#   ./scripts/bench-vs-redis.sh              # uses the running swiftcache namespace
#   IMAGE=swiftcache:abc ./scripts/bench-vs-redis.sh
#
# Redis runs with persistence off (SwiftCache is in-memory too). Note the semantic gap:
# every SwiftCache operation is a strictly serializable, replicated transaction
# (replication 2, 3 transaction logs); Redis is a single unreplicated process.
set -euo pipefail
cd "$(dirname "$0")/.."
NS=swiftcache
K="kubectl -n $NS"
IMAGE="${IMAGE:-swiftcache:latest}"
CONCURRENCY_LEVELS="${CONCURRENCY_LEVELS:-32 256}"
VALUE_SIZE="${VALUE_SIZE:-100}"
KEYS="${KEYS:-10000}"
SECONDS_PER_TEST="${SECONDS_PER_TEST:-15}"
MODES="${MODES:-get set txn}"
# SwiftCache client I/O threads. Benchmark tasks run on them (`--executor loop`); one is
# fastest on a 4-vCPU node, where every extra client thread competes with the servers.
# (redis-benchmark is single-threaded too.)  REDIS=0 skips the Redis runs.

$K apply -f - >/dev/null <<EOF
apiVersion: apps/v1
kind: Deployment
metadata: {name: redis}
spec:
  replicas: 1
  selector: {matchLabels: {app: redis}}
  template:
    metadata: {labels: {app: redis}}
    spec:
      containers:
        - name: redis
          image: redis:7.4-alpine
          imagePullPolicy: IfNotPresent
          args: ["--save", "", "--appendonly", "no", "--protected-mode", "no"]
          ports: [{containerPort: 6379}]
---
apiVersion: v1
kind: Service
metadata: {name: redis}
spec:
  selector: {app: redis}
  ports: [{port: 6379}]
EOF
$K rollout status deployment/redis --timeout=120s >/dev/null

run() {
  local image="$1"; shift
  $K run "bench-$RANDOM$RANDOM" --rm -i --quiet --restart=Never --image="$image" --image-pull-policy=IfNotPresent \
    --env=SWIFTCACHE_CLUSTER="$($K get configmap swiftcache-config -o jsonpath='{.data.CLUSTER}')" --command -- "$@"
}

for CONCURRENCY in $CONCURRENCY_LEVELS; do
if [[ "${REDIS:-1}" == 1 ]]; then
echo "== Redis 7, $CONCURRENCY clients (redis-benchmark -d $VALUE_SIZE -r $KEYS, no pipelining)"
N=$((CONCURRENCY * 3000))
run redis:7.4-alpine redis-benchmark -h redis -c "$CONCURRENCY" -n "$N" -d "$VALUE_SIZE" -r "$KEYS" -t set,get --csv \
  | tr -d '"' | awk -F, 'NR==1{next} {printf "%s: %.0f ops/s  p50=%sms p99=%sms max=%sms\n", tolower($1), $2, $5, $7, $8}'
fi

echo "== SwiftCache, $CONCURRENCY clients (same value size and key space)"
for mode in $MODES; do
  run "$IMAGE" swiftcache-cli bench --mode "$mode" --concurrency "$CONCURRENCY" --value-size "$VALUE_SIZE" \
    --keys "$KEYS" --seconds "$SECONDS_PER_TEST" --threads "${THREADS:-1}"
done
done
