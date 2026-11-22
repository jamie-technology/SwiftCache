#!/usr/bin/env bash
# Rebuilds the image and rolls it onto the running swiftcache namespace (dev loop).
set -euo pipefail
cd "$(dirname "$0")/.."
IMG=$(./scripts/build-image.sh "swiftcache:dev-$(date +%s)" | tail -1)
sed "s#image: swiftcache:latest#image: $IMG#" deploy/k8s/swiftcache.yaml | kubectl apply -f - >/dev/null
kubectl -n swiftcache scale statefulset swiftcache-storage --replicas="${STORAGE:-6}" >/dev/null
for ss in coordinator log storage; do
  kubectl -n swiftcache rollout status statefulset/swiftcache-$ss --timeout=300s >/dev/null
done
kubectl -n swiftcache run "wait-$RANDOM" --rm -i --quiet --restart=Never --image="$IMG" \
  --env=SWIFTCACHE_CLUSTER="$(kubectl -n swiftcache get configmap swiftcache-config -o jsonpath='{.data.CLUSTER}')" \
  --command -- swiftcache-cli wait-ready --shards "${SHARDS:-3}" --timeout 300 >&2
echo "$IMG"
