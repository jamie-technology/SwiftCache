#!/usr/bin/env bash
# Starts a local release-build cluster on 127.0.0.1 (3 coordinators, 3 logs, N storage)
# for profiling and benchmarking without Docker networking. `scripts/local-cluster.sh stop`.
set -euo pipefail
cd "$(dirname "$0")/.."
pkill -f "swiftcache-server --class" 2>/dev/null || true
[[ "${1:-}" == "stop" ]] && exit 0
sleep 1
STORAGE="${STORAGE:-6}"
B=.build/release
L=.build/local-cluster
rm -rf "$L" && mkdir -p "$L"
export SWIFTCACHE_CLUSTER=127.0.0.1:4500,127.0.0.1:4501,127.0.0.1:4502
run() {
  $B/swiftcache-server --class "$1" --listen "127.0.0.1:$2" --advertise "127.0.0.1:$2" \
    --coordinators "$SWIFTCACHE_CLUSTER" --replication 2 --health-port 0 --log-level ${LOG_LEVEL:-info} "${@:3}" >> "$L/p$2.log" 2>&1 &
}
for p in 4500 4501 4502; do run stateless $p --coordinator --data-dir "$L/d$p"; done
for p in 4510 4511 4512; do run log $p; done
for i in $(seq 0 $((STORAGE - 1))); do run storage $((4520 + i)); done
$B/swiftcache-cli wait-ready --shards $((STORAGE / 2)) --timeout 120
