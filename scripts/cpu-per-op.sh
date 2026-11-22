#!/usr/bin/env bash
# CPU cost per operation against the local cluster (robust to background load):
# server CPU (all processes) and client CPU divided by completed operations, plus
# single-client latency. Usage: scripts/cpu-per-op.sh [mode...]
set -euo pipefail
cd "$(dirname "$0")/.."
export SWIFTCACHE_CLUSTER=127.0.0.1:4500,127.0.0.1:4501,127.0.0.1:4502
cpu() { ps -o time= -p "$(pgrep -d, -f 'swiftcache-server --class')" | awk -F'[:.]' '{ s += ($1*60 + $2) + $3/100 } END { printf "%.2f", s }'; }
for mode in "${@:-grv read-at get set txn}"; do
  for m in $mode; do
    before=$(cpu)
    out=$( { /usr/bin/time -p .build/release/swiftcache-cli bench --mode "$m" --seconds 6 --concurrency 32 --threads 2; } 2>&1 )
    after=$(cpu)
    ops=$(echo "$out" | awk '/ops\/s/ {print $2}')
    user=$(echo "$out" | awk '/^user/ {print $2}'); sys=$(echo "$out" | awk '/^sys/ {print $2}')
    lat=$(.build/release/swiftcache-cli bench --mode "$m" --seconds 2 --concurrency 1 | sed 's/.*p50=\([0-9.]*\)ms.*/\1/')
    awk -v m="$m" -v ops="$ops" -v b="$before" -v a="$after" -v u="$user" -v s="$sys" -v lat="$lat" 'BEGIN {
      n = ops * 6; printf "%-8s %8.0f ops/s   server %6.1f µs/op   client %6.1f µs/op   1-client p50 %sms\n", m, ops, (a-b)*1e6/n, (u+s)*1e6/n, lat }'
  done
done
