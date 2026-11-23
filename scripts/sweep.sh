#!/usr/bin/env bash
# Full validation sweep: random chaos (3 × SEEDS seeds in parallel) + every scenario.
set -uo pipefail
cd "$(dirname "$0")/.."
SEEDS="${SEEDS:-110}"; SCEN="${SCENARIO_SEEDS:-15}"
D=.build/sweep; mkdir -p $D; rm -f $D/r*.txt
rm -f $D/sim
cp .build/release/swiftcache-sim $D/sim || { echo "could not copy the simulator binary" >&2; exit 1; }
cmp -s .build/release/swiftcache-sim $D/sim || { echo "simulator copy is stale" >&2; exit 1; }
$D/sim --seed 1 --count "$SEEDS" > $D/r1.txt 2>&1 &
$D/sim --seed $((SEEDS + 1)) --count "$SEEDS" > $D/r2.txt 2>&1 &
$D/sim --seed $((2 * SEEDS + 1)) --count "$SEEDS" > $D/r3.txt 2>&1 &
$D/sim --scenario all --seed 400 --count "$SCEN" > $D/r4.txt 2>&1 &
wait
grep -hE "FAIL|✗|runs passed" $D/r*.txt
! grep -q FAIL $D/r*.txt
