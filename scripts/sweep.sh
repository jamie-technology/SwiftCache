#!/usr/bin/env bash
# Full validation sweep: random chaos (3 × SEEDS seeds in parallel) + every scenario.
set -uo pipefail
cd "$(dirname "$0")/.."
SEEDS="${SEEDS:-110}"; SCEN="${SCENARIO_SEEDS:-15}"
D=.build/sweep; mkdir -p $D; rm -f $D/r*.txt
cp .build/release/swiftcache-sim $D/sim
$D/sim --seed 1 --count "$SEEDS" > $D/r1.txt 2>&1 &
$D/sim --seed $((SEEDS + 1)) --count "$SEEDS" > $D/r2.txt 2>&1 &
$D/sim --seed $((2 * SEEDS + 1)) --count "$SEEDS" > $D/r3.txt 2>&1 &
$D/sim --scenario all --seed 400 --count "$SCEN" > $D/r4.txt 2>&1 &
wait
grep -hE "FAIL|✗|runs passed" $D/r*.txt
! grep -q FAIL $D/r*.txt
