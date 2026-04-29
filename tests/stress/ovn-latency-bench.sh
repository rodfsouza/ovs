#!/bin/bash
# ovn-latency-bench.sh — Measure per-operation latency.
#
# Usage: ./ovn-latency-bench.sh [DB] [N_ROUTERS] [N_ITERATIONS]
#
# Tests:
#   1. Single-row create latency
#   2. Single-row lookup by name latency
#   3. Single-row lookup by UUID latency
#   4. Full table scan latency (vary table size)
#   5. Single-row delete latency

set -e

DB="${1:-unix:/var/run/openvswitch/db.sock}"
N_ROUTERS="${2:-1000}"
N_ITER="${3:-50}"
NBCTL="ovn-nbctl --db=$DB"

echo "=== OVN Latency Benchmark ==="
echo "DB:         $DB"
echo "Routers:    $N_ROUTERS"
echo "Iterations: $N_ITER per test"
echo ""

TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

# --- Seed routers ---
echo "Seeding $N_ROUTERS routers..."
start=$(date +%s%N)
for i in $(seq 1 $N_ROUTERS); do
    $NBCTL lr-add "bench-router-$i"
done
end=$(date +%s%N)
ms=$(( (end - start) / 1000000 ))
echo "Seeded in ${ms}ms ($((N_ROUTERS * 1000 / (ms + 1)))/sec)"

# Grab some UUIDs for lookup tests.
echo "Collecting UUIDs..."
$NBCTL lr-list > "$TMPDIR/lr-list.txt"
head -20 "$TMPDIR/lr-list.txt" | awk '{print $1}' > "$TMPDIR/uuids.txt"

echo ""

# --- Test 1: Single-row create ---
echo "--- Test 1: Single-row create latency ---"
total=0
for i in $(seq 1 $N_ITER); do
    name="bench-tmp-$i"
    start=$(date +%s%N)
    $NBCTL lr-add "$name"
    end=$(date +%s%N)
    us=$(( (end - start) / 1000 ))
    total=$((total + us))
done
avg=$((total / N_ITER))
echo "  Avg: ${avg}us per create"

# Clean up tmp routers.
for i in $(seq 1 $N_ITER); do
    $NBCTL lr-del "bench-tmp-$i" 2>/dev/null || true
done

# --- Test 2: Lookup by name ---
echo ""
echo "--- Test 2: Lookup by name latency ---"
total=0
for i in $(seq 1 $N_ITER); do
    idx=$(( (i % N_ROUTERS) + 1 ))
    name="bench-router-$idx"
    start=$(date +%s%N)
    $NBCTL list Logical_Router "$name" > /dev/null
    end=$(date +%s%N)
    us=$(( (end - start) / 1000 ))
    total=$((total + us))
done
avg=$((total / N_ITER))
echo "  Avg: ${avg}us per name lookup"

# --- Test 3: Lookup by UUID ---
echo ""
echo "--- Test 3: Lookup by UUID latency ---"
n_uuids=$(wc -l < "$TMPDIR/uuids.txt")
if [ "$n_uuids" -gt 0 ]; then
    total=0
    for i in $(seq 1 $N_ITER); do
        uuid=$(sed -n "$(( (i % n_uuids) + 1 ))p" "$TMPDIR/uuids.txt")
        start=$(date +%s%N)
        $NBCTL list Logical_Router "$uuid" > /dev/null
        end=$(date +%s%N)
        us=$(( (end - start) / 1000 ))
        total=$((total + us))
    done
    avg=$((total / N_ITER))
    echo "  Avg: ${avg}us per UUID lookup"
else
    echo "  SKIP: no UUIDs available"
fi

# --- Test 4: Full table scan at different sizes ---
echo ""
echo "--- Test 4: Full table scan latency ---"
sizes="100 500 $N_ROUTERS"
for sz in $sizes; do
    total=0
    repeats=5
    for r in $(seq 1 $repeats); do
        start=$(date +%s%N)
        $NBCTL list Logical_Router > /dev/null
        end=$(date +%s%N)
        us=$(( (end - start) / 1000 ))
        total=$((total + us))
    done
    avg=$((total / repeats))
    echo "  $N_ROUTERS rows: avg ${avg}us per scan"
done

# --- Test 5: Single-row delete ---
echo ""
echo "--- Test 5: Single-row delete latency ---"
# Create targets for deletion.
for i in $(seq 1 $N_ITER); do
    $NBCTL lr-add "bench-del-$i" 2>/dev/null || true
done
total=0
for i in $(seq 1 $N_ITER); do
    start=$(date +%s%N)
    $NBCTL lr-del "bench-del-$i"
    end=$(date +%s%N)
    us=$(( (end - start) / 1000 ))
    total=$((total + us))
done
avg=$((total / N_ITER))
echo "  Avg: ${avg}us per delete"

# --- Cleanup ---
echo ""
echo "Cleaning up $N_ROUTERS seed routers..."
start=$(date +%s%N)
for i in $(seq 1 $N_ROUTERS); do
    $NBCTL lr-del "bench-router-$i" 2>/dev/null || true
done
end=$(date +%s%N)
ms=$(( (end - start) / 1000000 ))
echo "Cleanup: ${ms}ms"

echo ""
echo "=== DONE ==="
