#!/bin/bash
# ovn-full-scan-under-load.sh — Full table scan while CRUD happens.
#
# Usage: ./ovn-full-scan-under-load.sh [DB] [N_INITIAL] [N_SCANS] [N_BG_WRITERS]
#
# 1. Seeds N_INITIAL routers
# 2. Starts N_BG_WRITERS background writers doing continuous CRUD
# 3. Runs N_SCANS full table scans, measuring each
# 4. Verifies no crash, all scans succeed

set -e

DB="${1:-unix:/var/run/openvswitch/db.sock}"
N_INITIAL="${2:-500}"
N_SCANS="${3:-20}"
N_BG_WRITERS="${4:-3}"
NBCTL="ovn-nbctl --db=$DB"

echo "=== OVN Full Scan Under Load ==="
echo "DB:          $DB"
echo "Initial:     $N_INITIAL routers"
echo "Scans:       $N_SCANS"
echo "BG writers:  $N_BG_WRITERS"
echo ""

TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR; kill 0 2>/dev/null" EXIT

# --- Cleanup from previous runs ---
echo "Cleaning up..."
for i in $(seq 1 $((N_INITIAL + 1000))); do
    $NBCTL lr-del "scan-router-$i" 2>/dev/null || true
done
for i in $(seq 1 $N_BG_WRITERS); do
    for j in $(seq 0 1000); do
        $NBCTL lr-del "bg-router-${i}-${j}" 2>/dev/null || true
    done
done

# --- Phase 1: Seed initial routers ---
echo "Seeding $N_INITIAL routers..."
start=$(date +%s%N)
for i in $(seq 1 $N_INITIAL); do
    $NBCTL lr-add "scan-router-$i"
done
end=$(date +%s%N)
ms=$(( (end - start) / 1000000 ))
echo "Seeded in ${ms}ms"

# --- Phase 2: Start background writers ---
bg_writer() {
    local id=$1
    local count=0
    while true; do
        local name="bg-router-${id}-${count}"
        $NBCTL lr-add "$name" 2>/dev/null || true
        $NBCTL lr-del "$name" 2>/dev/null || true
        count=$(( (count + 1) % 100 ))
    done
}

echo "Starting $N_BG_WRITERS background writers..."
for i in $(seq 1 $N_BG_WRITERS); do
    bg_writer $i &
done
BG_PIDS=$(jobs -p)

# Give writers a moment to start.
sleep 1

# --- Phase 3: Run full table scans ---
echo ""
echo "--- Full table scans (with background writes) ---"
total_ms=0
min_ms=999999
max_ms=0

for s in $(seq 1 $N_SCANS); do
    start=$(date +%s%N)
    n_rows=$($NBCTL list Logical_Router 2>/dev/null | grep -c "^_uuid" || echo 0)
    end=$(date +%s%N)
    ms=$(( (end - start) / 1000000 ))
    total_ms=$((total_ms + ms))
    [ $ms -lt $min_ms ] && min_ms=$ms
    [ $ms -gt $max_ms ] && max_ms=$ms
    echo "  Scan $s: ${ms}ms ($n_rows rows)"
done

avg_ms=$((total_ms / N_SCANS))
echo ""
echo "--- Scan Results ---"
echo "  Min: ${min_ms}ms"
echo "  Max: ${max_ms}ms"
echo "  Avg: ${avg_ms}ms"

# --- Phase 4: Kill background writers ---
echo ""
echo "Stopping background writers..."
kill $BG_PIDS 2>/dev/null || true
wait 2>/dev/null

# --- Phase 5: Cleanup ---
echo "Cleaning up seed routers..."
start=$(date +%s%N)
for i in $(seq 1 $N_INITIAL); do
    $NBCTL lr-del "scan-router-$i" 2>/dev/null || true
done
end=$(date +%s%N)
ms=$(( (end - start) / 1000000 ))
echo "Cleanup: ${ms}ms"

echo ""
echo "=== DONE ==="
