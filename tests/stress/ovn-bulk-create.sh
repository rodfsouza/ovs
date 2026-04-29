#!/bin/bash
# ovn-bulk-create.sh — Bulk create/delete routers, switches, and ports.
#
# Usage: ./ovn-bulk-create.sh [DB] [N_ROUTERS] [N_SWITCHES] [N_PORTS_PER_SWITCH]
#
# Measures throughput for each phase: create, verify, delete.

set -e

DB="${1:-unix:/var/run/openvswitch/db.sock}"
N_ROUTERS="${2:-100}"
N_SWITCHES="${3:-100}"
N_PORTS="${4:-5}"
NBCTL="ovn-nbctl --db=$DB"

echo "=== OVN Bulk Create Stress Test ==="
echo "DB:       $DB"
echo "Routers:  $N_ROUTERS"
echo "Switches: $N_SWITCHES"
echo "Ports/sw: $N_PORTS"
echo ""

cleanup() {
    echo ""
    echo "--- Cleanup ---"
    local start=$(date +%s%N)
    for i in $(seq 1 $N_ROUTERS); do
        $NBCTL lr-del "stress-router-$i" 2>/dev/null || true
    done
    for i in $(seq 1 $N_SWITCHES); do
        $NBCTL ls-del "stress-switch-$i" 2>/dev/null || true
    done
    local end=$(date +%s%N)
    local ms=$(( (end - start) / 1000000 ))
    echo "Cleanup: ${ms}ms"
}

# Clean up any leftovers from previous runs.
cleanup 2>/dev/null

# --- Phase 1: Create routers ---
echo "--- Phase 1: Create $N_ROUTERS routers ---"
start=$(date +%s%N)
for i in $(seq 1 $N_ROUTERS); do
    $NBCTL lr-add "stress-router-$i"
done
end=$(date +%s%N)
ms=$(( (end - start) / 1000000 ))
rate=$(( N_ROUTERS * 1000 / (ms + 1) ))
echo "Created $N_ROUTERS routers in ${ms}ms ($rate/sec)"

# --- Phase 2: Create switches ---
echo ""
echo "--- Phase 2: Create $N_SWITCHES switches ---"
start=$(date +%s%N)
for i in $(seq 1 $N_SWITCHES); do
    $NBCTL ls-add "stress-switch-$i"
done
end=$(date +%s%N)
ms=$(( (end - start) / 1000000 ))
rate=$(( N_SWITCHES * 1000 / (ms + 1) ))
echo "Created $N_SWITCHES switches in ${ms}ms ($rate/sec)"

# --- Phase 3: Add ports to switches ---
total_ports=$((N_SWITCHES * N_PORTS))
echo ""
echo "--- Phase 3: Create $total_ports ports ($N_PORTS per switch) ---"
start=$(date +%s%N)
for i in $(seq 1 $N_SWITCHES); do
    for p in $(seq 1 $N_PORTS); do
        $NBCTL lsp-add "stress-switch-$i" "sw${i}-port${p}"
    done
done
end=$(date +%s%N)
ms=$(( (end - start) / 1000000 ))
rate=$(( total_ports * 1000 / (ms + 1) ))
echo "Created $total_ports ports in ${ms}ms ($rate/sec)"

# --- Phase 4: Verify counts ---
echo ""
echo "--- Phase 4: Verify ---"
start=$(date +%s%N)
n_lr=$($NBCTL lr-list | wc -l)
n_ls=$($NBCTL ls-list | wc -l)
end=$(date +%s%N)
ms=$(( (end - start) / 1000000 ))
echo "lr-list: $n_lr routers (expected >= $N_ROUTERS)"
echo "ls-list: $n_ls switches (expected >= $N_SWITCHES)"
echo "Query time: ${ms}ms"

if [ "$n_lr" -lt "$N_ROUTERS" ]; then
    echo "FAIL: expected at least $N_ROUTERS routers, got $n_lr"
    exit 1
fi
if [ "$n_ls" -lt "$N_SWITCHES" ]; then
    echo "FAIL: expected at least $N_SWITCHES switches, got $n_ls"
    exit 1
fi

# --- Phase 5: Full table scan ---
echo ""
echo "--- Phase 5: Full table scan (list Logical_Router) ---"
start=$(date +%s%N)
$NBCTL list Logical_Router > /dev/null
end=$(date +%s%N)
ms=$(( (end - start) / 1000000 ))
echo "Full scan: ${ms}ms"

# --- Phase 6: Delete everything ---
echo ""
echo "--- Phase 6: Delete all ---"
start=$(date +%s%N)
for i in $(seq 1 $N_SWITCHES); do
    $NBCTL ls-del "stress-switch-$i"
done
for i in $(seq 1 $N_ROUTERS); do
    $NBCTL lr-del "stress-router-$i"
done
end=$(date +%s%N)
ms=$(( (end - start) / 1000000 ))
echo "Deleted everything in ${ms}ms"

# --- Verify empty ---
n_lr=$($NBCTL lr-list | grep -c "stress-router" || true)
n_ls=$($NBCTL ls-list | grep -c "stress-switch" || true)
if [ "$n_lr" -ne 0 ] || [ "$n_ls" -ne 0 ]; then
    echo "FAIL: leftover resources (routers=$n_lr, switches=$n_ls)"
    exit 1
fi

echo ""
echo "=== PASSED ==="
