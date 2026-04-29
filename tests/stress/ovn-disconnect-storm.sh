#!/bin/bash
# ovn-disconnect-storm.sh — Rapid connect/disconnect to stress
# binary streaming lifecycle (cancellation, cleanup, no crashes).
#
# Usage: ./ovn-disconnect-storm.sh [DB] [N_INITIAL] [N_STORMS] [TIMEOUT_MS]
#
# Seeds N_INITIAL routers, then rapidly connects clients that
# disconnect before streaming completes (via timeout).

set -e

DB="${1:-unix:/var/run/openvswitch/db.sock}"
N_INITIAL="${2:-1000}"
N_STORMS="${3:-50}"
TIMEOUT_MS="${4:-100}"
NBCTL="ovn-nbctl --db=$DB"

echo "=== OVN Disconnect Storm Test ==="
echo "DB:       $DB"
echo "Initial:  $N_INITIAL routers"
echo "Storms:   $N_STORMS rapid connects"
echo "Timeout:  ${TIMEOUT_MS}ms per connect"
echo ""

# --- Seed data so binary streaming has work to do ---
echo "Seeding $N_INITIAL routers..."
for i in $(seq 1 $N_INITIAL); do
    $NBCTL lr-add "storm-router-$i" 2>/dev/null || true
done
echo "Done seeding."

# --- Record server PID ---
SERVER_PID=$(pgrep -f "ovsdb-server.*db.sock" | head -1)
if [ -z "$SERVER_PID" ]; then
    echo "WARNING: cannot find ovsdb-server PID (crash detection disabled)"
fi

# --- Storm: rapid connect + disconnect ---
echo ""
echo "--- Disconnect storm ($N_STORMS iterations) ---"
failures=0

for s in $(seq 1 $N_STORMS); do
    # Connect with a short timeout — client starts binary streaming
    # then gets killed before INITIAL_END arrives.
    timeout_sec=$(echo "scale=3; $TIMEOUT_MS / 1000" | bc)
    if timeout "$timeout_sec" $NBCTL lr-list > /dev/null 2>&1; then
        # Completed within timeout — that's fine.
        :
    else
        exit_code=$?
        if [ $exit_code -eq 124 ]; then
            # Timeout (expected — client killed during streaming).
            :
        else
            failures=$((failures + 1))
        fi
    fi

    # Check server is still alive.
    if [ -n "$SERVER_PID" ] && ! kill -0 "$SERVER_PID" 2>/dev/null; then
        echo "FATAL: ovsdb-server crashed at iteration $s!"
        exit 1
    fi

    # Progress every 10 iterations.
    if [ $((s % 10)) -eq 0 ]; then
        echo "  Completed $s/$N_STORMS storms (failures: $failures)"
    fi
done

echo ""
echo "--- Results ---"
echo "Storms: $N_STORMS"
echo "Failures: $failures"

# --- Verify server still works ---
echo ""
echo "--- Post-storm verification ---"
start=$(date +%s%N)
n_lr=$($NBCTL lr-list | wc -l)
end=$(date +%s%N)
ms=$(( (end - start) / 1000000 ))
echo "lr-list: $n_lr routers in ${ms}ms"

if [ "$n_lr" -lt "$N_INITIAL" ]; then
    echo "WARNING: expected >= $N_INITIAL routers, got $n_lr"
fi

# --- Cleanup ---
echo ""
echo "Cleaning up..."
for i in $(seq 1 $N_INITIAL); do
    $NBCTL lr-del "storm-router-$i" 2>/dev/null || true
done

if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
    echo "Server still running (PID $SERVER_PID) — no crash."
else
    echo "WARNING: server may have exited."
fi

echo ""
echo "=== DONE ==="
