#!/bin/bash
# run-all.sh — Run all OVN stress tests.
#
# Usage: ./run-all.sh [DB]
#
# Runs each test sequentially with moderate parameters.
# For heavy stress, run individual scripts with larger values.

set -e

DB="${1:-unix:/var/run/openvswitch/db.sock}"
DIR="$(cd "$(dirname "$0")" && pwd)"

echo "========================================"
echo "  OVN/OVS Stress Test Suite"
echo "  DB: $DB"
echo "  $(date)"
echo "========================================"

run_test() {
    local name="$1"
    shift
    echo ""
    echo "========================================"
    echo "  Running: $name"
    echo "========================================"
    if "$DIR/$name" "$DB" "$@"; then
        echo ">>> $name: PASSED"
    else
        echo ">>> $name: FAILED (exit $?)"
        return 1
    fi
}

failures=0

# Test 1: Bulk create/delete (moderate: 100 routers, 50 switches, 3 ports)
run_test ovn-bulk-create.sh 100 50 3 || failures=$((failures + 1))

# Test 2: Parallel query (3 writers, 4 readers, 15 seconds)
run_test ovn-parallel-query.sh 3 4 15 || failures=$((failures + 1))

# Test 3: Full scan under load (200 initial, 10 scans, 2 bg writers)
run_test ovn-full-scan-under-load.sh 200 10 2 || failures=$((failures + 1))

# Test 4: Disconnect storm (500 initial, 30 storms, 200ms timeout)
run_test ovn-disconnect-storm.sh 500 30 200 || failures=$((failures + 1))

# Test 5: Latency benchmark (500 routers, 20 iterations)
run_test ovn-latency-bench.sh 500 20 || failures=$((failures + 1))

echo ""
echo "========================================"
echo "  Results: $((5 - failures))/5 passed"
if [ $failures -gt 0 ]; then
    echo "  $failures FAILED"
    exit 1
else
    echo "  All tests PASSED"
fi
echo "========================================"
