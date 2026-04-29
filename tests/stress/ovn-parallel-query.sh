#!/bin/bash
# ovn-parallel-query.sh — Multiple clients query in parallel while
# background writes happen continuously.
#
# Usage: ./ovn-parallel-query.sh [DB] [N_WRITERS] [N_READERS] [DURATION_SEC]
#
# Writers create/delete routers continuously.
# Readers do full table scans and single-row lookups simultaneously.

set -e

DB="${1:-unix:/var/run/openvswitch/db.sock}"
N_WRITERS="${2:-3}"
N_READERS="${3:-5}"
DURATION="${4:-30}"
NBCTL="ovn-nbctl --db=$DB"

echo "=== OVN Parallel Query Stress Test ==="
echo "DB:       $DB"
echo "Writers:  $N_WRITERS"
echo "Readers:  $N_READERS"
echo "Duration: ${DURATION}s"
echo ""

TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR; kill 0 2>/dev/null" EXIT

# --- Writer: create and delete routers in a loop ---
writer() {
    local id=$1
    local count=0
    local end_time=$(($(date +%s) + DURATION))

    while [ $(date +%s) -lt $end_time ]; do
        local name="par-w${id}-r${count}"
        $NBCTL lr-add "$name" 2>/dev/null || true
        $NBCTL lr-del "$name" 2>/dev/null || true
        count=$((count + 1))
    done
    echo "$count" > "$TMPDIR/writer-$id.count"
}

# --- Reader (full scan): list all routers ---
reader_scan() {
    local id=$1
    local count=0
    local errors=0
    local end_time=$(($(date +%s) + DURATION))

    while [ $(date +%s) -lt $end_time ]; do
        if $NBCTL lr-list > /dev/null 2>&1; then
            count=$((count + 1))
        else
            errors=$((errors + 1))
        fi
    done
    echo "$count" > "$TMPDIR/reader-scan-$id.count"
    echo "$errors" > "$TMPDIR/reader-scan-$id.errors"
}

# --- Reader (point lookup): get specific routers by name ---
reader_lookup() {
    local id=$1
    local count=0
    local errors=0
    local end_time=$(($(date +%s) + DURATION))

    # Create a stable router for lookups.
    local target="par-lookup-target-$id"
    $NBCTL lr-add "$target" 2>/dev/null || true

    while [ $(date +%s) -lt $end_time ]; do
        if $NBCTL list Logical_Router "$target" > /dev/null 2>&1; then
            count=$((count + 1))
        else
            errors=$((errors + 1))
        fi
    done
    $NBCTL lr-del "$target" 2>/dev/null || true
    echo "$count" > "$TMPDIR/reader-lookup-$id.count"
    echo "$errors" > "$TMPDIR/reader-lookup-$id.errors"
}

# --- Launch writers ---
echo "Launching $N_WRITERS writers..."
for i in $(seq 1 $N_WRITERS); do
    writer $i &
done

# --- Launch readers (half scan, half lookup) ---
n_scan=$((N_READERS / 2))
n_lookup=$((N_READERS - n_scan))

echo "Launching $n_scan scan readers + $n_lookup lookup readers..."
for i in $(seq 1 $n_scan); do
    reader_scan $i &
done
for i in $(seq 1 $n_lookup); do
    reader_lookup $i &
done

echo "Running for ${DURATION}s..."
wait

# --- Collect results ---
echo ""
echo "--- Results ---"

total_writes=0
for i in $(seq 1 $N_WRITERS); do
    c=$(cat "$TMPDIR/writer-$i.count" 2>/dev/null || echo 0)
    total_writes=$((total_writes + c))
    echo "Writer $i: $c create+delete cycles"
done

total_scans=0
total_scan_errors=0
for i in $(seq 1 $n_scan); do
    c=$(cat "$TMPDIR/reader-scan-$i.count" 2>/dev/null || echo 0)
    e=$(cat "$TMPDIR/reader-scan-$i.errors" 2>/dev/null || echo 0)
    total_scans=$((total_scans + c))
    total_scan_errors=$((total_scan_errors + e))
    echo "Scan reader $i: $c scans, $e errors"
done

total_lookups=0
total_lookup_errors=0
for i in $(seq 1 $n_lookup); do
    c=$(cat "$TMPDIR/reader-lookup-$i.count" 2>/dev/null || echo 0)
    e=$(cat "$TMPDIR/reader-lookup-$i.errors" 2>/dev/null || echo 0)
    total_lookups=$((total_lookups + c))
    total_lookup_errors=$((total_lookup_errors + e))
    echo "Lookup reader $i: $c lookups, $e errors"
done

echo ""
echo "--- Summary ---"
echo "Total write cycles:  $total_writes ($((total_writes / DURATION))/sec)"
echo "Total full scans:    $total_scans ($((total_scans / DURATION))/sec)"
echo "Total point lookups: $total_lookups ($((total_lookups / DURATION))/sec)"
echo "Scan errors:         $total_scan_errors"
echo "Lookup errors:       $total_lookup_errors"

if [ "$total_scan_errors" -gt 0 ] || [ "$total_lookup_errors" -gt 0 ]; then
    echo ""
    echo "WARNING: some reads failed (may be expected during heavy writes)"
fi

echo ""
echo "=== DONE ==="
