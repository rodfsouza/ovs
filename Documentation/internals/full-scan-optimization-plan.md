# Research: Improving Full-Database Scans

## Problem

When a client requests the full database (e.g., `ovn-northd` with `monitor_cond [true]`, or `ovn-nbctl list TABLE` without record_id), the current implementation has three memory/performance problems:

### 1. Cache Thrashing (No Eviction Deferral)

`ovsdb_table_query()` Path 3 (disk cursor scan) inserts every deserialized row into the cache (table.c:738-740). But the disk cursor path does **NOT** increment `cache->iterating`, so LRU eviction fires immediately on each insert when the cache is over budget.

Result: insert row N → evict row N-1 → insert row N+1 → evict row N-2. Continuous thrashing for the entire scan. The cache budget is respected but at massive CPU cost (malloc+free+pread+deserialize per row, none kept).

### 2. Full JSON Response in RAM

`ovsdb_monitor_compose_update()` (monitor.c:1173-1189) builds the entire initial dump as a single `struct json` tree. For 1M rows × 200 bytes JSON/row = 200+ MB in RAM before serialization. Plus the change set clones from `ovsdb_monitor_changes_update()` = another copy. Peak memory is 2-3x the database size.

### 3. Unnecessary Disk Reads for Cached Rows

During a full scan, the disk cursor path checks if a row is in the cache (table.c:712) and yields the cached version if found. But the cursor still opens all entries and checks each one. For a warm cache (e.g., after startup warmup), this is redundant — we could yield cached rows first without touching disk.

## Proposed Improvements

### Fix 1: Defer Cache Eviction During Full Scans

**`ovsdb/table.c`** — In `ovsdb_table_query()`, increment `cache->iterating` around the disk cursor loop:

```c
/* Step B: walk disk cursor if disk_store is present. */
if (!table->disk_store) return;

if (table->cache) {
    table->cache->iterating++;
}

/* ... disk cursor loop ... */

if (table->cache) {
    table->cache->iterating--;
    ovsdb_row_cache_sweep_deferred(table->cache);
}
```

**Impact:** Eviction is deferred until the scan completes. All scanned rows stay in cache during iteration. After the scan, deferred evictions fire and the cache settles back to its budget. This prevents the insert-evict-insert thrashing.

**Trade-off:** Cache temporarily exceeds its atom budget during the scan. For a 1M-row table this could be significant. But it's bounded by the scan duration (seconds, not permanent).

**Alternative:** Don't cache rows from unconditioned scans at all — just deserialize, yield to callback, destroy. This avoids both thrashing AND temporary over-budget:

```c
/* For unconditioned scans, don't pollute the cache.
 * The callback consumes the row data inline (e.g.,
 * monitor clones it into the change set). */
if (!check_cond && table->cache) {
    /* Skip cache insertion — yield and destroy. */
    bool cont = cb(disk_row, aux);
    ovsdb_row_destroy(disk_row);
    if (!cont) break;
    continue;
}
```

### Fix 2: Cache-First, Disk-Remainder Iteration

Instead of the current pattern (yield table->rows, then ALL disk entries with dedup), split into:

1. **Yield table->rows** (in-memory modifications, as now)
2. **Yield CACHED rows from row cache** (already in memory, no disk I/O)
3. **Yield remaining UNLOADED rows from disk** (only rows NOT in cache)

This is what `ovsdb_table_for_each_loaded_row()` partially does, but it stops at step 2 and never reads disk. The full variant would continue to disk for uncached rows.

**Benefit:** If the cache is 80% warm, only 20% of rows need disk I/O. The 80% cached rows are yielded from memory with zero pread calls.

**Implementation:** The row cache tracks which UUIDs are CACHED vs UNLOADED. During the disk cursor loop, skip entries whose UUID is already CACHED (they were yielded in step 2). Only pread UNLOADED entries.

```c
/* Step B: yield cached rows (no disk I/O). */
if (table->cache) {
    ovsdb_row_cache_for_each_loaded(table->cache,
                                    yield_cached_cb, &scan_aux);
}

/* Step C: yield remaining UNLOADED rows from disk. */
cursor = ovsdb_disk_store_cursor_open(...);
while ((disk_row = cursor_next(...))) {
    if (table_rows_contains(table, uuid)) { skip; }
    if (row_cache_get_state(cache, uuid) == CACHED) { skip; }
    /* UNLOADED — must read from disk. */
    cb(disk_row, aux);
    /* Don't cache — it's a full scan, would just thrash. */
    ovsdb_row_destroy(disk_row);
}
```

### Fix 3: Streaming JSON Responses (Future — Larger Scope)

Instead of building the entire JSON response in memory, serialize and send rows incrementally:

1. Send JSON array opening `[`
2. For each row: serialize to JSON string, send, free
3. Send JSON array closing `]`

This keeps peak memory at O(1) — only one row's JSON in memory at a time.

**Challenge:** The OVSDB protocol sends the full response as a single JSON-RPC reply. Streaming would require protocol changes or chunked transfer encoding. This is a larger architectural change.

**Intermediate step:** Use `json_serialized_object_create()` to pre-serialize each row and free the intermediate JSON tree, keeping only the serialized bytes. This doesn't reduce network traffic but reduces peak memory by avoiding the full JSON tree.

## Recommended Implementation Order

1. **Fix 1 (skip cache for unconditioned scans)** — 5-line change, biggest impact
2. **Fix 2 (cache-first iteration)** — moderate change, reduces disk I/O
3. **Fix 3 (streaming)** — future work, protocol-level change

## Files to Modify

| File | Change |
|------|--------|
| `ovsdb/table.c` | Skip cache insertion for unconditioned scans in Path 3 |
| `ovsdb/table.c` | Cache-first iteration: yield cached rows before disk cursor |

## Tests

- Full table scan with small cache budget: verify no thrashing (monitor memory)
- Full table scan with warm cache: verify cached rows yielded without disk I/O
- Conditioned scan: verify matching rows still cached (existing behavior)
- Monitor initial dump: verify same results, lower peak memory
