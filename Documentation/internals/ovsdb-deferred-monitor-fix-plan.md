# Fix: Binary Database Rows Invisible to Monitors and Queries (Lazy Loading Gap)

## Context

After converting an OVN Northbound database from JSON to binary format and serving it with disk-store mode, `ovn-nbctl show` returns empty output. The database has real data (switches, ports, routers, NAT rules) but the client sees nothing.

**Root cause**: In disk-store mode, `table->rows` hmap is empty — rows exist only in the disk store, tracked as `OVSDB_ROW_UNLOADED` in the row cache. Multiple critical code paths iterate `table->rows` directly via `HMAP_FOR_EACH`, bypassing the disk store entirely.

The safe single-row accessor `ovsdb_table_get_row()` (`ovsdb/table.c:358-421`) correctly handles cache → disk store → async load. But **bulk iteration** code paths never go through it.

## Affected Code Paths

| File | Line | Function | Impact |
|------|------|----------|--------|
| `ovsdb/monitor.c` | 1549 | `ovsdb_monitor_get_initial()` | **Primary bug** — empty initial monitor snapshot |
| `ovsdb/monitor.c` | ~1221 | `ovsdb_monitor_compose_cond_change_update()` | Empty condition-change updates |
| `ovsdb/query.c` | 45 | `ovsdb_query()` linear scan | Non-UUID SELECT returns no rows |
| `ovsdb/file.c` | ~321 | `ovsdb_file_clone()` | Clone misses disk-stored rows |
| `ovsdb/file.c` | ~442 | `ovsdb_to_txn_json()` | Snapshot serialization misses rows |
| `ovsdb/ovsdb.c` | ~614 | `ovsdb_clone_data()` | Background compaction misses rows |

## Design: Async Bulk Load + Deferred Monitor Reply

The fix must preserve the lazy-loading design — no synchronous bulk load that defeats memory optimization. Instead, we use the existing worker pool + trigger parking patterns.

### Three categories of fix:

**Category A — Monitor initial snapshot (async, deferred reply)**:
The monitor detects disk-store tables, submits bulk async load requests for all UNLOADED UUIDs, and defers the initial reply. When all rows are loaded by the worker pool, the monitor composes and sends the initial snapshot. This follows the same pattern as trigger `waiting_for_data`.

**Category B — Query linear scan (trigger parking, already works)**:
Queries go through triggers. Triggers already park via `waiting_for_data` when rows are UNLOADED/LOADING. The only gap is the linear scan path in `ovsdb_query()` which needs to submit bulk loads for the table, then the trigger retries.

**Category C — Clone/snapshot (background thread, sync disk read)**:
`ovsdb_clone_data()` and `ovsdb_to_txn_json()` run in `compaction_thread()` (background). They can iterate the disk store synchronously since they're off the main thread.

---

## Implementation Steps

### Step 1: Add `ovsdb_lazy_load_bulk_request()` to `ovsdb/lazy-load.c` + `.h`

New function that submits async load jobs for ALL unloaded rows in a table:

```c
/* Submit async load requests for all UNLOADED rows in 'table'.
 * Returns the number of load jobs submitted (0 means all already loaded).
 * Uses the row cache's UUID index to enumerate unloaded UUIDs. */
size_t ovsdb_lazy_load_bulk_request(struct ovsdb *db,
                                    struct ovsdb_table *table);
```

Implementation: iterate `table->cache` entries, for each with state `OVSDB_ROW_UNLOADED`, call `ovsdb_lazy_load_request()` and set state to `OVSDB_ROW_LOADING`.

This requires adding `ovsdb_row_cache_for_each_unloaded()` to `ovsdb/row-cache.c`:
```c
/* Calls 'cb' for each cache entry with state OVSDB_ROW_UNLOADED. */
void ovsdb_row_cache_for_each_unloaded(
    struct ovsdb_row_cache *cache,
    void (*cb)(const struct uuid *uuid, void *aux),
    void *aux);
```

### Step 2: Add deferred monitor state to `ovsdb/jsonrpc-server.c`

Extend `struct ovsdb_jsonrpc_monitor` (line 1348):
```c
struct ovsdb_jsonrpc_monitor {
    /* ... existing fields ... */
    struct json *deferred_request_id;  /* Non-NULL while loading initial data. */
    bool initial_loading;              /* True if bulk load in progress. */
    struct ovs_list deferred_node;     /* In session's deferred_monitors list. */
};
```

Add to session (or reuse existing structures):
```c
/* Track monitors waiting for initial data load. */
struct ovs_list deferred_monitors;  /* ovsdb_jsonrpc_monitor.deferred_node */
```

### Step 3: Modify `ovsdb_jsonrpc_monitor_create()` for deferred reply

In `ovsdb/jsonrpc-server.c`, around line 1604-1621:

```c
if (!m->change_set) {
    /* Check if any monitored table uses disk_store. */
    if (ovsdb_monitor_needs_bulk_load(m->dbmon)) {
        /* Submit bulk async loads for all monitored tables. */
        ovsdb_monitor_submit_bulk_load(m->dbmon);
        /* Defer the reply — will complete when rows are loaded. */
        m->deferred_request_id = json_clone(request_id);
        m->initial_loading = true;
        ovs_list_push_back(&s->deferred_monitors, &m->deferred_node);
        jsonrpc_msg_destroy(request);  /* Take ownership. */
        return NULL;  /* No reply yet. */
    }
    ovsdb_monitor_get_initial(m->dbmon, &m->change_set);
    initial = true;
}
```

When `ovsdb_jsonrpc_monitor_create()` returns NULL, `ovsdb_jsonrpc_session_got_request()` (line 1178) skips sending — the request is consumed.

### Step 4: Add monitor completion check to session run loop

In `ovsdb_jsonrpc_session_run()` (line 667), after `ovsdb_jsonrpc_trigger_complete_done(s)`:

```c
ovsdb_jsonrpc_trigger_complete_done(s);
ovsdb_jsonrpc_monitor_complete_deferred(s);  /* NEW */
```

New function `ovsdb_jsonrpc_monitor_complete_deferred()`:
```c
static void
ovsdb_jsonrpc_monitor_complete_deferred(struct ovsdb_jsonrpc_session *s)
{
    struct ovsdb_jsonrpc_monitor *m;
    LIST_FOR_EACH_SAFE (m, deferred_node, &s->deferred_monitors) {
        if (!m->initial_loading) {
            continue;
        }
        /* Check if all monitored tables are fully loaded. */
        if (ovsdb_monitor_all_rows_loaded(m->dbmon)) {
            /* All rows in cache — compose and send the initial snapshot. */
            ovsdb_monitor_get_initial(m->dbmon, &m->change_set);
            struct json *json = ovsdb_jsonrpc_monitor_compose_update(m, true);
            json = json ? json : json_object_create();
            /* ... handle V3 wrapper if needed ... */
            struct jsonrpc_msg *reply = jsonrpc_create_reply(
                json, m->deferred_request_id);
            ovsdb_jsonrpc_session_send(s, reply);
            json_destroy(m->deferred_request_id);
            m->deferred_request_id = NULL;
            m->initial_loading = false;
            ovs_list_remove(&m->deferred_node);
        }
    }
}
```

### Step 5: Add helper functions to `ovsdb/monitor.c`

```c
/* Returns true if any monitored table has a disk_store (needs bulk load). */
bool ovsdb_monitor_needs_bulk_load(const struct ovsdb_monitor *dbmon);

/* Submits bulk async load requests for all monitored tables. */
void ovsdb_monitor_submit_bulk_load(struct ovsdb_monitor *dbmon);

/* Returns true if all rows in monitored tables are CACHED (not UNLOADED/LOADING). */
bool ovsdb_monitor_all_rows_loaded(const struct ovsdb_monitor *dbmon);
```

`ovsdb_monitor_all_rows_loaded()` checks `ovsdb_row_cache_has_unloaded()` for each monitored table's cache.

### Step 6: Add `ovsdb_row_cache_has_unloaded()` to `ovsdb/row-cache.c`

```c
/* Returns true if the cache has any entries in UNLOADED or LOADING state. */
bool ovsdb_row_cache_has_unloaded(const struct ovsdb_row_cache *cache);
```

### Step 7: Fix `ovsdb_query()` linear scan in `ovsdb/query.c:45`

When the query does a linear scan on a table with `disk_store`, submit a bulk load and return an error that triggers parking:

```c
if (table->disk_store && table->cache
    && ovsdb_row_cache_has_unloaded(table->cache)) {
    /* Trigger bulk load. The trigger will park and retry. */
    ovsdb_lazy_load_bulk_request(table->db, table);
    /* Return empty results — the trigger will see "no rows"
     * and park via waiting_for_data. */
}
```

The existing trigger parking in `trigger.c` (lines 251-262) handles the retry.

### Step 8: Fix `ovsdb_to_txn_json()` and `ovsdb_clone_data()` for background threads

These run in `compaction_thread()` (background thread). Add a table-level disk store iterator:

In `ovsdb/table.h` + `ovsdb/table.c`:
```c
/* Iterate all rows. If table has a disk_store, reads directly from disk
 * (synchronous — intended for background threads only).
 * If no disk_store, iterates table->rows hmap. */
typedef bool (*ovsdb_table_row_cb)(const struct ovsdb_row *row, void *aux);
void ovsdb_table_for_each_row(const struct ovsdb_table *table,
                              ovsdb_table_row_cb cb, void *aux);
```

This is ONLY used by background thread operations (clone, snapshot) where blocking is acceptable. Main-thread code uses the async path.

Update `ovsdb_to_txn_json()` in `ovsdb/file.c` and `ovsdb_clone_data()` in `ovsdb/ovsdb.c` to use `ovsdb_table_for_each_row()` instead of `HMAP_FOR_EACH`.

---

## Files to Modify

| File | Change |
|------|--------|
| `ovsdb/lazy-load.h` | Declare `ovsdb_lazy_load_bulk_request()` |
| `ovsdb/lazy-load.c` | Implement bulk load (iterate unloaded cache entries) |
| `ovsdb/row-cache.h` | Declare `ovsdb_row_cache_for_each_unloaded()`, `ovsdb_row_cache_has_unloaded()` |
| `ovsdb/row-cache.c` | Implement cache iteration and has-unloaded check |
| `ovsdb/monitor.c` | Add `ovsdb_monitor_needs_bulk_load()`, `submit_bulk_load()`, `all_rows_loaded()` |
| `ovsdb/monitor.h` | Declare new monitor helpers |
| `ovsdb/jsonrpc-server.c` | Deferred monitor state, `monitor_complete_deferred()`, modify `monitor_create()` |
| `ovsdb/query.c` | Trigger bulk load before linear scan on disk-store tables |
| `ovsdb/table.h` | Declare `ovsdb_table_for_each_row()` callback + function |
| `ovsdb/table.c` | Implement `ovsdb_table_for_each_row()` with disk store cursor path |
| `ovsdb/file.c` | Use `ovsdb_table_for_each_row()` in `ovsdb_to_txn_json()`, `ovsdb_file_clone()` |
| `ovsdb/ovsdb.c` | Use `ovsdb_table_for_each_row()` in `ovsdb_clone_data()` |

---

## Test Plan

### T1: Unit Test — `test-lazy-load.c` (extend existing)

```
test-lazy-load bulk-request
  - Create disk store with 100 rows across 3 tables
  - Create row cache with all entries UNLOADED
  - Call ovsdb_lazy_load_bulk_request() for each table
  - Drain worker pool
  - Verify all entries now CACHED
  - Verify cache_count matches disk store count

test-lazy-load bulk-request-partial
  - Create disk store with 100 rows
  - Load 30 rows into cache (CACHED state)
  - Call ovsdb_lazy_load_bulk_request()
  - Verify only 70 load jobs submitted (not 100)
  - Drain pool — verify all 100 now CACHED

test-lazy-load bulk-request-no-pool
  - Do NOT initialize lazy-load subsystem
  - Call ovsdb_lazy_load_bulk_request()
  - Verify returns 0 (fallback — no jobs submitted)
```

### T2: Unit Test — `test-row-cache.c` (extend existing)

```
test-row-cache has-unloaded-true
  - Create cache, add UNLOADED entries
  - Verify ovsdb_row_cache_has_unloaded() returns true

test-row-cache has-unloaded-false
  - Create cache with only CACHED entries
  - Verify ovsdb_row_cache_has_unloaded() returns false

test-row-cache for-each-unloaded
  - Create cache with 50 UNLOADED + 50 CACHED entries
  - Call ovsdb_row_cache_for_each_unloaded()
  - Verify callback called exactly 50 times
  - Verify only UNLOADED UUIDs yielded
```

### T3: Unit Test — `test-table-iterator.c` (new)

```
test-table-for-each-row inmemory
  - Create table with 100 rows in table->rows hmap
  - Call ovsdb_table_for_each_row() with counting callback
  - Verify 100 rows iterated

test-table-for-each-row disk-store
  - Create table with disk_store containing 100 rows
  - table->rows hmap is empty
  - Call ovsdb_table_for_each_row()
  - Verify 100 rows iterated from disk store cursor
  - Verify rows are inserted into cache

test-table-for-each-row empty
  - Create table with no rows and no disk store
  - Call ovsdb_table_for_each_row()
  - Verify 0 rows iterated (no crash)
```

### T4: Autotest — `tests/ovsdb-deferred-monitor.at` (new)

```
AT_SETUP([deferred monitor - initial snapshot from binary DB])
  # Create OVN NB database with known data (switches, ports, router)
  # Convert to binary format via ovsdb-tool convert-format
  # Start ovsdb-server with --disk-store
  # Connect monitor client via ovsdb-client monitor
  # Verify initial snapshot contains all expected rows
  # Verify row counts match original JSON database
AT_CLEANUP

AT_SETUP([deferred monitor - server responsive during initial load])
  # Start ovsdb-server on large binary DB with --disk-store
  # Immediately send list-dbs request (should succeed fast)
  # Start monitor — verify initial snapshot eventually arrives
  # While monitor loading, send another list-dbs — must not hang
AT_CLEANUP

AT_SETUP([deferred monitor - multiple concurrent monitors])
  # Start ovsdb-server on binary DB
  # Connect 3 monitor clients simultaneously
  # Verify all 3 receive correct initial snapshots
  # Verify no duplicate load jobs (coalescing)
AT_CLEANUP

AT_SETUP([deferred monitor - incremental updates after initial])
  # Start server on binary DB, connect monitor
  # Wait for initial snapshot
  # Insert new row via ovsdb-client transact
  # Verify monitor receives incremental update
AT_CLEANUP
```

### T5: Autotest — `tests/ovsdb-binary-query.at` (new)

```
AT_SETUP([binary query - SELECT with conditions on disk-store table])
  # Create binary DB with known data
  # Start server with --disk-store
  # Run ovsdb-client transact with SELECT query (non-UUID condition)
  # Verify correct rows returned
AT_CLEANUP

AT_SETUP([binary query - UUID lookup on unloaded row])
  # Start server on binary DB
  # Query specific row by UUID via ovsdb-client transact
  # Verify row returned correctly
AT_CLEANUP

AT_SETUP([binary query - snapshot serialization includes disk rows])
  # Start server on binary DB with --disk-store
  # Insert enough data to trigger compaction
  # Wait for snapshot to complete
  # Stop server, restart from snapshot
  # Verify all rows present after restart
AT_CLEANUP
```

### T6: Autotest — `tests/ovsdb-binary-clone.at` (new)

```
AT_SETUP([binary clone - compaction includes disk-stored rows])
  # Create binary DB, start server with --disk-store
  # Insert data, trigger compaction (standalone snapshot)
  # Verify snapshot log contains all rows
  # Restart server from snapshot — verify data intact
AT_CLEANUP
```

### Test Execution Summary

| Test Category | Framework | File | Count |
|---------------|-----------|------|-------|
| Bulk load unit | C (extend test-lazy-load.c) | `tests/test-lazy-load.c` | 3 |
| Cache helpers unit | C (extend test-row-cache.c) | `tests/test-row-cache.c` | 3 |
| Table iterator unit | C (new) | `tests/test-table-iterator.c` | 3 |
| Deferred monitor integration | Autotest (new) | `tests/ovsdb-deferred-monitor.at` | 4 |
| Binary query integration | Autotest (new) | `tests/ovsdb-binary-query.at` | 3 |
| Binary clone integration | Autotest (new) | `tests/ovsdb-binary-clone.at` | 1 |
| **TOTAL** | | | **17 tests** |

---

## Verification

1. **Build**: `make -j4` — must compile without warnings under `--enable-Werror`
2. **Existing tests**: `make check TESTSUITEFLAGS="-j4 -k ovsdb"` — all existing tests pass
3. **New tests**: `make check TESTSUITEFLAGS="-k deferred-monitor -k binary-query -k binary-clone"`
4. **Unit tests**: `ovstest test-lazy-load`, `ovstest test-row-cache`, `ovstest test-table-iterator`
5. **ASAN**: Run all tests with `-fsanitize=address` — no leaks/UAF
6. **TSAN**: Run deferred monitor tests with `-fsanitize=thread` — no data races
7. **Manual repro**:
   ```bash
   ovsdb-tool convert-format ovnnb_db.db binary
   ovsdb-server --disk-store --remote=punix:/tmp/ovnnb.sock ovnnb_db.db &
   ovn-nbctl --db=unix:/tmp/ovnnb.sock show
   # Must show: sw01, sw11, public1, lr1, ports, NAT rules
   ```

## Risks

| Risk | Mitigation |
|------|-----------|
| Bulk load floods worker pool with thousands of jobs | Batch submissions: load N rows per job (e.g., 100) instead of 1 per job |
| Client timeout waiting for deferred monitor | Add timeout to deferred state; if exceeded, send partial snapshot with available rows |
| Memory spike during bulk load (all rows temporarily in cache) | After sending initial snapshot, unpin/allow eviction of loaded rows |
| Race between bulk load completion and monitor destroy | Check monitor still valid in completion callback; use refcount or generation counter |
| `ovsdb_table_for_each_row()` called from main thread accidentally | Document as "background thread only"; assert not on main thread in debug builds |
