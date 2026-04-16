# Fix: Serve every row from disk-store on demand, honoring the cache budget

## Context

With `--disk-store` against a pre-existing binary (`binaryv1`) database, `ovsdb-client` and `ovn-nbctl` cannot find rows that already exist on disk. Newly inserted rows are found immediately. The startup log shows `"opened in binary disk store mode"`, so `ovsdb_attach_disk_store()` (ovsdb/ovsdb.c:761-792) runs and `ovsdb_disk_store_for_each_uuid()` populates `table->cache` with UNLOADED entries — the uuid→offset index is built at startup.

Two separate gaps explain the user-visible symptom:

1. **Bulk-iteration serve paths skip uncached rows.** `ovsdb_monitor_get_initial()` (monitor.c:1585-1615, line 1605), `ovsdb_monitor_compose_cond_change_update()` (monitor.c:1270), and `ovsdb_query()` linear scan (query.c:77) all use `ovsdb_table_for_each_loaded_row()` (table.c:522), which yields only `table->rows` + CACHED entries. UNLOADED/LOADING entries are silently skipped — hence empty `ovn-nbctl show`, empty `ovsdb-client dump`, and empty non-UUID `select`.
2. **Deferred-monitor guard falls through to the broken iterator.** `ovsdb_jsonrpc_monitor_create()` (jsonrpc-server.c:1619-1637) defers only when `needs_bulk_load && submit_bulk_load() > 0`. When a second subscriber arrives during in-flight warm-up, every entry is LOADING (none UNLOADED), `submit_bulk_load()` returns 0, and we fall through to the empty-yielding path.

New inserts hit `table->rows`, so "create a new resource" always works.

A third architectural limitation exists but is out of scope for correctness in this change: the disk-store file has a uuid→offset index only. Any non-UUID lookup (e.g., `where name == "foo"`) on an UNLOADED row must cursor-scan the full table. Design sketch for a follow-up change is in Part E; in this change, we just make the scan correct.

## Design intent

1. **Correctness**: every JSON-RPC read path returns complete data for every row, regardless of cache residency. `ovsdb-client dump <table>` must show the full table, always.
2. **Budget honest**: cache size never exceeds `OVSDB_CACHE_MAX_ATOMS`. When reading rows from disk, standard LRU eviction is fine — the iterator uses a transient-pointer contract, so evicting a previously-yielded row (already consumed by the callback) is safe.
3. **Async-first**: the worker-pool deferred path stays the preferred route for large monitor subscriptions. Synchronous disk reads on the main thread are a fallback when there's no pool, not the default.

## Fix

### Part A — Correctness: sync-capable "every row" iterator (ovsdb/table.c, ovsdb/table.h)

Introduce one new iterator (working name `ovsdb_table_for_each_row_from_disk`):

```c
typedef bool (*ovsdb_table_row_cb)(const struct ovsdb_row *row, void *aux);

/* Invokes 'cb' once per row logically present in 'table'.
 *
 * Row pointers passed to 'cb' are TRANSIENT: valid only during the
 * call.  Callers must not retain the pointer past return.
 *
 * For disk-store backed tables, rows not already in 'table->rows'
 * are read from disk via the disk-store cursor.  Each row read from
 * disk is inserted into 'table->cache' (standard LRU eviction if the
 * cache is over budget — evicting previously-yielded rows is safe
 * because the callback already consumed them).  When an insertion
 * would evict the row we're about to yield (only possible if a
 * single row's atoms exceed max_atoms), fall back to yielding the
 * transient pointer without inserting.
 *
 * For in-memory tables, behaves like HMAP_FOR_EACH over table->rows.
 *
 * Intended for main-thread serve paths: monitor initial snapshot,
 * non-UUID query linear scan, cond-change update. */
void ovsdb_table_for_each_row_from_disk(
    const struct ovsdb_table *table,
    ovsdb_table_row_cb cb, void *aux);
```

Algorithm:

1. Walk `HMAP_FOR_EACH (row, hmap_node, &table->rows)` → `cb(row)`. Stable pointers, first yield.
2. If `!table->disk_store`, return.
3. Open disk-store cursor (reuse `ovsdb_disk_store_cursor_open/next/close` already used in `ovsdb_table_for_each_row()` at table.c:438-471).
4. For each cursor row:
   a. If its UUID is in `table->rows` (already yielded in step 1), destroy and continue.
   b. If its UUID is CACHED, destroy the transient disk row and yield the cached pointer.
   c. Otherwise, `ovsdb_row_cache_insert()` the disk row. Standard LRU eviction is fine. Yield the now-cached stable pointer. Transition state to CACHED via the path `row_load_done()` already uses at lazy-load.c:99-121.
5. Close the cursor.

Callback contract enforcement:
- `monitor_initial_row_cb` (monitor.c:1577-1583) calls `ovsdb_monitor_changes_update()` which copies data — safe, transient is fine.
- `query_scan_cb` in query.c — audit during implementation to confirm it does not retain the pointer. If it does, either fix it or pin while yielding.
- Add an explicit doc comment on the new iterator mandating the transient contract.

We deliberately do **not** change `ovsdb_table_for_each_row()` (table.c:438 — background-thread-only, used by `ovsdb_clone_data()` and `ovsdb_to_txn_json()`), nor `ovsdb_table_for_each_loaded_row()` (table.c:522 — still valid for callers who want only cache+hmap).

### Part B — Swap the iterator at the monitor serve sites

- `ovsdb/monitor.c:1605` — `ovsdb_monitor_get_initial`. The callback `monitor_initial_row_cb` feeds `ovsdb_monitor_changes_update()` (monitor.c:1418-1489), which deep-clones row data via `clone_monitor_row_data()` — does not retain the transient pointer, safe to swap.
- `ovsdb/monitor.c:1270` — `ovsdb_monitor_compose_cond_change_update`. The callback `cond_change_row_cb` (monitor.c:1206-1224) produces JSON and takes `ovsdb_row_get_uuid()` by value — does not retain, safe to swap.
- `ovsdb/query.c:77` — **NOT swapped in this change.** `query_row_set_cb` (query.c:91-97) retains pointers via `ovsdb_row_set_add_row()`, and `query_distinct_cb` (query.c:107-113) retains via `ovsdb_row_hash_insert()`. A transient iterator that triggers LRU eviction during iteration would leave those retained pointers dangling once the cache overflows. For DBs that fit in the cache, Part D's warm-up covers non-UUID queries via the existing `for_each_loaded_row()` call at query.c:77 (rows stay CACHED, `for_each_loaded_row` yields them all, retention is safe). For DBs larger than `OVSDB_CACHE_MAX_ATOMS`, non-UUID query completeness requires either a query-owned row-set that clones rows or an inline row→JSON streaming path. See Risks.

### Part C — Deferred-monitor guard fix (ovsdb/jsonrpc-server.c:1619-1637)

Change the guard from "defer iff `submit_bulk_load() > 0`" to "defer iff `needs_bulk_load` **and** a worker pool is available." When there is work to do and a pool, the async path is always preferable. When there is no pool, fall through to the inline call — which is now correct because Part A's iterator sync-reads from disk.

Pseudocode:

```c
if (ovsdb_monitor_needs_bulk_load(m->dbmon)
    && ovsdb_lazy_load_pool_available()) {
    ovsdb_monitor_submit_bulk_load(m->dbmon);  /* idempotent */
    m->deferred_request_id = json_clone(request_id);
    m->initial_loading = true;
    ovs_list_push_back(&s->deferred_monitors, &m->deferred_node);
    return NULL;
}
ovsdb_monitor_get_initial(m->dbmon, &m->change_set);
```

`ovsdb_lazy_load_pool_available()` is a new tiny accessor in lazy-load.c that returns whether the global `lazy_pool` is non-NULL. Update the stale comment at jsonrpc-server.c:1622-1626.

### Part D — Background warm-up at startup, stopped at `MAX_ATOMS` (ovsdb/ovsdb.c, ovsdb/lazy-load.c)

Inside `ovsdb_attach_disk_store()` at ovsdb.c:761-792, after UNLOADED population, submit async loads via a new bounded variant that stops once the cache is full. This avoids load-and-evict thrash on DBs larger than `OVSDB_CACHE_MAX_ATOMS`.

```c
if (ovsdb_lazy_load_pool_available()) {
    /* Warm up until the cache is full.  For DBs that fit, this ends
     * with the cache fully populated.  For DBs that exceed the atom
     * budget, we stop submitting the moment we'd be about to thrash
     * — remaining rows stay UNLOADED and are read on demand via
     * Part A's iterator.  Runs entirely on the lazy-load worker
     * pool; the main thread is never blocked. */
    size_t submitted = ovsdb_lazy_load_bulk_request_until_full(db, table);
    VLOG_DBG("%s: table %s: warm-up submitted %"PRIuSIZE" jobs",
             db->name, node->name, submitted);
}
```

New helper in `ovsdb/lazy-load.c` next to `ovsdb_lazy_load_bulk_request()` at lines 202-220:

```c
/* Submit async load requests for UNLOADED rows in 'table' until
 * doing so would exceed the cache's atom budget.  Returns the
 * number of submissions.  Uses a rolling estimate of remaining
 * room, accounting for submitted-but-not-yet-loaded jobs, to keep
 * the submission stream honest about the projected cache size. */
size_t ovsdb_lazy_load_bulk_request_until_full(
    struct ovsdb *db, struct ovsdb_table *table);
```

Implementation sketch — decide during coding:

- Walk the UNLOADED entries via `ovsdb_row_cache_for_each_unloaded()` (row-cache.c:392-405).
- Maintain a `size_t projected = cache->total_atoms` counter seeded from current cache occupancy.
- For each entry, estimate its atom cost. The disk-store record header's `n_columns` field (disk-store.c:73-81) is available from the in-memory disk-store index — use `n_columns × AVG_ATOMS_PER_COLUMN` (constant picked from measurement; 1 or 2 is a reasonable starting point) as the projection.
- If `projected + estimate > cache->max_atoms`, stop.
- Otherwise submit via `ovsdb_lazy_load_request()`, add the estimate to `projected`, continue.

The estimate may miss — but LRU eviction on the actual insert in `row_load_done()` still caps memory at the real budget. The estimate only matters for where to stop submitting; being slightly off just means we submit one or two jobs more or less than optimal.

If no worker pool is available, skip warm-up. Part A's iterator sync-reads on the first request. A dedicated warm-up thread for the no-pool case is out of scope (no-pool is an exceptional / test-only path).

### Part E — Secondary indexes for non-UUID lookups (design sketch; deferred implementation)

**Problem.** The disk-store in-memory index (`struct disk_store_index_entry`, disk-store.c:84-91) is `uuid → (offset, length)`. A query like `where name == "foo"` must cursor-scan every record in the table, parse each to extract the `name` column, and compare. That's O(N) disk I/O per lookup, even when the schema declares `name` as a unique index. For OVN NB with ~1000 `Logical_Switch` rows this is acceptable; for a 10⁶-row deployment it is not.

OVSDB schemas already declare indexed column sets: `struct ovsdb_table_schema::indexes[]` / `n_indexes` (table.h:29, populated in table.c:200-245) and the matching runtime hmaps `table->indexes[i]` (table.c:306-308) used today by transaction commits to enforce uniqueness. In disk-store mode these runtime hmaps are empty at startup because `table->rows` is empty — exactly the same gap as the row cache.

**Options evaluated.**

| Option | What | Memory cost | Startup cost | Write cost | Crash recovery | Complexity |
|--------|------|-------------|--------------|------------|----------------|------------|
| A. Eager in-memory secondary index at attach | One full cursor pass per table at startup: parse indexed columns only, populate `table->indexes[i]` with `key → uuid` entries for every row. | O(rows × indexes × avg_key_size). For OVN NB: ~tens of MB per million rows per index. | One full scan per table at startup (same I/O as warm-up, less CPU since we don't deserialize full rows). | None — updates go through existing transaction-commit hmap-update path. | Rebuilt from disk on every start; always consistent. | Low. Fits the existing `table->indexes[]` model. |
| B. Persistent on-disk secondary index files | Separate `db.idx.<table>.<index>` files (sorted array or hash on disk). | O(1) RAM per file when using mmap + binary search, or O(rows) if fully loaded. | None at startup (loaded lazily). | Every transaction must update every index file. Significant write amplification, extra fsync. | Each index file is a persistent structure that must be crash-safe — checksums, journaling, or a full rebuild-on-open fallback. | High. New format, new recovery path, new concurrency discipline. |
| C. Lazy in-memory secondary index on first use | On the first query that touches an indexed column, do one full scan, populate `table->indexes[i]`, then answer the query. Subsequent queries are O(1). | Same as A. | Zero at startup; amortized to first query. | Same as A (no write-path change). | Rebuilt on every start; always consistent. | Low. Cleaner startup. |
| D. Disk-resident B+ tree per index | One B+ tree file per declared index. | Working set only (page-cached). | None at startup. | Every write is a B+ tree insert — O(log N) disk I/O, node splits, page rewrites. | Full journal or WAL required. | Very high. |

**Data-structure note on B+ trees vs hashes.** For UUID lookups, hash tables are strictly better: UUIDs are already uniformly distributed so they have no range-query benefit from a tree, and the constant factors on hashes beat B+ tree node traversals. For declared indexes on strings / integers, OVSDB does not support range queries (`>`, `<=`, etc.) on index columns — the condition parser in condition.c only evaluates equality against schema indexes for fast paths. So a tree's range capability is wasted. Hash tables (or sorted arrays for tiny data where cache locality wins) are the right fit.

**Recommended direction (follow-up change, not this one).**

Option C — lazy in-memory index build on first indexed query — as the primary path. Rationale:

- Fits the existing `table->indexes[]` hmap abstraction; no new format on disk.
- Zero startup cost for deployments that never issue indexed queries.
- Memory is bounded by the schema (declared indexes only), not by the cache budget. For OVN-scale schemas this is tens of MB even on large DBs.
- Write path is unchanged: transaction commits keep updating `table->indexes[i]` as they do today.
- Concurrency story is simple: the first indexed query blocks (or trigger-parks) while the index is being built; subsequent queries hit it.

Out-of-scope for this change: implementing any of A–D. This section exists so the follow-up is designed against the same constraints (cache budget, lazy-first, async worker pool) as the rest of the disk-store work.

### Critical files to modify (Parts A–D only)

| File | Change |
|------|--------|
| `ovsdb/table.h` | Declare `ovsdb_table_for_each_row_from_disk`. |
| `ovsdb/table.c` | Implement the new iterator using the existing cursor pattern at lines 438-471; enforce transient-row contract in the header comment. |
| `ovsdb/monitor.c` | Swap iterator at lines 1605 (initial) and 1270 (cond-change). |
| `ovsdb/query.c` | Swap iterator at line 77. |
| `ovsdb/jsonrpc-server.c` | Rework deferral guard at 1619-1637 to condition on pool availability; update comment at 1622-1626. |
| `ovsdb/ovsdb.c` | Bounded warm-up inside `ovsdb_attach_disk_store()` at 761-792 via `ovsdb_lazy_load_bulk_request_until_full()`; extend startup `VLOG_DBG` at 787-790 with submitted count. |
| `ovsdb/lazy-load.c` + `.h` | Add `ovsdb_lazy_load_bulk_request_until_full()` (budget-aware submission stopping at `cache->max_atoms`) and `ovsdb_lazy_load_pool_available()` (trivial accessor for whether `lazy_pool` is non-NULL). |

### Utilities reused, not duplicated

- `ovsdb_disk_store_cursor_open/next/close()` — disk-store.c; already used by `ovsdb_table_for_each_row()` at table.c:438-471.
- `ovsdb_row_cache_lookup()` / `ovsdb_row_cache_insert()` / `ovsdb_row_cache_has_unloaded()` — row-cache.c; same API `row_load_done()` uses at lazy-load.c:99-121.
- `ovsdb_row_count_atoms()` — row.c; already used by cache insertion.
- `ovsdb_lazy_load_bulk_request()` — lazy-load.c:202-220; sibling API; new `_until_full()` variant shares the `bulk_load_cb` / `ovsdb_lazy_load_request()` plumbing.
- `ovsdb_row_cache_for_each_unloaded()` — row-cache.c:392-405; walked by the new bounded submission helper.
- `ovsdb_monitor_needs_bulk_load` / `submit_bulk_load` / `all_rows_loaded` — monitor.c:1620-1672.

## Tests

### T1: Autotest — restart + UUID lookup (`tests/ovsdb-binary-serve.at`)

Create schema, insert row with known UUID, `convert-format binary`, start/stop/restart with `--disk-store`, `ovsdb-client transact '[...select where uuid == X...]'` returns the row.

### T2: Autotest — restart + non-UUID (name) lookup

Same setup; `where ["name","==","foo"]` returns the pre-existing row. Exercises the query path.

### T3: Autotest — restart + `ovsdb-client dump <table>` shows full table

Same setup with ≥3 pre-existing rows. After restart, `ovsdb-client dump unix:sock db table` must print all rows. Direct coverage of the user-named canonical command.

### T4: Autotest — restart + monitor initial snapshot

Same setup; `ovsdb-client monitor` after restart yields initial reply containing all N pre-existing rows. Exact `ovn-nbctl show` scenario.

### T5: Autotest — DB larger than cache budget

Expose a test-only knob for `OVSDB_CACHE_MAX_ATOMS` (e.g., a hidden `--cache-max-atoms` flag on ovsdb-server, or a `setenv OVS_OVSDB_CACHE_MAX_ATOMS=…` read in ovsdb-server.c). Insert rows whose total atoms exceed the budget. After restart:
- `ovsdb-client dump` returns **every** row.
- Debug appctl (or `VLOG_DBG` counter) shows `cache->total_atoms <= max_atoms` at all times.
This is the crucial correctness test; previous plan drafts missed it.

### T6: Autotest — concurrent monitor during in-flight warm-up

Populate a DB large enough that warm-up takes >1 main-loop tick. Subscribe monitor A then B back-to-back post-startup. Both snapshots must include every row. Exercises Part C's guard change.

### T7: Autotest — no worker pool

If the lazy-load pool can be disabled (locate the init path in lazy-load.c and add a test knob if needed), start server with pool disabled and run T3 + T4 — monitors and dumps must still return all rows via Part A's sync read.

### T8: Autotest — warm-up runs on worker pool, main thread stays responsive, bounded by `MAX_ATOMS`

Two assertions:

1. **Responsiveness**: start server on a moderately large binary DB with pool enabled. Immediately after startup (before warm-up can possibly complete) issue a small `list-dbs` or schema request — it must return promptly, proving the main thread isn't blocked by warm-up.
2. **No thrash**: start server on a DB whose total atoms exceed `OVSDB_CACHE_MAX_ATOMS` (reuse T5's knob). Under `-vvlog:dbg`, the count of warm-up submissions per table is strictly less than the total UNLOADED count — the bounded helper stopped once projected fullness hit the budget. Cache `total_atoms` at steady state is ≤ `max_atoms`.

### T9: C unit tests — iterator semantics (`tests/test-ovsdb.c` or new file)

- 50 disk rows, empty cache: yields 50, cache fills up to `max_atoms`, any overflow uses transient yielding.
- 30 in `table->rows` + 20 on disk (disjoint UUIDs): yields 50, no duplicates.
- 30 in `table->rows` overlapping 20 on disk: yields 30 (hmap wins).
- Rows larger than `max_atoms` each: every row still yielded; cache either holds the one most recent or stays below budget.
- Callback that returns `false` early: iteration stops cleanly; cursor closed, no leaks.

### T10: ASAN & TSAN

- ASAN on T1–T9: no leaks from cursor path, cache inserts, or early-termination paths.
- TSAN on T6: no races between warm-up workers and the main-thread iterator.

## Verification

1. **Build**: `./boot.sh && ./configure --enable-Werror CFLAGS="-g -O2" && make -j4`.
2. **No regression**: `make check TESTSUITEFLAGS="-j4 -k ovsdb"`.
3. **Focused**: `make check TESTSUITEFLAGS="-k binary-serve"` — existing + T1–T9 green.
4. **Manual repro on the exact failing workflow**:
   ```bash
   ovsdb-tool convert-format ovnnb_db.db binary
   ovsdb-server --disk-store --detach --no-chdir --pidfile \
     --remote=punix:/tmp/ovnnb.sock ovnnb_db.db
   ovn-nbctl --db=unix:/tmp/ovnnb.sock show      # all LS/LR/ports
   ovsdb-client dump unix:/tmp/ovnnb.sock         # every row
   ovsdb-client transact unix:/tmp/ovnnb.sock \
     '["OVN_Northbound",{"op":"select","table":"Logical_Switch",
        "where":[["name","==","sw01"]]}]'         # finds pre-existing LS
   ovs-appctl -t ovsdb-server exit
   ```
5. **Large-DB correctness**: repeat with a DB whose row atoms exceed `OVSDB_CACHE_MAX_ATOMS`. All rows still served by `dump`; cache stays within budget (check via debug appctl or log).

## Risks & notes

- **Main-thread sync I/O on no-pool path**: Part C's fallback into the Part A iterator will synchronously scan the disk store for a large DB when no worker pool is available. Acceptable per the design intent; documented in the iterator header. If this stall becomes a problem in production, the next step is a trigger-style park on monitor/query creation — a larger change and separate follow-up.
- **`ovsdb_row_count_atoms()` cost**: cache insertion needs atom counts. Confirm during implementation that the function is O(row_size) and acceptable inside the iterator; if not, thread the count through the cursor path so it's computed during deserialization.
- **Callback-retention audit**: transient-pointer contract requires that `monitor_initial_row_cb`, `query_scan_cb`, and the cond-change callback (monitor.c:1249-1268) do not retain yielded pointers. Audit and, if any do, copy into stable storage before the callback returns or switch that specific call site to a different strategy.
- **Non-UUID lookups still O(N) without Part E**: the exact scenario the user called out. Part A makes the scan correct; Part E (secondary indexes) makes it fast. Landing only A–D is a correctness fix for this change; Part E is a pre-designed follow-up.
- **Test harness for budget enforcement**: T5 needs a way to exceed `OVSDB_CACHE_MAX_ATOMS` in test. Decide between (a) a hidden `--cache-max-atoms` flag for test/debug builds, (b) an env-var read in ovsdb-server.c, or (c) compile-time override. Pick the lowest-risk option and keep it out of production surface.
