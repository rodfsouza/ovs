# Plan: Materialized JSON Snapshot + Worker Pool + Cache Fix

## Context

When northd initializes or any client requests an unconditioned full-database monitor, `ovsdb-server`'s main thread blocks on three expensive operations: full table scan, change-set construction (datum cloning), and JSON composition + serialization. For a 100K-row database this can stall the main loop for seconds.

**Root cause:** The JSON representation clients need is built **on-demand** at monitor-connect time, every time. This is wasteful — we already walk the data at startup to build indexes, bloom filters, and UUID→offset mappings.

**New approach:** Build a **materialized JSON snapshot** during startup (piggybacking on the existing data walk), persist it on disk alongside the database, and serve it directly to clients. A dirty counter triggers background rebuilds when enough changes accumulate.

---

## Phase 1: `--num-workers` CLI Parameter

**Files:** `ovsdb/ovsdb-server.c`

1. Add `OPT_NUM_WORKERS` to the option enum (after `OPT_CACHE_MAX_ATOMS`, ~line 2678)
2. Add `{"num-workers", required_argument, NULL, OPT_NUM_WORKERS}` to `long_options[]` (~line 2708)
3. Add case handler in `parse_options()` — parse with `str_to_ullong()`, validate range 1..64, store in `static size_t n_io_workers = OVSDB_IO_WORKER_THREADS`
4. Replace `OVSDB_IO_WORKER_THREADS` in `ovsdb_worker_pool_create()` call (line 835) with `n_io_workers`
5. Add unixctl `ovsdb-server/get-num-workers` for runtime query
6. Update `usage()` to document the new flag

**Verification:** Start with `--num-workers=2` and `--num-workers=8`, confirm pool creation log and unixctl output.

---

## Phase 2: Fix Row Cache Not Shrinking

**Files:** `ovsdb/row-cache.c`

### Problem A: `entries` hmap never shrinks

After burst mode (startup warm-up or unconditioned monitor scan via `ovsdb_table_query`), the cache may insert 100K entries then evict 90K. The `total_atoms` and `n_entries` counters are correct, but `hmap_shrink()` is **never called** on `cache->entries`. The bucket array stays sized for peak occupancy.

**Fix:** After eviction in `ovsdb_row_cache_evict__()` (line 432, after `compact__`), add:

```c
if (cache->n_entries < hmap_count(&cache->entries) / 4) {
    hmap_shrink(&cache->entries);
}
```

Also add the same check in `sweep_deferred__()` after draining deferred entries, since multiple entries are freed in batch there.

### Problem B: Burst mode resets sweeper progress

Every unconditioned monitor scan calls `enter_burst()` (`table.c:702`) which resets `max_atoms = high_water_atoms` (line 935). This undoes any gradual shrinking the sweeper accomplished. If clients reconnect frequently, the cache stays at high-water permanently.

**Fix:** Track whether the burst is from startup vs. query. For query-driven bursts (monitor scans), use a **capped burst** that doesn't exceed `2 * base_max_atoms` instead of `high_water_atoms`. Add `ovsdb_row_cache_enter_query_burst()` in `row-cache.c` with the lower cap. Update `ovsdb_table_query()` (`table.c:702`) to call the query variant.

### Problem C: Monitor scans pollute the hot cache

Unconditioned scans via `ovsdb_table_query()` use burst mode and insert every row into the **main clock buffer**. This evicts the hot working set that transactions need.

**Fix:** This is addressed by Phase 3 — once monitor requests are served from the materialized JSON snapshot, `ovsdb_table_query()` is no longer called for unconditioned initial snapshots. The cache stays clean.

**Verification:** Add a test that creates a large DB, runs a monitor scan, then checks that cache `n_entries` and hmap bucket count stabilize at `base_max_atoms`-proportional levels after the sweeper runs.

---

## Phase 3: Materialized JSON Snapshot — On-Disk Sidecar

**New files:** `ovsdb/json-snapshot.h`, `ovsdb/json-snapshot.c`, `ovsdb/automake.mk` update

### 3a. Snapshot file format

Sidecar file: `<dbname>.json-snapshot` (e.g., `OVN_Northbound.json-snapshot`)

```
[Header: 32 bytes]
  magic:        8 bytes  "OVSJSNAP"
  version:      4 bytes  (format version, initially 1)
  txn_id:       16 bytes (UUID of last transaction included)
  n_tables:     4 bytes

[Per-table directory: variable]
  table_name:   2-byte len + UTF-8
  offset:       8 bytes  (offset into file where table JSON starts)
  length:       8 bytes  (byte length of table JSON)
  n_rows:       4 bytes

[Table JSON blocks: variable]
  For each table: pre-serialized JSON bytes for the <table-updates2> object
  containing all rows in monitor_cond_since V2 "initial" format.
```

The format stores **pre-serialized JSON bytes** (not parsed JSON trees). Each table's block is independently readable — a conditioned monitor that only needs 2 tables reads only those 2 blocks.

### 3b. Snapshot builder (worker thread)

```c
struct ovsdb_json_snapshot_builder;

/* Create a builder that will write to 'path'.
 * Reads row data directly from disk-store, bypassing the row cache. */
struct ovsdb_json_snapshot_builder *
ovsdb_json_snapshot_build_start(const char *path,
                                struct ovsdb_disk_store *ds,
                                const struct ovsdb_schema *schema,
                                const struct uuid *txn_id);

/* Worker function: iterates all rows for all tables, converts to JSON,
 * writes to the sidecar file.  Returns NULL on success. */
void *ovsdb_json_snapshot_build_fn(void *builder);

/* Main-thread completion callback. */
void ovsdb_json_snapshot_build_done(void *result, void *aux);
```

**Key design: Bypasses the row cache entirely.** The builder reads raw rows from disk-store via `ovsdb_disk_store_read_row()` (uses `pread()` + deserialize), converts each to JSON via `ovsdb_datum_to_json()`, serializes to bytes, and writes directly to the sidecar file. No cache lookup, no cache insertion, no cache pollution.

For in-memory databases (no disk-store), the builder takes a **snapshot of the rows hmap** (copy the `ovsdb_row *` pointers under a brief lock) and then iterates the snapshot on the worker. Row data is immutable once committed (copy-on-write during transactions), so pointer reads are safe.

### 3c. Startup integration — Piggyback on existing walk

In `ovsdb_attach_disk_store()` (`ovsdb.c:768-848`), after Pass 4 (warm-up), add Pass 5:

```c
/* Pass 5: Submit JSON snapshot build to worker pool.
 * Reads directly from disk-store — does not go through cache.
 * Runs concurrently with warm-up I/O. */
if (ovsdb_lazy_load_pool_available()) {
    ovsdb_json_snapshot_submit_build(db, io_worker_pool);
}
```

This submits a single job to the worker pool. The builder iterates the disk-store index (already in memory from Pass 1) and `pread()`s each row's data. The sidecar file is written atomically (write to `.tmp`, then `rename()`).

If a valid sidecar file already exists on disk with a `txn_id` matching the current database state, **skip the build** — just reuse it. This makes restarts near-instant.

### 3d. Snapshot reader (for serving monitors)

```c
/* Read the pre-built snapshot for a set of tables.
 * Returns a pre-serialized JSON object ready to send as monitor reply.
 * Reads directly from the sidecar file — no cache involvement. */
struct json *ovsdb_json_snapshot_read(const char *path,
                                      const char **table_names,
                                      size_t n_tables);
```

This does `pread()` of the relevant table blocks from the sidecar file and wraps them in a `json_serialized_object` (pre-serialized, no parsing). The result is ready to send as a JSON-RPC reply.

For conditioned monitors, the snapshot is not used — they fall through to the existing compose path (which is fast since they're selective).

### 3e. Dirty counter and background rebuild

Add to `struct ovsdb` (`ovsdb.h`):

```c
size_t json_snapshot_dirty_count;    /* Rows changed since last snapshot. */
size_t json_snapshot_rebuild_threshold; /* Trigger rebuild at this count. */
bool json_snapshot_building;         /* True while worker is building. */
struct uuid json_snapshot_txn_id;    /* txn_id of current snapshot. */
```

In `ovsdb_trigger_run()` (or transaction commit path), after a successful commit:

```c
db->json_snapshot_dirty_count += n_changed_rows;
if (db->json_snapshot_dirty_count >= db->json_snapshot_rebuild_threshold
    && !db->json_snapshot_building) {
    ovsdb_json_snapshot_submit_build(db, io_worker_pool);
    db->json_snapshot_building = true;
}
```

The done callback resets `dirty_count = 0` and `building = false`.

Default threshold: 1000 rows (configurable via unixctl `ovsdb-server/set-snapshot-threshold`).

---

## Phase 4: Serve Monitors from Snapshot

**Files:** `ovsdb/jsonrpc-server.c`, `ovsdb/monitor.c`

### 4a. Modify `ovsdb_jsonrpc_monitor_create()` (~line 1620-1650)

**Current flow:**
```
if (needs_bulk_load && pool_available && !conditional) → defer disk load
else → get_initial_conditioned() + compose_update() inline (BLOCKS)
```

**New flow:**
```
if (!conditional && ovsdb_json_snapshot_available(db)) {
    /* Snapshot ready — serve directly from sidecar file */
    json = ovsdb_json_snapshot_read(path, monitored_tables, n_tables);
    /* json is pre-serialized, no composition needed */
    → send reply immediately
}
else if (!conditional && db->json_snapshot_building) {
    /* Snapshot building in progress — defer reply */
    m->deferred_request_id = json_clone(request_id);
    m->waiting_for_snapshot = true;
    list_push_back(&s->deferred_monitors, &m->deferred_node);
    return NULL;
}
else if (needs_bulk_load && pool_available && !conditional) {
    → existing defer disk load path
}
else {
    → existing inline path (conditioned monitors, small tables)
}
```

### 4b. Extend `ovsdb_jsonrpc_monitor_complete_deferred()` 

Add handling for `waiting_for_snapshot`:

```c
if (m->waiting_for_snapshot) {
    if (!ovsdb_json_snapshot_available(m->db)) {
        continue;  /* Still building */
    }
    /* Snapshot ready — read and send */
    json = ovsdb_json_snapshot_read(path, monitored_tables, n_tables);
    reply = jsonrpc_create_reply(json, m->deferred_request_id);
    session_send(s, reply);
    /* cleanup... */
}
```

### 4c. Stale snapshot handling

When a monitor is served from a snapshot whose `txn_id` is older than the current DB state, the monitor also needs the delta (changes between snapshot txn_id and now). Two options:

**Option A (simpler):** Only serve from snapshot if `snapshot_txn_id == db->current_txn_id`. If stale, fall through to the existing compose path. The dirty counter ensures rebuilds happen before staleness gets too large.

**Option B (better):** Serve the snapshot as the initial reply, then immediately send an `update` notification with changes since `snapshot_txn_id`. This leverages the existing change-set tracking (the monitor already tracks changes per txn_id).

**Chosen: Option A for Phase 4, Option B as future optimization.** Option A is safe and simple — if the snapshot is even slightly stale, we compose inline. With a threshold of 1000 rows, the snapshot stays fresh for most reconnect scenarios.

### 4d. Add `waiting_for_snapshot` field to `struct ovsdb_jsonrpc_monitor`

```c
bool waiting_for_snapshot;  /* True while waiting for snapshot build. */
```

---

## Phase 5: Remove Cache Pollution from Monitor Path

**Files:** `ovsdb/table.c`, `ovsdb/monitor.c`

Once Phase 4 is active, unconditioned monitors no longer call `ovsdb_table_query()` (they use the snapshot). But conditioned monitors still go through `ovsdb_table_query()`. Ensure the conditioned path uses `bulk_read` mode (scan ring, not burst) — this is already the case (`table.c:705-706`).

Remove the `enter_burst()` call for unconditioned scans in `ovsdb_table_query()` (`table.c:701-703`). Since unconditioned monitors now use the snapshot, any remaining caller of `ovsdb_table_query(table, NULL, ...)` should use bulk_read mode to avoid cache pollution.

**Verification:** Run a full test suite to confirm no monitor path still relies on burst-mode caching.

---

## Phase 6: Testing

### Autotest additions

1. **`--num-workers` test** — start with `--num-workers=2`, verify via unixctl
2. **Cache shrink test** — create 50K rows, evict to 5K, verify hmap bucket count shrinks and `total_atoms` matches expectations
3. **Snapshot build at startup** — open disk-store DB, verify `.json-snapshot` sidecar is created
4. **Snapshot reuse on restart** — restart ovsdb-server, verify snapshot is reused (not rebuilt) if txn_id matches
5. **Monitor from snapshot** — create monitor, verify initial reply comes from snapshot (check logs)
6. **Dirty counter rebuild** — insert rows past threshold, verify background rebuild triggers
7. **Deferred snapshot wait** — connect monitor before snapshot is ready, verify reply arrives after build completes
8. **Snapshot bypass cache** — monitor from snapshot, verify cache hit/miss counters unchanged
9. **Conditioned monitor bypass** — conditioned monitor still uses inline path, not snapshot
10. **Session disconnect during snapshot wait** — disconnect client while waiting, verify no crash/leak

### Build verification
```bash
make -j4 && make check TESTSUITEFLAGS="-j4 -k monitor"
make check TESTSUITEFLAGS="-j4 -k ovsdb"
```

---

## Critical Files

| File | Changes |
|------|---------|
| `ovsdb/ovsdb-server.c` | `--num-workers` CLI, snapshot build submission at startup |
| `ovsdb/json-snapshot.h` | **New** — snapshot builder/reader API, sidecar format |
| `ovsdb/json-snapshot.c` | **New** — build (worker), read (main), dirty counter logic |
| `ovsdb/jsonrpc-server.c` | Serve monitors from snapshot, deferred snapshot wait |
| `ovsdb/row-cache.c` | hmap_shrink on eviction, query-burst cap, sweep fixes |
| `ovsdb/table.c` | Remove burst mode for unconditioned scans |
| `ovsdb/ovsdb.c` | Snapshot build at startup (Pass 5 in attach_disk_store) |
| `ovsdb/ovsdb.h` | Dirty counter, snapshot state fields on `struct ovsdb` |
| `ovsdb/monitor.c` | Minor — yield parameter for worker compose (retained for Option B future) |
| `ovsdb/automake.mk` | Add new source files |

## Implementation Order

1. **Phase 1** (`--num-workers`) — standalone, zero risk
2. **Phase 2** (cache shrink fixes) — bug fix, independent of snapshot work
3. **Phase 3** (json-snapshot module) — new code, no integration yet
4. **Phase 4** (serve monitors from snapshot) — the key integration
5. **Phase 5** (remove cache pollution) — cleanup after Phase 4
6. **Phase 6** (tests) — incrementally after each phase, full suite after Phase 5

## Thread Safety Summary

| Data | Accessed by snapshot builder? | Safe? | Why |
|------|-------------------------------|-------|-----|
| `disk_store->index` | Yes (read-only) | Yes | Index is immutable after startup rebuild |
| Disk file (pread) | Yes | Yes | pread is thread-safe; writes use separate fd |
| `table->rows` hmap | Only for in-memory DBs (brief snapshot) | Yes | Pointer copy under lock; row data is COW |
| Row cache | **No** | Yes | Builder bypasses cache entirely |
| Sidecar file | Writer: worker. Reader: main thread | Yes | Atomic rename; reader only opens completed file |
| `db->json_snapshot_dirty_count` | Main thread only | Yes | Incremented in commit path, read in main loop |
| `db->json_snapshot_building` | Set by main, read by main | Yes | Worker signals completion via done callback |

## Data Flow Diagram

```
STARTUP:
  disk-store file ──pread()──→ [worker thread] ──datum_to_json()──→ sidecar file
                                                  (bypasses cache)

MONITOR REQUEST (unconditioned):
  sidecar file ──pread()──→ pre-serialized JSON ──→ JSON-RPC reply
                             (bypasses cache)

MONITOR REQUEST (conditioned):
  row cache / disk-store ──→ ovsdb_table_query() ──→ compose inline
                              (existing path, uses bulk_read/scan ring)

TRANSACTION COMMIT:
  dirty_count++ ──→ if > threshold ──→ [worker thread] rebuilds sidecar
```
