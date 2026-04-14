# OVSDB Multi-Threaded Serialization: Detailed Implementation Plan (Phases 0-2)

## Context

OVSDB uses a single-threaded reactor pattern (`ovsdb-server.c:main_loop()`). All client
I/O, Raft consensus, transaction execution, and JSON serialization run on one thread with
zero mutexes. When the database exceeds ~2GB, `ovsdb_to_txn_json()` and `json_to_ds()`
block the main thread for 30-60 seconds during snapshot/compaction.

The clustered (Raft) code path already solves this with `compaction_thread()` in
`ovsdb/ovsdb.c:620-640` — it clones the database and serializes in a background thread,
signaling completion via `seq_change()`. **Standalone mode does not use this pattern and
blocks synchronously.**

This plan extends the background threading pattern incrementally across three phases,
culminating in a producer/consumer architecture with lazy disk loading.

---

## Agent Decomposition

The implementation is split into four independent agents (work units) with well-defined
interfaces. Each agent can be developed and tested in isolation before integration.

### Agent 1: Standalone Snapshot Threading (Phase 0)
- **Scope**: `ovsdb/ovsdb.c` only
- **Owner**: Modifies `ovsdb_snapshot()` to use `compaction_thread()` for standalone DBs
- **Interface**: No new public API — extends existing `ovsdb_snapshot_*()` functions

### Agent 2: Disk Store Engine (Phase 1)
- **Scope**: New `ovsdb/disk-store.c`, `ovsdb/disk-store.h`
- **Owner**: Binary row format, UUID-to-offset index, sequential iteration
- **Interface**: `ovsdb_disk_store_*()` API consumed by Agent 3

### Agent 3: Row Cache (Phase 1)
- **Scope**: New `ovsdb/row-cache.c`, `ovsdb/row-cache.h`
- **Owner**: LRU cache with pinning, atom-weighted eviction
- **Interface**: `ovsdb_row_cache_*()` API, integrates with `ovsdb_table_get_row()`
- **Dependencies**: Agent 2 (disk store for cache miss path)

### Agent 4: I/O Worker Pool + Lazy Loading (Phase 2)
- **Scope**: New `ovsdb/worker-pool.c`, `ovsdb/worker-pool.h`; modify `ovsdb/trigger.c`, `ovsdb/ovsdb-server.c`
- **Owner**: Thread pool, async row loading, request parking
- **Interface**: `ovsdb_worker_pool_*()` API; extends `ovsdb_trigger` with data-wait state
- **Dependencies**: Agent 2 + Agent 3

### Integration Points Between Agents

```
Agent 1 (Phase 0)          Agent 2 (Phase 1)         Agent 3 (Phase 1)         Agent 4 (Phase 2)
ovsdb_snapshot()     --->  ovsdb_disk_store_*()  --> ovsdb_row_cache_*()  --> ovsdb_worker_pool_*()
                           disk_store_open()          cache_lookup()            pool_submit()
                           disk_store_write_row()     cache_insert()            pool_wait()
                           disk_store_read_row()      cache_evict()             trigger "data-wait"
                           disk_store_iterate()       cache_pin/unpin()
                           disk_store_close()
```

---

## Phase 0: Standalone Snapshot Threading

### Goal
Make standalone database snapshots non-blocking by reusing the `compaction_thread()` pattern
that already works for clustered (Raft) databases.

### Current Code Path (standalone, `ovsdb/ovsdb.c:664-729`)

```c
// In ovsdb_snapshot(), when applied_index == 0 (standalone):
if (!applied_index) {
    state = xzalloc(sizeof *state);
    state->data = ovsdb_to_txn_json(db, "compacting database online", true);  // BLOCKS
    state->schema = ovsdb_schema_to_json(db->schema);
}
// Falls through to ovsdb_storage_store_snapshot() — also BLOCKS
```

This serializes the entire database synchronously on the main thread.

### Target Code Path

Apply the same three-phase pattern used by Raft (lines 691-728):

1. **Launch**: Clone DB, spawn `compaction_thread()`, return immediately
2. **Poll**: Main loop detects completion via `ovsdb_snapshot_ready()`
3. **Harvest**: Join thread, store snapshot to log, free clone

### Implementation Details

**File: `ovsdb/ovsdb.c`**

**Change 1: Remove standalone early-exit in `ovsdb_snapshot()`** (line 677)

Current:
```c
if (!applied_index) {
    state = xzalloc(sizeof *state);
    state->data = ovsdb_to_txn_json(db, "compacting database online", true);
    state->schema = ovsdb_schema_to_json(db->schema);
}
```

New:
```c
if (!applied_index && !ovsdb_snapshot_ready(db)) {
    /* Standalone: launch background compaction thread, same as clustered. */
    ovs_assert(!db->snap_state);
    state = xzalloc(sizeof *state);

    state->db = ovsdb_clone_data(db);
    state->schema = ovsdb_schema_to_json(db->schema);
    state->applied_index = 0;
    state->done = seq_create();
    state->seqno = seq_read(state->done);
    state->thread = ovs_thread_create("compaction",
                                      compaction_thread, state);
    state->init_time = time_msec() - start_time;

    db->snap_state = state;
    return NULL;  /* Async — will harvest on next call. */
}
```

**Change 2: Unified harvest path for both standalone and clustered** (line 682)

The `ovsdb_snapshot_ready()` path already handles thread join and storage. Standalone
snapshots now enter this same path on the next `ovsdb_snapshot()` call. The only difference
is `applied_index == 0`, which `ovsdb_storage_store_snapshot()` already handles by
dispatching to `ovsdb_log_replace()` instead of `raft_store_snapshot()`.

**Change 3: Adjust main_loop snapshot trigger** (`ovsdb-server.c:~342`)

Current condition already works:
```c
if (!ovsdb_snapshot_in_progress(db->db)
    && (ovsdb_storage_should_snapshot(db->db->storage)
        || ovsdb_snapshot_ready(db->db))) {
    log_and_free_error(ovsdb_snapshot(db->db, trim_memory));
}
```

This naturally handles:
- First call: `!in_progress && should_snapshot` → launches thread
- Second call: `!in_progress && snapshot_ready` → harvests and stores

No main_loop changes needed.

### Risks and Mitigations

| Risk | Mitigation |
|------|-----------|
| `allow_shallow_copies=true` was safe for synchronous standalone path because main thread owned the data. Background thread needs deep copies. | `compaction_thread()` already uses `allow_shallow_copies=false`. `ovsdb_clone_data()` deep-copies all rows. No change needed. |
| Standalone log replace (`ovsdb_log_replace()`) uses file I/O that could block. | Storage write happens in harvest phase on main thread — same as current behavior. Only serialization is offloaded. |
| If standalone DB is modified between clone and harvest, snapshot is stale. | Same as Raft mode — snapshot captures a point-in-time. Subsequent transactions are appended to the log normally. |

---

## Phase 1: Disk Store Engine + Row Cache

### Goal
Introduce a binary disk-backed row store and an in-memory LRU cache so that:
- Snapshots can stream from disk without building the full JSON in memory
- Memory usage is bounded by cache size, not database size
- Row access API (`ovsdb_table_get_row()`) is unchanged for callers

### Agent 2: Disk Store Engine

**New files: `ovsdb/disk-store.c`, `ovsdb/disk-store.h`**

#### Binary Row Format

Each row is self-contained and independently addressable:

```
┌──────────────────────────────────────────────────────┐
│ Row Header (24 bytes)                                │
│   uuid:       16 bytes (raw UUID)                    │
│   total_len:   4 bytes (uint32_t, includes header)   │
│   n_columns:   2 bytes (uint16_t)                    │
│   flags:       2 bytes (uint16_t, 0x01=deleted)      │
├──────────────────────────────────────────────────────┤
│ Column 0                                             │
│   col_type:    1 byte  (OVSDB_TYPE_*)                │
│   datum_n:     4 bytes (uint32_t, number of atoms)   │
│   keys:        variable (n atoms, type-dependent)    │
│   values:      variable (n atoms, for maps only)     │
├──────────────────────────────────────────────────────┤
│ Column 1 ...                                         │
└──────────────────────────────────────────────────────┘
```

Atom encoding by type:
- `OVSDB_TYPE_INTEGER`: 8 bytes (int64_t, network byte order)
- `OVSDB_TYPE_REAL`: 8 bytes (double, IEEE 754)
- `OVSDB_TYPE_BOOLEAN`: 1 byte
- `OVSDB_TYPE_STRING`: 4 bytes length + UTF-8 bytes (no null terminator)
- `OVSDB_TYPE_UUID`: 16 bytes (raw)

#### API

```c
/* Lifecycle */
struct ovsdb_disk_store *ovsdb_disk_store_open(const char *filename,
                                               const struct ovsdb_schema *);
void ovsdb_disk_store_close(struct ovsdb_disk_store *);

/* Single-row operations */
struct ovsdb_error *ovsdb_disk_store_write_row(struct ovsdb_disk_store *,
                                               const struct ovsdb_row *);
struct ovsdb_error *ovsdb_disk_store_delete_row(struct ovsdb_disk_store *,
                                                const struct uuid *);
struct ovsdb_row *ovsdb_disk_store_read_row(struct ovsdb_disk_store *,
                                            struct ovsdb_table *,
                                            const struct uuid *);

/* Batch operations */
struct ovsdb_error *ovsdb_disk_store_write_batch(struct ovsdb_disk_store *,
                                                 const struct ovsdb_row **rows,
                                                 size_t n);

/* Iteration (for snapshots) */
struct ovsdb_disk_store_cursor *ovsdb_disk_store_cursor_open(
    struct ovsdb_disk_store *, const char *table_name);
struct ovsdb_row *ovsdb_disk_store_cursor_next(
    struct ovsdb_disk_store_cursor *);
void ovsdb_disk_store_cursor_close(struct ovsdb_disk_store_cursor *);

/* Index */
size_t ovsdb_disk_store_count(const struct ovsdb_disk_store *,
                              const char *table_name);
bool ovsdb_disk_store_contains(const struct ovsdb_disk_store *,
                               const struct uuid *);

/* Maintenance */
struct ovsdb_error *ovsdb_disk_store_compact(struct ovsdb_disk_store *);
```

#### Internal Design

- **Index**: In-memory `struct hmap` of `{uuid → file_offset}` entries loaded at open.
  For 1M rows: ~24 bytes/entry = ~24MB. Acceptable.
- **Write strategy**: Append-only. Writes append new row data and update the index.
  Deletes set the `deleted` flag in the index (lazy). `compact()` rewrites the file
  without deleted rows.
- **WAL**: Append-only write + `fdatasync()` before returning. Index rebuilt from
  sequential scan on crash recovery (same as current JSON log replay).
- **File header**: Magic bytes + schema hash + row count + index offset.

### Agent 3: Row Cache

**New files: `ovsdb/row-cache.c`, `ovsdb/row-cache.h`**

#### API

```c
/* Lifecycle */
struct ovsdb_row_cache *ovsdb_row_cache_create(size_t max_atoms);
void ovsdb_row_cache_destroy(struct ovsdb_row_cache *);

/* Lookup (returns NULL on miss) */
struct ovsdb_row *ovsdb_row_cache_lookup(struct ovsdb_row_cache *,
                                         const struct uuid *);

/* Insert/update (evicts if over budget) */
void ovsdb_row_cache_insert(struct ovsdb_row_cache *,
                            struct ovsdb_row *);
void ovsdb_row_cache_remove(struct ovsdb_row_cache *,
                            const struct uuid *);

/* Pinning (prevents eviction) */
void ovsdb_row_cache_pin(struct ovsdb_row_cache *,
                         const struct uuid *);
void ovsdb_row_cache_unpin(struct ovsdb_row_cache *,
                           const struct uuid *);

/* Stats */
size_t ovsdb_row_cache_n_atoms(const struct ovsdb_row_cache *);
size_t ovsdb_row_cache_count(const struct ovsdb_row_cache *);
size_t ovsdb_row_cache_hits(const struct ovsdb_row_cache *);
size_t ovsdb_row_cache_misses(const struct ovsdb_row_cache *);
```

#### Internal Design

- **Data structure**: `struct hmap` (same as current `table->rows`) for O(1) UUID lookup,
  plus a doubly-linked LRU list for eviction ordering.
- **Cost metric**: `n_atoms` per row (already computed by `count_atoms()` in
  `ovsdb/transaction.c:1002-1037`). Evict highest-cost rows first when
  `total_atoms > max_atoms`.
- **Pin mechanism**: Pinned UUIDs stored in a separate `struct hmap`. Pinned rows are
  skipped during eviction. Callers must unpin explicitly.
- **No locking**: Cache is accessed only from the main thread. Worker threads (Phase 2)
  insert via a lock-free queue that the main thread drains.

### Integration with `ovsdb/table.c`

**Modify `ovsdb_table_get_row()`**:

Current (`ovsdb/table.c`):
```c
const struct ovsdb_row *
ovsdb_table_get_row(const struct ovsdb_table *table, const struct uuid *uuid)
{
    /* Search table->rows hmap by UUID hash */
}
```

New:
```c
const struct ovsdb_row *
ovsdb_table_get_row(const struct ovsdb_table *table, const struct uuid *uuid)
{
    /* 1. Check cache first (fast path) */
    struct ovsdb_row *row = ovsdb_row_cache_lookup(table->cache, uuid);
    if (row) {
        return row;
    }

    /* 2. Cache miss — load from disk store */
    if (table->disk_store) {
        row = ovsdb_disk_store_read_row(table->disk_store, table, uuid);
        if (row) {
            ovsdb_row_cache_insert(table->cache, row);
        }
    }
    return row;
}
```

**Modify `struct ovsdb_table`** (`ovsdb/table.h`):
```c
struct ovsdb_table {
    struct ovsdb_table_schema *schema;
    struct ovsdb_txn_table *txn_table;
    struct hmap rows;                      /* Kept for backward compat / unlimited cache */
    struct hmap *indexes;
    bool log;
    /* New fields: */
    struct ovsdb_row_cache *cache;         /* NULL if cache disabled */
    struct ovsdb_disk_store *disk_store;   /* NULL if disk store disabled */
};
```

### Integration with `ovsdb/transaction.c`

**Modify `ovsdb_txn_row_commit()`** to write-back to disk:

After the existing index update logic, add:
```c
if (table->disk_store) {
    if (new) {
        ovsdb_disk_store_write_row(table->disk_store, new);
        ovsdb_row_cache_insert(table->cache, new);
    } else {
        ovsdb_disk_store_delete_row(table->disk_store, &old->uuid);
        ovsdb_row_cache_remove(table->cache, &old->uuid);
    }
}
```

**Pin rows during transaction** in `ovsdb_txn_row_modify()`:
```c
if (table->cache) {
    ovsdb_row_cache_pin(table->cache, ovsdb_row_get_uuid(row));
}
```

Unpin in `ovsdb_txn_complete()` after monitors are notified.

---

## Phase 2: I/O Worker Pool + Lazy Loading

### Goal
- Start accepting client connections immediately at startup (load only the UUID index)
- Load rows from disk asynchronously in a thread pool on cache miss
- Park client requests that need unloaded rows, resume when loaded
- Reuse the existing trigger parking mechanism

### Agent 4: Worker Pool

**New files: `ovsdb/worker-pool.c`, `ovsdb/worker-pool.h`**

#### API

```c
/* Pool lifecycle */
struct ovsdb_worker_pool *ovsdb_worker_pool_create(size_t n_threads,
                                                   const char *name);
void ovsdb_worker_pool_destroy(struct ovsdb_worker_pool *);

/* Job submission */
typedef void (*ovsdb_worker_fn)(void *arg);
typedef void (*ovsdb_worker_done_fn)(void *arg, void *result);

struct ovsdb_worker_job *ovsdb_worker_pool_submit(
    struct ovsdb_worker_pool *,
    ovsdb_worker_fn fn,           /* Runs in worker thread */
    void *arg,                    /* Passed to fn */
    ovsdb_worker_done_fn done_fn, /* Runs in main thread after fn completes */
    void *done_arg);

/* Polling (main thread) */
void ovsdb_worker_pool_run(struct ovsdb_worker_pool *);   /* Drain completed jobs */
void ovsdb_worker_pool_wait(struct ovsdb_worker_pool *);  /* Register with poll */
bool ovsdb_worker_pool_has_pending(struct ovsdb_worker_pool *);
```

#### Internal Design

```
Main Thread                           Worker Threads (N)
    |                                      |
    +-- submit(fn, arg) -----> [job_queue] |
    |                              |       |
    |                              +---->  fn(arg)
    |                              |       |
    |   [done_queue] <------------ +       |
    |       |                              |
    +-- pool_run()                         |
    |   done_fn(done_arg, result)          |
    |                                      |
    +-- pool_wait()                        |
        seq_wait(pool->done_seq, seqno)    |
```

- **Job queue**: Lock-protected linked list (`ovs_mutex` + `ovs_list`). Workers dequeue
  under lock, execute `fn(arg)` without lock, then enqueue result to done queue.
- **Done queue**: Lock-protected linked list. Main thread drains in `pool_run()`.
- **Signaling**: Workers call `seq_change(pool->done_seq)` after enqueuing result.
  Main thread calls `seq_wait(pool->done_seq, seqno)` in `pool_wait()`.
- **Thread lifecycle**: Threads loop: lock → dequeue job → unlock → execute → lock →
  enqueue result → signal → repeat. Exit when pool is destroyed (poison pill).

#### Row State Tracker

Embedded in `struct ovsdb_row_cache`:

```c
enum ovsdb_row_state {
    OVSDB_ROW_UNLOADED,   /* UUID known, data on disk */
    OVSDB_ROW_LOADING,    /* Load job submitted to worker pool */
    OVSDB_ROW_CACHED,     /* Data in memory */
};

struct ovsdb_row_cache_entry {
    struct hmap_node hmap_node;   /* In cache->entries by UUID */
    struct uuid uuid;
    enum ovsdb_row_state state;
    struct ovsdb_row *row;        /* NULL if UNLOADED or LOADING */
    struct ovs_list waiters;      /* Parked triggers waiting for this row */
    bool pinned;
};
```

#### Trigger Extension

**Modify `ovsdb/trigger.c`** to add "waiting for data" state:

New state in `ovsdb_trigger_try()` (`trigger.c:225-432`):

```c
/* Before executing the transaction, check if all referenced rows are loaded */
if (ovsdb_txn_needs_unloaded_rows(txn)) {
    /* Park this trigger — will be resumed when rows are loaded */
    ovsdb_row_cache_register_waiter(cache, needed_uuids, t);
    return false;  /* Not complete yet */
}
```

When the worker pool loads a row and inserts it into the cache, the cache drains
the waiter list for that UUID and sets `db->run_triggers = true`, causing the
main loop to re-run `ovsdb_trigger_run()` which retries the parked triggers.

This maps exactly to the existing pattern where `ovsdb_trigger_try()` returns false
and gets retried on the next `ovsdb_trigger_run()` call.

#### Main Loop Integration

**Modify `ovsdb-server.c:main_loop()`**:

In the run phase (after `ovsdb_trigger_run()`):
```c
/* Drain completed worker jobs (loads rows into cache, resumes triggers) */
ovsdb_worker_pool_run(worker_pool);
```

In the wait phase:
```c
/* Register worker pool completion with poll */
ovsdb_worker_pool_wait(worker_pool);
```

#### Startup Sequence Change

Current startup (`ovsdb-server.c`):
```
open storage → replay ALL log entries → accept connections
```

New startup:
```
open storage → load UUID index from disk store → accept connections
              → rows loaded on demand via worker pool
```

The `read_db()` function in `ovsdb-server.c` currently replays transactions into
in-memory tables. With disk store, initial log entries are written to the disk store
during first open, and the UUID index is loaded. Rows are loaded lazily.

---

## Database Format Migration and Backward Compatibility

### Problem

Phase 1 introduces a new binary disk store format alongside the existing JSON-based log
format (`OVSDB JSON <len> <sha1>`). Deployments upgrading from current OVS will have
databases in the old format. We must handle:

1. **Upgrade**: Old JSON log → new binary disk store
2. **Downgrade**: New binary format → old JSON log (for rollback)
3. **Mixed clusters**: Raft nodes at different versions during rolling upgrade
4. **Tool compatibility**: `ovsdb-tool compact`, `ovsdb-tool convert` must work

### Current Format Versioning

OVSDB identifies file format by magic string in the first record header:
- `OVSDB_MAGIC = "JSON"` — standalone log (`ovsdb/log.h`)
- `RAFT_MAGIC = "CLUSTER"` — clustered Raft log (`ovsdb/raft.h`)

Detection in `ovsdb_storage_open__()` (`storage.c:62-97`):
```c
ovsdb_log_open(filename, OVSDB_MAGIC"|"RAFT_MAGIC, ...)
if (!strcmp(ovsdb_log_get_magic(log), RAFT_MAGIC)) { ... }
```

Schema versioning is separate: the first log record contains the JSON schema with a
semantic version string (e.g., `"7.15.0"`). Schema conversion uses `ovsdb_convert()`
in `ovsdb/file.c` with column-by-name matching and type coercion.

### Strategy: Dual-Format Support with Transparent Migration

**New magic string**: `OVSDB_BINARY_MAGIC = "BINARYV1"`

**File structure for binary disk store**:
```
┌─────────────────────────────────────────────────────┐
│ File Header (64 bytes)                              │
│   magic:          8 bytes  "BINARYV1"               │
│   format_version: 4 bytes  uint32_t (1)             │
│   schema_hash:   20 bytes  SHA-1 of schema JSON     │
│   schema_offset:  8 bytes  offset to embedded schema │
│   index_offset:   8 bytes  offset to UUID index      │
│   row_count:      8 bytes  total rows                │
│   flags:          4 bytes  (0x01=compressed)         │
│   reserved:       4 bytes                            │
├─────────────────────────────────────────────────────┤
│ Embedded Schema (JSON, variable length)             │
│   Same format as current log record 0               │
├─────────────────────────────────────────────────────┤
│ Row Data Region (binary rows, variable length)      │
│   Row 0: [header][columns...]                       │
│   Row 1: [header][columns...]                       │
│   ...                                               │
├─────────────────────────────────────────────────────┤
│ UUID Index (fixed-size entries)                      │
│   [16B uuid | 8B offset | 4B length | 4B table_id] │
│   ...                                               │
└─────────────────────────────────────────────────────┘
```

The `format_version` field enables future binary format changes without new magic strings.
The embedded schema enables `ovsdb-tool` to inspect the binary file.

### Migration Paths

**Upgrade (automatic at startup)**:

When `ovsdb_storage_open__()` detects `OVSDB_MAGIC` ("JSON") and the server is configured
with `--enable-disk-store`:

1. Open old log, replay all entries into in-memory tables (current behavior)
2. Write all rows to new binary disk store file (`.db.bin`)
3. Write new JSON log with single snapshot record pointing to binary store
4. Atomic rename to replace old file
5. Subsequent startups open binary format directly

This is the same pattern as `ovsdb_log_replace()` — create temp, write, atomic rename.

**Downgrade (via ovsdb-tool)**:

```bash
ovsdb-tool convert-format mydb.db --format=json
```

New `ovsdb-tool` command:
1. Open binary disk store
2. Iterate all rows, serialize to JSON
3. Write standard JSON log file
4. Atomic rename

**No automatic downgrade at startup** — if a node runs old OVS that doesn't understand
`BINARYV1`, it fails to open with a clear error message. Admin must run
`ovsdb-tool convert-format` before downgrading.

### Mixed Raft Clusters During Rolling Upgrade

Raft replication uses JSON for log entries and snapshots regardless of local storage
format. The binary disk store is **local storage only** — it does not change the Raft
wire protocol.

```
Node A (old, JSON log)           Node B (new, binary disk store)
         │                                    │
         ├── Raft AppendEntries ─────────────►│
         │   (JSON log entries)               │ Receives JSON → writes to binary store
         │                                    │
         │◄── Raft AppendEntries ─────────────┤
         │   (JSON log entries)               │ Reads from binary store → sends as JSON
```

**Key invariant**: Raft entries are always JSON on the wire. Local storage format is
transparent to other nodes. Rolling upgrade requires no coordination.

### ovsdb-tool Integration

**Existing commands that must work with binary format**:

| Command | Change |
|---------|--------|
| `ovsdb-tool compact DB` | Detect format, compact binary OR JSON |
| `ovsdb-tool convert DB SCHEMA` | Read binary, convert schema, write binary |
| `ovsdb-tool db-version DB` | Read embedded schema from binary header |
| `ovsdb-tool db-cksum DB` | Read schema hash from binary header |
| `ovsdb-tool db-is-standalone DB` | Check magic: `JSON` or `BINARYV1` |
| `ovsdb-tool show-log DB` | Decode binary rows for human-readable output |

**New commands**:

| Command | Purpose |
|---------|---------|
| `ovsdb-tool convert-format DB --format=json\|binary` | Convert between formats |
| `ovsdb-tool db-format DB` | Print "json" or "binaryv1" |

### Migration Tests (added to test plan)

```
AT_SETUP([migration - json to binary upgrade])
  # Create DB in old JSON format
  # Start ovsdb-server with --enable-disk-store
  # Verify automatic migration occurred
  # Query data — verify all rows intact
  # Check ovsdb-tool db-format reports "binaryv1"
AT_CLEANUP

AT_SETUP([migration - binary to json downgrade])
  # Create DB in binary format
  # Run ovsdb-tool convert-format --format=json
  # Verify JSON format output
  # Start old ovsdb-server — verify it opens successfully
AT_CLEANUP

AT_SETUP([migration - schema convert on binary DB])
  # Create binary DB with schema v1
  # Run ovsdb-tool convert with schema v2
  # Verify columns added/removed correctly
  # Verify binary format preserved
AT_CLEANUP

AT_SETUP([migration - rolling upgrade in Raft cluster])
  # Start 3-node Raft cluster with JSON format
  # Upgrade node 1 to binary format, restart
  # Insert data via any node — verify replication works
  # Upgrade node 2, restart — verify cluster health
  # Upgrade node 3 — all nodes now binary
  # Verify data consistency across all nodes
AT_CLEANUP

AT_SETUP([migration - binary format version forward compat])
  # Create binary file with format_version=99 (future)
  # Attempt to open — verify clean error, not crash
AT_CLEANUP
```

---

## Phase 3: Deferred JSON Serialization with Disk Caching (Expanded)

### Goal

Offload `json_to_ds()` from the main thread to background workers so the main loop
can continue processing transactions and client requests while large JSON payloads are
serialized. Additionally, cache serialized JSON on disk to avoid re-serialization when
the same data is requested again (e.g., multiple monitors, Raft snapshots).

### Current Send Path Bottleneck

The critical path in `jsonrpc_send()` (`lib/jsonrpc.c:254-306`):

```c
int jsonrpc_send(struct jsonrpc *rpc, struct jsonrpc_msg *msg) {
    json = jsonrpc_msg_to_json(msg);
    json_to_ds(json, 0, &ds);          // ← BLOCKS main thread (O(n) CPU)
    json_destroy(json);
    ofpbuf_use_ds(buf, &ds);           // Wrap serialized string
    ovs_list_push_back(&rpc->output, &buf->list_node);  // Queue
    rpc->backlog += length;
    jsonrpc_run(rpc);                  // Non-blocking stream_send()
}
```

For a 2GB database snapshot, `json_to_ds()` takes 30-60 seconds. During this time the
main thread cannot process any other client, run Raft, or handle transactions.

The existing `JSON_SERIALIZED_OBJECT` cache in `ovsdb/monitor.c` helps for repeated
monitor updates but does NOT help for initial snapshots or Raft InstallSnapshot — those
are one-shot serializations.

### Architecture: Deferred Serialization + JSON Disk Cache

```
Main Thread                    Serialization Workers         JSON Disk Cache
    │                                  │                          │
    ├─ Transaction commits             │                          │
    │  (fast, single-threaded)         │                          │
    │                                  │                          │
    ├─ Monitor update needed           │                          │
    │  Build JSON tree (cheap)         │                          │
    │                                  │                          │
    ├─ Check disk cache ──────────────────────────────────────────┤
    │  cache_key = sha1(json_tree)     │                     hit? │
    │                                  │                          │
    │  [HIT] ◄─────────────────────────────── Return ofpbuf ─────┤
    │  Queue pre-serialized buffer     │       (mmap or pread)    │
    │                                  │                          │
    │  [MISS] Enqueue serialize job ──►│                          │
    │  Mark session "pending"          ├─ json_to_ds(json)        │
    │                                  ├─ Write to disk cache ───►│
    │  ... process other clients ...   │                          │
    │                                  │                          │
    │  ◄── seq_change() ──────────────┤                          │
    │  pool_run() → done_fn()          │                          │
    │  Queue ofpbuf to session output  │                          │
    │  Unmark "pending"                │                          │
    │                                  │                          │
    ├─ jsonrpc_run()                   │                          │
    │  stream_send() (non-blocking)    │                          │
```

### Deferred Serialization Design

**New function: `jsonrpc_send_deferred()`**

```c
/* Queue a message for deferred serialization. The JSON object is handed off
 * to a worker thread for serialization. The connection remains open and can
 * process other messages. When serialization completes, the serialized buffer
 * is queued for sending.
 *
 * Returns 0 on success. The caller must not access 'msg' after this call. */
int jsonrpc_send_deferred(struct jsonrpc *rpc, struct jsonrpc_msg *msg,
                          struct ovsdb_worker_pool *pool);
```

**Implementation**:

1. Convert `msg` to `struct json` (cheap — just wraps method/params/id)
2. Submit to worker pool:
   - Worker thread calls `json_to_ds(json, 0, &ds)` (expensive, off main thread)
   - Worker thread optionally writes serialized string to disk cache
   - Worker signals completion via `seq_change()`
3. Main thread's `jsonrpc_run_deferred()` (called from main loop):
   - Checks for completed serialization jobs
   - Wraps serialized buffer in `ofpbuf`, pushes to `rpc->output`
   - Calls `jsonrpc_run()` to send

**Session state**: Add a `pending_serialization` flag to `ovsdb_jsonrpc_session`:
```c
struct ovsdb_jsonrpc_session {
    /* ... existing fields ... */
    size_t n_pending_serializations;  /* Count of in-flight serializations */
};
```

The session can still receive new requests while serialization is pending. The main
thread continues dispatching other sessions. Only the `jsonrpc_send()` for this
session is deferred — not the entire connection.

**Threshold**: Only defer serialization for messages larger than a configurable
threshold (e.g., 1MB estimated size). Small messages serialize inline as today.

Size estimation: `json_estimate_size()` — walk JSON tree counting atoms and string
lengths without actually serializing. O(n) but with tiny constant (just pointer
chasing, no allocation).

### Integration with Monitor Send Path

Current (`ovsdb/jsonrpc-server.c:1903-1929`):
```c
void ovsdb_jsonrpc_monitor_flush_all(session) {
    for each monitor {
        json = ovsdb_jsonrpc_monitor_compose_update(m, false);
        msg = jsonrpc_create_notify(..., json);
        jsonrpc_session_send(session->js, msg);  // BLOCKS on json_to_ds()
    }
}
```

New:
```c
void ovsdb_jsonrpc_monitor_flush_all(session) {
    for each monitor {
        json = ovsdb_jsonrpc_monitor_compose_update(m, false);
        msg = jsonrpc_create_notify(..., json);

        size_t est_size = json_estimate_size(json);
        if (est_size > DEFERRED_THRESHOLD) {
            jsonrpc_send_deferred(session->js, msg, serialize_pool);
        } else {
            jsonrpc_session_send(session->js, msg);  // Inline for small
        }
    }
}
```

### JSON Disk Cache

**Purpose**: Avoid re-serializing the same JSON when:
- Multiple monitors request the same initial snapshot
- Raft snapshot is requested shortly after compaction
- Server restarts and needs to re-send a recent snapshot

**Cache location**: `<db-dir>/.json-cache/`

**Cache entry format**:
```
Filename: sha1_hex(json_content).json
Content:  Raw serialized JSON string (same as json_to_ds output)
Metadata: <sha1>.meta  { "created": <timestamp>, "size": <bytes>,
                          "source_atoms": <n_atoms>, "db_txnid": <uuid> }
```

**Cache key**: SHA-1 of the JSON tree computed via `json_hash()`. This is
cheaper than full serialization — it walks the tree but only computes a hash.

**Cache API**:

```c
/* Lifecycle */
struct ovsdb_json_cache *ovsdb_json_cache_create(const char *dir,
                                                  size_t max_bytes,
                                                  int max_age_seconds);
void ovsdb_json_cache_destroy(struct ovsdb_json_cache *);

/* Lookup — returns ofpbuf with serialized string, or NULL on miss */
struct ofpbuf *ovsdb_json_cache_get(struct ovsdb_json_cache *,
                                    const struct json *);

/* Insert — takes ownership of serialized string */
void ovsdb_json_cache_put(struct ovsdb_json_cache *,
                          const struct json *key,
                          const char *serialized, size_t len);

/* Maintenance */
void ovsdb_json_cache_evict(struct ovsdb_json_cache *);
```

### Cache Staleness and Drift Policy

The JSON disk cache must not serve stale data. Two eviction triggers:

**1. Time-based eviction**:
- Cache entries older than `max_age_seconds` (default: 300s / 5 minutes) are evicted
- Rationale: After 5 minutes, the database has likely changed enough that the cached
  JSON is useless (clients would need incremental updates, not the old snapshot)

**2. Transaction-count drift**:
- Track `db->n_txns_since_cache` — incremented on each committed transaction
- When `n_txns_since_cache > max_drift_txns` (default: 1000), invalidate all
  cache entries for this database
- Rationale: After 1000 transactions, the probability that any cached JSON is still
  useful drops to near zero

**3. Size-based eviction**:
- Total cache size capped at `max_bytes` (default: 1GB or 50% of DB size, whichever
  is smaller)
- LRU eviction when over budget

**4. Explicit invalidation**:
- `ovsdb_json_cache_invalidate(cache, db_txnid)` — called when a schema conversion
  or database replace occurs. Wipes all entries.

**Configuration via ovsdb-server options**:
```
--json-cache-max-size=1073741824   # 1GB
--json-cache-max-age=300           # 5 minutes
--json-cache-max-drift=1000        # transactions
--json-cache-dir=/var/run/openvswitch/.json-cache
--no-json-cache                    # Disable entirely
```

### Data Immutability Contract

When a serialization job is submitted to the worker pool, the JSON tree must not be
modified until serialization completes. This is guaranteed by existing OVSDB patterns:

- **Monitor updates**: `ovsdb_monitor_compose_update()` returns a freshly built JSON
  tree. Once returned, the monitor layer does not modify it. The worker serializes this
  tree, then the main thread destroys it in `done_fn`.
- **Snapshots**: `ovsdb_to_txn_json()` builds a new JSON tree from cloned data
  (in `compaction_thread`). The clone is independent.
- **Transaction replies**: Built fresh per request, not reused.

Ownership transfer: `jsonrpc_send_deferred()` takes ownership of `msg`. The worker
thread reads it, the `done_fn` frees it.

### Files Modified

| File | Change |
|------|--------|
| `lib/jsonrpc.c` | Add `jsonrpc_send_deferred()`, `jsonrpc_run_deferred()` |
| `lib/jsonrpc.h` | Declare new functions |
| `lib/json.c` | Add `json_estimate_size()`, `json_hash()` |
| `ovsdb/jsonrpc-server.c` | Use deferred send for large monitor updates |
| `ovsdb/monitor.c` | Pass serialized cache to `json_serialized_object_create()` |
| `ovsdb/ovsdb-server.c` | Initialize/poll serialization pool + JSON cache |
| New: `ovsdb/json-cache.c` | JSON disk cache implementation |
| New: `ovsdb/json-cache.h` | Public API |

### Phase 3 Tests

```
test-json-cache basic
  - Create cache, put serialized string, get by JSON key — verify match

test-json-cache miss
  - Get with unknown JSON key — verify returns NULL

test-json-cache time-eviction
  - Put entry, advance clock past max_age, verify evicted

test-json-cache drift-eviction
  - Put entry, simulate 1001 transactions, verify evicted

test-json-cache size-eviction
  - Set max_bytes=1000, insert 2000 bytes — verify LRU eviction

test-json-cache invalidate
  - Put entries, call invalidate — verify all removed

test-deferred-send basic
  - Send message via jsonrpc_send_deferred()
  - Call jsonrpc_run_deferred() — verify message queued to output
  - Call jsonrpc_run() — verify sent to stream

test-deferred-send concurrent-sessions
  - 10 sessions each with deferred send
  - Verify all messages serialized and delivered
  - Verify main thread was not blocked

test-deferred-send threshold
  - Small message (< 1MB): verify inline send (not deferred)
  - Large message (> 1MB): verify deferred send

test-deferred-send asan-ownership
  - Submit deferred message, verify msg freed exactly once
  - ASAN detects double-free or leak
```

**Autotest**:
```
AT_SETUP([deferred serialization - server responsive during large send])
  # Start ovsdb-server with serialization workers
  # Create monitor on large table (100K rows)
  # While initial snapshot serializes, send transact request
  # Verify transact completes quickly (< 1s)
  # Verify monitor snapshot eventually delivered
AT_CLEANUP

AT_SETUP([json cache - avoids re-serialization])
  # Start ovsdb-server with JSON cache
  # Connect two monitor clients to same table
  # Verify second client gets cached serialization (check logs)
  # Modify data, verify cache invalidated
AT_CLEANUP
```

---

## Remaining Phases (High-Level)

### Phase 4: Chunked Monitor Protocol (V4)
- New monitor version with chunked initial snapshot delivery
- Server iterates disk store cursor, serializes N rows per chunk
- C and Python IDL clients buffer chunks and reassemble
- Backward compatible: old clients negotiate V1-V3

### Phase 5: Chunked Raft Snapshots
- `raft_install_snapshot_request` sends data in chunks via disk store cursor
- Receiver writes chunks to disk store incrementally
- Node becomes partially operational during sync

### Phase 6: Tuning and Hardening
- Cache sizing heuristics, worker pool sizing
- Crash recovery testing, performance regression benchmarks

---

## Test Plan

### Test Infrastructure

All tests use the existing OVS test infrastructure:
- **Autotest (.at files)** for integration tests with running daemons
- **C unit tests (`tests/test-*.c`)** for low-level correctness, concurrency, and memory
- **ASAN** (`-fsanitize=address`): Detects heap-buffer-overflow, use-after-free,
  double-free, memory leaks (with `detect_leaks=1`)
- **UBSAN** (`-fsanitize=undefined`): Detects undefined behavior, integer overflow,
  null pointer dereference
- **TSAN** (`-fsanitize=thread`): **New for this project** — detects data races,
  lock-order-inversions, thread leaks. Not previously used in OVS.
- **Valgrind**: Used via `tests/valgrind-wrapper.in` for deeper leak analysis
- **`MALLOC_PERTURB_=165`**: Already set in `tests/atlocal.in` — overwrites freed memory
  with 0xA5 pattern to catch use-after-free even without ASAN
- **`MALLOC_CHECK_=2`**: Already set — enables glibc malloc sanity checks

### Test File Organization

```
tests/
  test-disk-store.c          # Agent 2: Disk store unit tests
  test-row-cache.c           # Agent 3: Row cache unit tests
  test-worker-pool.c         # Agent 4: Worker pool unit/stress tests
  test-snapshot-threading.c  # Agent 1: Standalone snapshot threading
  test-json-cache.c          # Phase 3: JSON disk cache unit tests
  test-deferred-send.c       # Phase 3: Deferred serialization unit tests
  ovsdb-snapshot.at          # Phase 0: Integration tests
  ovsdb-disk-store.at        # Phase 1: Disk store integration tests
  ovsdb-row-cache.at         # Phase 1: Cache integration tests
  ovsdb-lazy-load.at         # Phase 2: Lazy loading integration tests
  ovsdb-migration.at         # Migration: Format upgrade/downgrade tests
  ovsdb-deferred-send.at     # Phase 3: Deferred serialization integration
  ovsdb-integration.at       # Cross-agent integration tests
```

---

### Phase 0 Tests: Standalone Snapshot Threading

#### T0.1: C Unit Test — `test-snapshot-threading.c`

```
test-snapshot-threading basic
  - Create standalone ovsdb with 10 tables, 1000 rows each
  - Call ovsdb_snapshot() — verify it returns NULL (async launch)
  - Verify ovsdb_snapshot_in_progress(db) == true
  - Verify ovsdb_snapshot_ready(db) == false
  - Spin until ovsdb_snapshot_ready(db) == true
  - Call ovsdb_snapshot() again — verify it harvests and stores
  - Verify snapshot file exists and is valid (re-read and compare)

test-snapshot-threading concurrent-modification
  - Create standalone ovsdb with data
  - Launch snapshot (ovsdb_snapshot returns NULL)
  - While snapshot in progress: insert 100 new rows into the live DB
  - Harvest snapshot
  - Verify snapshot contains original data (not the new rows)
  - Verify live DB contains original + new rows

test-snapshot-threading double-call
  - Create standalone ovsdb
  - Call ovsdb_snapshot() — async launch
  - Call ovsdb_snapshot() again immediately — verify returns NULL
    (snapshot_in_progress guard)
  - Wait for ready, harvest

test-snapshot-threading empty-db
  - Create standalone ovsdb with empty tables
  - Call ovsdb_snapshot() — verify it handles zero rows

test-snapshot-threading large-rows
  - Create rows with large map/set datums (10K atoms each)
  - Snapshot and verify correctness
```

**Memory safety**: Run under ASAN. The clone+serialize path must not reference
the live database's memory. `allow_shallow_copies=false` in `compaction_thread()`
ensures deep copies. ASAN will catch any use-after-free if the clone is incomplete.

**Segfault detection**: ASAN + `MALLOC_PERTURB_=165` will catch null pointer
dereferences and accesses to freed memory (filled with 0xA5 pattern).

#### T0.2: Autotest — `ovsdb-snapshot.at`

```
AT_SETUP([standalone snapshot - non-blocking])
  # Start ovsdb-server with standalone DB
  # Insert enough data to trigger snapshot threshold
  # Verify server remains responsive during snapshot
  # (send transact requests while snapshot runs)
  # Check logs for "Compaction thread started" message
  # Check logs for timing: init_time + thread_time
AT_CLEANUP

AT_SETUP([standalone snapshot - crash recovery])
  # Start ovsdb-server, insert data, trigger snapshot
  # Kill ovsdb-server during snapshot (SIGKILL)
  # Restart — verify DB is intact (pre-snapshot state)
  # The .tmp file should be cleaned up or ignored
AT_CLEANUP

AT_SETUP([standalone snapshot - data integrity])
  # Insert known data pattern (UUIDs, strings, maps, sets)
  # Trigger snapshot and wait for completion
  # Stop server, restart from snapshot file
  # Query all data — verify exact match with original
AT_CLEANUP
```

---

### Phase 1 Tests: Disk Store + Row Cache

#### T1.1: C Unit Test — `test-disk-store.c`

```
test-disk-store create-open-close
  - Create disk store, close, reopen — verify index is preserved

test-disk-store write-read-single
  - Write one row, read it back by UUID — verify all columns match
  - Test each datum type: integer, real, boolean, string, uuid
  - Test complex types: set of integers, map of string→uuid

test-disk-store write-read-batch
  - Write 10,000 rows in batches of 100
  - Read each back by UUID — verify correctness
  - Verify disk_store_count() returns 10,000

test-disk-store delete
  - Write row, delete by UUID, read — verify returns NULL
  - Verify disk_store_contains() returns false after delete

test-disk-store iterate
  - Write 1,000 rows across 3 tables
  - Open cursor for each table — iterate and collect UUIDs
  - Verify all expected UUIDs seen, no duplicates

test-disk-store compact
  - Write 1,000 rows, delete 500
  - Compact — verify file size decreased
  - Read remaining 500 — verify correctness

test-disk-store corrupt-file
  - Write rows, close, truncate file mid-row
  - Reopen — verify error handling (graceful, not crash)

test-disk-store large-datum
  - Write row with a map of 100,000 entries
  - Read back — verify all entries present

test-disk-store binary-format-roundtrip
  - For each ovsdb_type: create datum, serialize to binary, deserialize
  - Compare original and deserialized — must be identical
  - Use ovsdb_datum_equals() for comparison
```

**Memory safety (ASAN)**:
```
test-disk-store asan-read-after-close
  - Open store, read row (get pointer), close store
  - Access row fields — must NOT crash (row is independent of store)

test-disk-store asan-double-free
  - Read same row twice — verify no double-free on destroy

test-disk-store asan-leak-check
  - Write and read 10,000 rows
  - Close store without explicitly freeing rows
  - ASAN detect_leaks=1 must report if rows were leaked
```

**Segfault/invalid address**:
```
test-disk-store null-uuid-lookup
  - Read with all-zeros UUID — verify returns NULL, no crash

test-disk-store invalid-offset
  - Corrupt index to point to invalid file offset
  - Read — verify error, no segfault
```

#### T1.2: C Unit Test — `test-row-cache.c`

```
test-row-cache basic-insert-lookup
  - Create cache with max_atoms=10000
  - Insert row, lookup by UUID — verify same pointer returned

test-row-cache eviction-by-atoms
  - Create cache with max_atoms=100
  - Insert rows with 10 atoms each until 15 rows (150 atoms > 100 budget)
  - Verify oldest unpinned rows were evicted
  - Verify cache_count() <= budget

test-row-cache pin-prevents-eviction
  - Create cache with max_atoms=50
  - Insert and pin 5 rows (50 atoms)
  - Insert 5 more unpinned rows (50 atoms, over budget)
  - Verify pinned rows still present
  - Verify unpinned rows evicted first

test-row-cache unpin-allows-eviction
  - Pin row, fill cache, unpin row
  - Insert more rows — verify previously-pinned row can now be evicted

test-row-cache remove
  - Insert row, remove by UUID
  - Lookup — verify returns NULL
  - Verify cache_count() decreased

test-row-cache stats
  - Perform known hit/miss pattern
  - Verify cache_hits() and cache_misses() counts match

test-row-cache zero-budget
  - Create cache with max_atoms=0
  - Insert row — verify immediately evicted (or never stored)
  - Lookups always return NULL

test-row-cache unlimited
  - Create cache with max_atoms=SIZE_MAX
  - Insert 100,000 rows — verify all retained (no eviction)
  - This matches current OVS behavior (all in memory)
```

**Memory safety (ASAN)**:
```
test-row-cache asan-evicted-row-access
  - Insert row, get pointer, trigger eviction of that row
  - Verify the pointer is no longer valid (ASAN detects use-after-free
    if caller illegally holds the pointer past eviction)
  - Correct code must re-lookup after any operation that could evict

test-row-cache asan-pin-lifetime
  - Pin row, get pointer, unpin, trigger eviction
  - Verify row was valid while pinned, freed after unpin+evict
  - ASAN catches if row is accessed after eviction

test-row-cache asan-destroy-with-rows
  - Insert rows, destroy cache
  - Verify all rows are freed (no leaks with detect_leaks=1)
```

#### T1.3: Integration Test — Agent 2 + Agent 3

```
test-disk-store-cache integration
  - Create disk store with 10,000 rows
  - Create cache with max_atoms=1000 (fits ~100 rows)
  - Simulate ovsdb_table_get_row() pattern:
    lookup cache → miss → read from disk → insert cache
  - Access 10,000 rows in random order
  - Verify all returned correctly
  - Verify cache hit rate improves on second pass
  - Verify no memory leaks (ASAN)
```

#### T1.4: Autotest — `ovsdb-disk-store.at`

```
AT_SETUP([disk store - basic CRUD via ovsdb-server])
  # Start ovsdb-server with disk store enabled
  # Insert rows via ovs-vsctl / ovsdb-client transact
  # Query rows — verify returned correctly
  # Delete rows — verify removed
  # Restart server — verify data persists from disk
AT_CLEANUP

AT_SETUP([disk store - cache eviction under memory pressure])
  # Start ovsdb-server with small cache limit (--row-cache-size=1MB)
  # Insert many rows (100MB worth)
  # Query random rows — verify all accessible
  # Check memory usage stays bounded (via ovs-appctl memory/show)
AT_CLEANUP
```

---

### Phase 2 Tests: I/O Worker Pool + Lazy Loading

#### T2.1: C Unit Test — `test-worker-pool.c`

```
test-worker-pool basic
  - Create pool with 4 threads
  - Submit 100 jobs (each increments an atomic counter)
  - Run pool until all complete
  - Verify counter == 100

test-worker-pool ordering
  - Submit jobs that record their execution order
  - Verify all jobs executed (order may vary — non-deterministic)

test-worker-pool done-callback
  - Submit job with done_fn callback
  - Call pool_run() on main thread
  - Verify done_fn was called on main thread (check thread ID)

test-worker-pool stress
  - Create pool with 8 threads
  - Submit 100,000 jobs concurrently from main thread
  - Each job: allocate 1KB, fill with pattern, free
  - Verify no crashes, no leaks (ASAN)

test-worker-pool destroy-with-pending
  - Submit 1000 jobs, immediately destroy pool
  - Verify no crash, no leak (pending jobs are drained or cancelled)

test-worker-pool zero-threads
  - Create pool with 0 threads (synchronous fallback)
  - Submit job — verify it executes inline on submit
```

**Synchronization tests (TSAN)**:
```
test-worker-pool tsan-data-race
  - Submit jobs that read/write a shared counter WITHOUT atomics
  - TSAN must detect the race
  - Fix: use atomic_count — TSAN must report clean

test-worker-pool tsan-seq-signaling
  - Submit job that signals via seq_change()
  - Main thread waits via seq_wait() + poll_block()
  - TSAN verifies no races in seq/latch/poll interaction

test-worker-pool tsan-queue-integrity
  - 8 threads submitting + 8 threads consuming simultaneously
  - TSAN verifies job_queue and done_queue mutex discipline
```

**Memory safety (ASAN)**:
```
test-worker-pool asan-job-use-after-free
  - Submit job that stores result pointer
  - Main thread reads result in done_fn
  - ASAN verifies no stale pointer access

test-worker-pool asan-pool-destroy-leak
  - Submit 1000 jobs, destroy pool
  - ASAN detect_leaks=1 verifies all job structs freed
```

#### T2.2: C Unit Test — Lazy Loading Integration

```
test-lazy-load basic
  - Create disk store with 1000 rows, cache with budget for 100
  - Create worker pool with 4 threads
  - Request row by UUID — verify it returns (after async load)
  - Request same row again — verify cache hit (no disk I/O)

test-lazy-load concurrent-requests
  - Request 100 different rows simultaneously
  - Worker pool loads them in parallel
  - Verify all 100 returned correctly
  - Verify no duplicate loads (coalescing)

test-lazy-load request-coalescing
  - Request same UUID from 10 simulated clients
  - Verify only 1 disk read occurs
  - Verify all 10 clients get the same row

test-lazy-load startup-immediate
  - Create large disk store (100K rows)
  - Open with lazy loading — measure time to "ready"
  - Verify ready in < 1 second (only index loaded)
  - Verify rows accessible on demand afterward
```

#### T2.3: Trigger Parking Tests

```
test-trigger-data-wait basic
  - Create trigger for transaction that references unloaded row
  - Verify trigger is parked (ovsdb_trigger_try returns false)
  - Load the row via worker pool
  - Run triggers again — verify trigger completes

test-trigger-data-wait timeout
  - Create trigger for unloaded row with 5s timeout
  - Do NOT load the row
  - Verify trigger times out and returns error to client

test-trigger-data-wait multiple
  - Park 10 triggers waiting for different rows
  - Load rows one at a time
  - Verify each trigger resumes as its row becomes available
```

#### T2.4: Autotest — `ovsdb-lazy-load.at`

```
AT_SETUP([lazy load - server accepts connections immediately])
  # Create large database offline (insert 100K rows)
  # Start ovsdb-server with lazy loading
  # Immediately attempt ovsdb-client list-dbs — must succeed
  # Query specific rows — verify they load on demand
AT_CLEANUP

AT_SETUP([lazy load - concurrent client requests during load])
  # Start server with large DB + lazy loading
  # Launch 10 ovsdb-client processes simultaneously
  # Each queries different rows
  # Verify all get correct responses (no errors, no timeouts)
AT_CLEANUP

AT_SETUP([lazy load - transaction on unloaded rows])
  # Start server with lazy loading
  # Submit transaction that modifies an unloaded row
  # Verify transaction completes (after row loads)
  # Verify row has new value
AT_CLEANUP

AT_SETUP([lazy load - monitor on partially loaded DB])
  # Start server with lazy loading
  # Start monitor immediately
  # Verify initial snapshot delivered (may be chunked in Phase 4)
  # Verify incremental updates work normally
AT_CLEANUP
```

---

### Cross-Agent Integration Tests

#### TI.1: Full Pipeline — `ovsdb-integration.at`

```
AT_SETUP([integration - snapshot with disk store])
  # Start server with disk store + cache
  # Insert data, trigger standalone snapshot
  # Verify snapshot streams from disk (check logs for thread timing)
  # Restart server — verify data restored from snapshot
AT_CLEANUP

AT_SETUP([integration - lazy load + snapshot])
  # Start server with lazy loading
  # Before all rows loaded, trigger snapshot
  # Snapshot must iterate disk store (not cache)
  # Verify snapshot contains all rows
AT_CLEANUP

AT_SETUP([integration - transaction commit + cache + disk])
  # Start server with cache + disk store
  # Insert row via transaction
  # Verify row in cache AND on disk
  # Evict row from cache (reduce budget)
  # Query row — verify loaded from disk into cache
AT_CLEANUP

AT_SETUP([integration - worker pool + cache + trigger])
  # Start server with all Phase 2 components
  # Submit 100 transactions referencing unloaded rows
  # Verify all complete (triggers park → rows load → triggers resume)
  # Verify no deadlocks (timeout after 30s)
AT_CLEANUP
```

#### TI.2: C Integration Test — `test-integration.c`

```
test-integration full-pipeline
  - Create disk store, row cache, worker pool
  - Write 10,000 rows to disk store
  - Simulate ovsdb_table_get_row() with lazy loading:
    cache lookup → miss → submit worker job → wait → cache insert
  - Access all 10,000 rows
  - Verify all correct, no leaks, no races
  - Run under ASAN + TSAN

test-integration snapshot-during-load
  - Start lazy loading 10,000 rows via worker pool
  - While loading, trigger snapshot (iterate disk store)
  - Verify snapshot contains all 10,000 rows
  - Verify loading completes without interference
```

---

### Sanitizer CI Configuration

**Add to `.ci/linux-build.sh`**:

```bash
if [ "$TSAN" ]; then
    CFLAGS_TSAN="-fno-omit-frame-pointer -fno-common -fsanitize=thread"
    CFLAGS_FOR_OVS="${CFLAGS_FOR_OVS} ${CFLAGS_TSAN}"
fi
```

**Add to `tests/atlocal.in`**:
```bash
TSAN_OPTIONS='halt_on_error=true:log_path=sanitizers:second_deadlock_stack=1:$TSAN_OPTIONS'
export TSAN_OPTIONS
```

**GitHub Actions matrix** (`.github/workflows/build-and-test.yml`):
Add TSAN build alongside existing ASAN/UBSAN builds:
```yaml
- compiler: gcc
  opts: --disable-ssl
  sanitizers: thread
```

### Test Execution Summary

| Test Category | Framework | Sanitizers | Count |
|--------------|-----------|------------|-------|
| Phase 0 unit | C (`test-snapshot-threading.c`) | ASAN, UBSAN | 5 tests |
| Phase 0 integration | Autotest (`ovsdb-snapshot.at`) | ASAN | 3 tests |
| Phase 1 disk store unit | C (`test-disk-store.c`) | ASAN, UBSAN | 14 tests |
| Phase 1 cache unit | C (`test-row-cache.c`) | ASAN, UBSAN | 13 tests |
| Phase 1 Agent 2+3 integration | C | ASAN | 1 test |
| Phase 1 integration | Autotest (`ovsdb-disk-store.at`) | ASAN | 2 tests |
| Phase 2 worker pool unit | C (`test-worker-pool.c`) | ASAN, TSAN | 11 tests |
| Phase 2 lazy load unit | C | ASAN, TSAN | 4 tests |
| Phase 2 trigger parking | C | ASAN | 3 tests |
| Phase 2 integration | Autotest (`ovsdb-lazy-load.at`) | ASAN | 4 tests |
| Migration | Autotest (`ovsdb-migration.at`) | ASAN | 5 tests |
| Phase 3 JSON cache unit | C (`test-json-cache.c`) | ASAN | 6 tests |
| Phase 3 deferred send unit | C (`test-deferred-send.c`) | ASAN, TSAN | 4 tests |
| Phase 3 integration | Autotest | ASAN | 2 tests |
| Cross-agent integration | Autotest (`ovsdb-integration.at`) | ASAN | 4 tests |
| Cross-agent integration | C (`test-integration.c`) | ASAN, TSAN | 2 tests |
| **TOTAL** | | | **83 tests** |

### Memory/Safety Guarantees Per Test Category

| Guarantee | Tool | What It Catches |
|-----------|------|-----------------|
| Heap buffer overflow | ASAN | Out-of-bounds read/write on heap |
| Stack buffer overflow | ASAN | Out-of-bounds on stack |
| Use-after-free | ASAN + `MALLOC_PERTURB_=165` | Access to freed memory |
| Double-free | ASAN | Freeing same pointer twice |
| Memory leaks | ASAN `detect_leaks=1` | Unreachable allocated memory |
| Null pointer deref | ASAN + UBSAN | Dereferencing NULL |
| Integer overflow | UBSAN | Signed integer overflow |
| Data races | TSAN | Unsynchronized shared memory access |
| Lock order inversion | TSAN | Potential deadlocks (A→B, B→A) |
| Thread leaks | TSAN | Threads not joined before exit |
| Use-after-return | ASAN `detect_stack_use_after_return=1` | Stack variable access after function returns |
| Uninitialized memory | Valgrind (supplementary) | Reading uninitialized bytes |
