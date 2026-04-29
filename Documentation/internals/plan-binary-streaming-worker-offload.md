# Plan: Complete Binary Streaming — Client Opt-In, Table/Column Filtering, Worker Offload, Tests

## Context

The binary streaming protocol (Phases 1-6) is implemented but unused: `binary_transport` defaults to `false`, so all clients fall back to the synchronous JSON path that blocks the main thread. Additionally, the worker scans ALL database tables instead of only monitored ones, and uses per-UUID `ovsdb_table_get_row()` which causes cache thrashing at ~2M atoms.

**End-to-end flow after this plan:**

```
ovn-nbctl list Logical_Router
  → IDL sends monitor_cond_since with {"format": "binary"} (Part A: default on)
    → ovsdb-server: monitor_create() detects binary
      → Sends JSON ack reply
      → Calls ovsdb_jsonrpc_monitor_send_binary_initial()
        → Iterates ONLY monitored tables (Part B: table filter fix)
        → Submits one binary_stream_job per table to worker pool
          → Worker thread runs disk_cursor_stream_worker_fn() (Part C: new function)
            → Opens disk cursor (snapshot of index entries)
            → For each row: pread() → deserialize → binary_serialize → batch
            → Enqueues batches under mutex, signals seq
            → Destroys row immediately (no cache interaction)
          → Main thread session_run() drains batches → sends as binary frames
          → When all done → sends INITIAL_END with txn_id
    → Client IDL receives binary ROW_BATCH frames
      → ovsdb_cs_process_binary_row_batch() deserializes
      → Feeds to IDL as table-updates2
    → ovn-nbctl prints rows from IDL
```

**Main thread is NEVER blocked by disk I/O.** All `pread()` calls happen on worker threads.

---

## Part A: Enable Binary Transport by Default

### File: `lib/ovsdb-cs.c`

**Change:** In `ovsdb_cs_create()`, set `cs->binary_transport = true`.

Currently `ovsdb_cs` is created with `xzalloc()`, so `binary_transport` defaults to `false` (zero). Change to explicit initialization:

```c
/* In ovsdb_cs_create(), after xzalloc: */
cs->binary_transport = true;
```

**Backward compatibility:**
- **New client + old server:** Server ignores the 5th parameter in `monitor_cond_since`. Returns standard 3-element V3 reply. Client already handles 3 or 4 element replies (fix from earlier commit).
- **New client + new server:** Binary negotiation succeeds. Server sends binary frames, client processes them.
- **Old client + new/old server:** Old client never sends 5th param. Unchanged.

### File: `lib/ovsdb-cs.h`

No changes needed — `ovsdb_cs_set_binary_transport()` already exists as an override.

---

## Part B: Fix Table Filtering in Binary Streaming

### Problem

`ovsdb_jsonrpc_monitor_send_binary_initial()` at line 2230 iterates `m->db->tables` — **all tables in the database**. A monitor for `Logical_Router` should only scan `Logical_Router`, not `Logical_Switch`, `ACL`, `Load_Balancer`, etc.

### Fix 1: Add monitor table accessor

**File: `ovsdb/monitor.h`** — add:

```c
/* Iterates over the monitored tables in 'dbmon'. For each table,
 * calls 'cb' with the table name, the ovsdb_table pointer, the
 * monitored column set, and 'aux'.  The column set contains only
 * the columns this monitor is tracking (not all table columns).
 *
 * This is used by the binary streaming subsystem to submit worker
 * jobs only for tables the monitor cares about. */
typedef void (*ovsdb_monitor_table_cb)(
    const char *table_name,
    const struct ovsdb_table *table,
    const struct ovsdb_column_set *monitored_columns,
    void *aux);
void ovsdb_monitor_for_each_table(const struct ovsdb_monitor *,
                                   ovsdb_monitor_table_cb, void *aux);
```

**File: `ovsdb/monitor.c`** — implement:

```c
void
ovsdb_monitor_for_each_table(const struct ovsdb_monitor *dbmon,
                              ovsdb_monitor_table_cb cb, void *aux)
{
    struct shash_node *node;

    SHASH_FOR_EACH (node, &dbmon->tables) {
        struct ovsdb_monitor_table *mt = node->data;
        struct ovsdb_column_set columns;
        size_t i;

        ovsdb_column_set_init(&columns);
        for (i = 0; i < mt->n_monitored_columns; i++) {
            if (mt->columns[i].monitored) {
                ovsdb_column_set_add(&columns, mt->columns[i].column);
            }
        }

        cb(node->name, mt->table, &columns, aux);
        ovsdb_column_set_destroy(&columns);
    }
}
```

### Fix 2: Update `ovsdb_jsonrpc_monitor_send_binary_initial()`

**File: `ovsdb/jsonrpc-server.c`**

Replace the current implementation that iterates `m->db->tables` with a callback-based approach using the new accessor:

```c
/* Callback context for submitting binary stream jobs. */
struct binary_initial_ctx {
    struct ovsdb_jsonrpc_session *session;
    struct ovsdb_jsonrpc_monitor *monitor;
    struct ovsdb_worker_pool *pool;
    size_t n_submitted;
};

static void
binary_initial_submit_table(const char *table_name,
                             const struct ovsdb_table *table,
                             const struct ovsdb_column_set *columns,
                             void *aux_)
{
    struct binary_initial_ctx *ctx = aux_;
    struct binary_stream_job *job = xzalloc(sizeof *job);

    job->table = (struct ovsdb_table *) table;
    job->table_name = xstrdup(table_name);
    ovsdb_column_set_clone(&job->columns, columns);
    ovs_list_init(&job->batches);
    ovs_mutex_init(&job->mutex);
    job->seq = seq_create();
    job->done = false;
    job->session = ctx->session;
    job->monitor = ctx->monitor;

    ovs_list_push_back(&ctx->monitor->stream_jobs, &job->job_node);

    ovsdb_worker_pool_submit(ctx->pool,
                             disk_cursor_stream_worker_fn,
                             job, binary_stream_done_fn, NULL);
    ctx->n_submitted++;
}

static void
ovsdb_jsonrpc_monitor_send_binary_initial(
    struct ovsdb_jsonrpc_session *s,
    struct ovsdb_jsonrpc_monitor *m,
    bool initial OVS_UNUSED)
{
    struct ovsdb_worker_pool *pool = ovsdb_lazy_load_get_pool();

    if (!pool) {
        VLOG_WARN("no worker pool available for binary streaming");
        return;
    }

    m->binary_initial_streaming = true;
    ovs_list_init(&m->stream_jobs);
    m->stream_seqno = 0;

    struct binary_initial_ctx ctx = {
        .session = s,
        .monitor = m,
        .pool = pool,
        .n_submitted = 0,
    };
    ovsdb_monitor_for_each_table(m->dbmon, binary_initial_submit_table, &ctx);

    /* Initialize seqno. */
    if (!ovs_list_is_empty(&m->stream_jobs)) {
        struct binary_stream_job *first;
        first = CONTAINER_OF(ovs_list_front(&m->stream_jobs),
                             struct binary_stream_job, job_node);
        m->stream_seqno = seq_read(first->seq);
    }

    VLOG_INFO("binary initial snapshot: submitted %"PRIuSIZE
              " table jobs to worker pool", ctx.n_submitted);
}
```

### Also need: `ovsdb_column_set_clone()`

**File: `ovsdb/column.h`** — add declaration:

```c
void ovsdb_column_set_clone(struct ovsdb_column_set *dst,
                             const struct ovsdb_column_set *src);
```

**File: `ovsdb/column.c`** — implement:

```c
void
ovsdb_column_set_clone(struct ovsdb_column_set *dst,
                        const struct ovsdb_column_set *src)
{
    ovsdb_column_set_init(dst);
    for (size_t i = 0; i < src->n_columns; i++) {
        ovsdb_column_set_add(dst, src->columns[i]);
    }
}
```

---

## Part C: Worker Strategy — Cache-Through vs. Disk Cursor Bypass

### The Two Worker Approaches

There are two distinct patterns for how a worker reads rows during a binary initial snapshot. The choice depends on whether the monitor tracks a **subset** of tables or **all** tables.

#### Option 1: Cache-Through Worker (for single-table / subset monitors)

The worker calls `ovsdb_table_get_row(uuid)` per row, which goes:
```
bloom → cache lookup → cache hit? return cached row
                      → cache miss? pread() → deserialize → cache insert → return
```

**Pros:**
- Warms the cache for subsequent single-row lookups (e.g., `ovn-nbctl get Logical_Router <uuid>`)
- Subsequent queries on the same table hit the cache instead of disk
- Query-burst mode (2x base budget) limits cache pollution — the cache was designed for this
- The cache + clock-sweep eviction naturally ages out cold entries

**Cons:**
- For very large tables (>cache budget), inline eviction fires on every insert → CPU thrashing
- Each row acquisition write-locks the cache rwlock briefly
- Still sequentially calling `pread()` per UUID (random I/O, not sequential)

**Best for:** `ovn-nbctl list Logical_Router` — monitors one table, cache warm-up benefits future queries.

#### Option 2: Disk Cursor Bypass Worker (for full-DB monitors)

The worker opens a disk cursor and iterates sequentially, bypassing the cache entirely:
```
cursor_open() → snapshot of index entries
while (cursor_next()):
    pread() → deserialize → serialize_binary → destroy row
```

**Pros:**
- Sequential I/O (cursor reads entries in index order → potentially better disk locality)
- Zero cache interaction (no lock contention, no eviction, no thrashing)
- Predictable memory: only one row alive at a time on the worker thread
- No cache pollution: doesn't evict hot entries needed by other queries

**Cons:**
- Does NOT warm the cache — subsequent single-row lookups still hit disk
- If the client later does per-row operations, those will be cache-cold
- Reads ALL rows in the table (no condition filtering at the disk level)

**Best for:** northd / ovn-controller — monitors ALL tables, initial snapshot is one-time, subsequent updates arrive via monitor change tracking (not per-row lookups).

### Decision: Use Both, Selected Per-Monitor

The monitor submission function chooses the worker based on the monitor's scope:

```c
/* In binary_initial_submit_table(): */
bool full_db_monitor = (monitor_table_count == db_table_count);

if (full_db_monitor && job->table->disk_store) {
    /* Full-DB monitor: bypass cache for bulk read. */
    ovsdb_worker_pool_submit(pool, disk_cursor_stream_worker_fn,
                             job, binary_stream_done_fn, NULL);
} else if (job->table->disk_store) {
    /* Subset monitor: read through cache for warm-up. */
    ovsdb_worker_pool_submit(pool, cache_through_stream_worker_fn,
                             job, binary_stream_done_fn, NULL);
} else {
    /* In-memory table. */
    ovsdb_worker_pool_submit(pool, memory_stream_worker_fn,
                             job, binary_stream_done_fn, NULL);
}
```

### Worker Function 1: `cache_through_stream_worker_fn` (for subset monitors)

Iterates UUIDs from the disk-store index, reads each row through the cache (cache hit → fast, cache miss → `pread()` + cache insert). Data enters the cache and warms it for future single-row lookups. Query-burst mode limits pollution.

**File: `ovsdb/jsonrpc-server.c`**

```c
/* Worker for subset monitors (e.g., ovn-nbctl list Logical_Router).
 * Reads through cache: cache hit = memory read, cache miss = pread
 * + cache insert.  Warms the cache for subsequent per-row lookups.
 *
 * Thread safety:
 *   - ovsdb_table_get_row() acquires cache rwlock internally (safe)
 *   - On cache miss, pread() is thread-safe (no shared file offset)
 *   - table->schema->columns is immutable
 *   - ovsdb_binary_serialize_row reads row->fields (pointer stable
 *     while cache entry exists, which is guaranteed because the
 *     worker holds no locks between get_row and serialize — the
 *     main thread cannot evict while session_run hasn't drained
 *     batches and re-entered poll_block, and the sweeper only
 *     evicts under wrlock which blocks while we hold rdlock
 *     through get_row's lookup)
 *   - batches enqueued under job->mutex, signaled via seq
 */
static void *
cache_through_stream_worker_fn(void *arg)
{
    struct binary_stream_job *job = arg;
    struct binary_stream_worker_ctx wctx;

    wctx.job = job;
    wctx.rows_in_batch = 0;
    ovsdb_binary_buf_init(&wctx.batch);

    /* Enter query burst to raise cache budget for this scan. */
    if (job->table->cache) {
        ovsdb_row_cache_enter_query_burst(job->table->cache);
    }

    /* Iterate UUIDs from the in-memory disk-store index (no disk I/O).
     * For each UUID, read through cache → serialize → batch. */
    ovsdb_disk_store_for_each_uuid(
        job->table->disk_store, job->table_name,
        cache_through_row_cb, &wctx);

    if (job->table->cache) {
        ovsdb_row_cache_exit_burst(job->table->cache);
    }

    binary_stream_flush_batch(&wctx);
    ovsdb_binary_buf_destroy(&wctx.batch);

    ovs_mutex_lock(&job->mutex);
    job->done = true;
    ovs_mutex_unlock(&job->mutex);
    seq_change(job->seq);
    return NULL;
}

/* Per-UUID callback for cache-through worker.
 *
 * Table filtering: ovsdb_disk_store_for_each_uuid() is called with
 * job->table_name, so only UUIDs belonging to THIS table are yielded.
 * No rows from other tables are ever seen.
 *
 * Column filtering: ovsdb_binary_serialize_row() is called with
 * job->columns, which contains ONLY the columns this monitor tracks
 * (set by ovsdb_monitor_for_each_table → ovsdb_column_set_clone in
 * binary_initial_submit_table). Non-monitored columns are never
 * serialized or sent over the wire.
 */
static void
cache_through_row_cb(const struct uuid *uuid, void *aux)
{
    struct binary_stream_worker_ctx *wctx = aux;
    const struct ovsdb_row *row;

    /* Read through cache: hit = memory, miss = pread + cache insert.
     * This is the same path as ovsdb_table_get_row():
     *   bloom filter → cache lookup → cache miss? → pread → cache insert
     * The row enters the cache, warming it for future single-row lookups. */
    row = ovsdb_table_get_row(wctx->job->table, uuid);
    if (!row) {
        return;
    }

    /* Serialize ONLY monitored columns (job->columns is the filtered set). */
    ovsdb_binary_serialize_row(&wctx->batch, uuid,
                                row->fields, &wctx->job->columns, true);
    wctx->rows_in_batch++;

    if (wctx->batch.size >= BINARY_BATCH_MAX_BYTES
        || wctx->rows_in_batch >= BINARY_BATCH_MAX_ROWS) {
        binary_stream_flush_batch(wctx);
    }
}
```

### Worker Function 2: `disk_cursor_stream_worker_fn` (for full-DB monitors)

Opens a disk cursor and iterates sequentially, bypassing the cache. Rows are serialized and destroyed immediately — zero cache interaction.

```c
/* Worker for full-DB monitors (northd, ovn-controller).
 * Uses sequential disk cursor, bypasses cache entirely.
 * No cache pollution, no eviction thrashing.
 *
 * Thread safety:
 *   - cursor_open() copies index entries under index_rwlock (safe snapshot)
 *   - cursor_next() calls pread() (thread-safe, no shared file offset)
 *   - ovsdb_row_create() inside disk_store_read_row only accesses
 *     table->schema->columns (immutable)
 *   - row is serialized and destroyed immediately (no shared state)
 *   - batches enqueued under job->mutex, signaled via seq
 *
 * Does NOT touch: table->rows, table->cache, db->triggers
 */
static void *
disk_cursor_stream_worker_fn(void *arg)
{
    struct binary_stream_job *job = arg;
    struct binary_stream_worker_ctx wctx;

    wctx.job = job;
    wctx.rows_in_batch = 0;
    ovsdb_binary_buf_init(&wctx.batch);

    if (!job->table->disk_store) {
        /* In-memory table fallback: iterate hmap directly. */
        const struct ovsdb_row *row;
        HMAP_FOR_EACH (row, hmap_node, &job->table->rows) {
            const struct uuid *uuid = ovsdb_row_get_uuid(row);
            ovsdb_binary_serialize_row(&wctx.batch, uuid,
                                        row->fields, &job->columns,
                                        true);
            wctx.rows_in_batch++;
            if (wctx.batch.size >= BINARY_BATCH_MAX_BYTES
                || wctx.rows_in_batch >= BINARY_BATCH_MAX_ROWS) {
                binary_stream_flush_batch(&wctx);
            }
        }
        goto done;
    }

    /* Open disk cursor (snapshot of index entries). */
    {
        struct ovsdb_disk_store_cursor *cursor;
        cursor = ovsdb_disk_store_cursor_open(job->table->disk_store,
                                              job->table_name);
        if (!cursor) {
            VLOG_WARN("disk_cursor_stream: failed to open cursor for %s",
                      job->table_name);
            goto done;
        }

        struct ovsdb_row *row;
        while ((row = ovsdb_disk_store_cursor_next(cursor, job->table))) {
            const struct uuid *uuid = ovsdb_row_get_uuid(row);

            ovsdb_binary_serialize_row(&wctx.batch, uuid,
                                        row->fields, &job->columns,
                                        true);
            wctx.rows_in_batch++;

            /* Destroy immediately — no cache insertion. */
            ovsdb_row_destroy(row);

            if (wctx.batch.size >= BINARY_BATCH_MAX_BYTES
                || wctx.rows_in_batch >= BINARY_BATCH_MAX_ROWS) {
                binary_stream_flush_batch(&wctx);
            }
        }
        ovsdb_disk_store_cursor_close(cursor);
    }

done:
    binary_stream_flush_batch(&wctx);
    ovsdb_binary_buf_destroy(&wctx.batch);

    ovs_mutex_lock(&job->mutex);
    job->done = true;
    ovs_mutex_unlock(&job->mutex);
    seq_change(job->seq);
    return NULL;
}
```

### Worker Selection in `binary_initial_submit_table()`

The submission callback (Part B) selects the worker function based on whether the monitor tracks all database tables or a subset:

```c
/* Add to struct binary_initial_ctx: */
size_t n_db_tables;     /* Total tables in the database. */
size_t n_mon_tables;    /* Tables in this monitor. */

/* In binary_initial_submit_table(): */
bool full_db = (ctx->n_mon_tables == ctx->n_db_tables);

ovsdb_worker_pool_fn_type worker_fn;
if (!table->disk_store) {
    /* In-memory table: use disk_cursor worker (has hmap fallback). */
    worker_fn = disk_cursor_stream_worker_fn;
} else if (full_db) {
    /* Full-DB monitor (northd, ovn-controller): bypass cache. */
    worker_fn = disk_cursor_stream_worker_fn;
} else {
    /* Subset monitor (ovn-nbctl list Logical_Router): warm cache. */
    worker_fn = cache_through_stream_worker_fn;
}

ovsdb_worker_pool_submit(ctx->pool, worker_fn,
                         job, binary_stream_done_fn, NULL);
```

### Summary: When Each Worker Is Used

| Client | Monitor scope | Worker | Cache? |
|--------|--------------|--------|--------|
| `ovn-nbctl list Logical_Router` | 1 table | `cache_through` | Yes — warms cache for future lookups |
| `ovn-nbctl list ACL` | 1 table | `cache_through` | Yes |
| northd | All NB tables | `disk_cursor` | No — avoids massive cache pollution |
| ovn-controller | All SB tables | `disk_cursor` | No |
| Custom client monitoring 3 tables | Subset | `cache_through` | Yes |

### Main Thread Integration (Already Exists)

The existing `session_run_binary_streaming()` / `session_wait_binary_streaming()` / `binary_stream_drain_batches()` / `binary_stream_cleanup_jobs()` functions work unchanged — they just drain binary batches from the mutex-protected queue and send them as binary frames. The worker function swap is transparent to them.

### Diagram: Worker Thread Offload (Both Paths)

```
CLIENT                    MAIN THREAD                  WORKER THREAD(s)
  |                           |                              |
  |-- monitor_cond_since ---->|                              |
  |   + {"format":"binary"}   |                              |
  |                           |                              |
  |                     monitor_create()                     |
  |                     detect binary=true                   |
  |                     send JSON ack reply                  |
  |<---- JSON ack ------------|                              |
  |                           |                              |
  |                     send_binary_initial()                |
  |                     for_each_table(dbmon):               |
  |                       (only MONITORED tables)            |
  |                       create binary_stream_job           |
  |                       select worker function:            |
  |                         full_db? → disk_cursor_worker    |
  |                         subset?  → cache_through_worker  |
  |                       submit to worker_pool ------------>|
  |                                                          |
  |                     return to event loop                 |
  |                     (main thread is FREE)                |
  |                           |                              |
  |                           |  [cache_through_worker]:     |
  |                           |    for_each_uuid:            |
  |                           |      get_row(cache→disk)     |
  |                           |      serialize_row(binary)   |
  |                           |      batch → mutex → seq     |
  |                           |                              |
  |                           |  [disk_cursor_worker]:       |
  |                           |    cursor_open(disk_store)   |
  |                           |    while(cursor_next):       |
  |                           |      pread() → row           |
  |                           |      serialize_row(binary)   |
  |                           |      ovsdb_row_destroy(row)  |
  |                           |      batch → mutex → seq     |
  |                           |                              |
  |                     session_run():                       |
  |                     drain_batches():                     |
  |                       mutex_lock                         |
  |                       pop batch                          |
  |                       mutex_unlock                       |
  |<---- ROW_BATCH ----------|                              |
  |                           |                              |
  |                     (repeat drain on next session_run)   |
  |                           |              ...more rows... |
  |                           |              job->done=true  |
  |                           |              seq_change()    |
  |                           |                              |
  |                     session_run():                       |
  |                     drain_batches(): all_done=true       |
  |<---- INITIAL_END --------|                              |
  |                           |                              |
  |                     cleanup_jobs()                       |
  |                     streaming=false                      |
```

### Wait Integration (Already Exists)

In `session_wait()`, `ovsdb_jsonrpc_session_wait_binary_streaming()` registers `seq_wait(job->seq, m->stream_seqno)` for each streaming job. This wakes the main thread when a new batch is available or when the worker finishes.

### Disconnect Safety (Already Exists)

If the client disconnects during streaming, `monitor_destroy()` calls `binary_stream_cleanup_jobs()` which:
1. Drains and discards remaining batches under mutex
2. Destroys mutex, seq, columns
3. Frees the job
The worker may still be running, but it only writes to `job->batches` under mutex and reads from `job->done` — both are safe.

---

## Part D: Phase 7 Tests (Non-Negotiable)

Implement ALL tests from the original plan Phase 7.

### New Files

**`tests/ovsdb-binary-test.c`** — Test helper program for unit tests:

```c
/* Exercises binary codec and protocol functions directly.
 *
 * Usage: ovsdb-binary-test <test-name>
 *
 * Tests:
 *   atoms    — round-trip every atom type × {native, nbo}
 *   datums   — round-trip scalar, set, map × types
 *   rows     — round-trip multi-column rows
 *   edges    — empty string, INT64_MIN/MAX, empty set/map
 *   frames   — binary frame encode/decode
 *   invalid  — truncated header, wrong magic, oversized payload
 */
```

Test functions:
```c
static void test_atoms(void)
{
    /* For each type in {INTEGER, REAL, BOOLEAN, STRING, UUID}:
     *   1. Create atom with known value
     *   2. Serialize with ovsdb_binary_serialize_atom(nbo=false)
     *   3. Deserialize with ovsdb_binary_deserialize_atom(nbo=false)
     *   4. Compare values
     *   5. Repeat with nbo=true
     */
}

static void test_datums(void)
{
    /* For {scalar int, set of strings, map<string,int>}:
     *   1. Create datum with known values
     *   2. Serialize with ovsdb_binary_serialize_datum
     *   3. Deserialize with ovsdb_binary_deserialize_datum
     *   4. Compare with ovsdb_datum_equals()
     *   5. Test both nbo=false and nbo=true
     */
}

static void test_rows(void)
{
    /* 1. Create a table schema with 5 columns (int, string, bool, uuid, set)
     * 2. Create a row with known values
     * 3. Serialize with ovsdb_binary_serialize_row(nbo=true)
     * 4. Deserialize with ovsdb_binary_deserialize_row(nbo=true)
     * 5. Compare each column's datum
     */
}

static void test_edge_cases(void)
{
    /* Test: empty string, INT64_MIN, INT64_MAX, 0.0, -0.0, NaN,
     *       empty set (n=0), empty map (n=0),
     *       string with embedded NUL (should be rejected),
     *       max-length string (16MB boundary)
     */
}

static void test_frames(void)
{
    /* 1. Encode frame with ovsdb_binary_frame_encode()
     * 2. Decode with ovsdb_binary_frame_decode()
     * 3. Verify magic, version, msg_type, payload_len match
     * 4. Test each msg_type (INITIAL_BEGIN through UPDATE_BATCH)
     */
}

static void test_invalid_frames(void)
{
    /* 1. Truncated header (< 8 bytes) → decode returns error
     * 2. Wrong magic (0xAB instead of 0xDB) → decode returns error
     * 3. Wrong version (0xFF) → decode returns error
     * 4. Payload > OVSDB_BINARY_MAX_PAYLOAD → decode returns error
     * 5. Zero-length payload → should succeed
     */
}
```

**`tests/ovsdb-binary.at`** — Autotest module:

```m4
AT_BANNER([OVSDB binary codec and protocol tests])

dnl ========== Unit Tests (1-4) ==========

AT_SETUP([binary codec - atom round-trip])
AT_KEYWORDS([ovsdb binary codec])
AT_CHECK([ovsdb-binary-test atoms], [0], [ignore])
AT_CLEANUP

AT_SETUP([binary codec - datum round-trip])
AT_KEYWORDS([ovsdb binary codec])
AT_CHECK([ovsdb-binary-test datums], [0], [ignore])
AT_CLEANUP

AT_SETUP([binary codec - row round-trip])
AT_KEYWORDS([ovsdb binary codec])
AT_CHECK([ovsdb-binary-test rows], [0], [ignore])
AT_CLEANUP

AT_SETUP([binary codec - edge cases])
AT_KEYWORDS([ovsdb binary codec])
AT_CHECK([ovsdb-binary-test edges], [0], [ignore])
AT_CLEANUP

dnl ========== Wire Protocol Tests (5-8) ==========

AT_SETUP([binary protocol - frame round-trip])
AT_KEYWORDS([ovsdb binary protocol])
AT_CHECK([ovsdb-binary-test frames], [0], [ignore])
AT_CLEANUP

AT_SETUP([binary protocol - interleaved JSON and binary])
AT_KEYWORDS([ovsdb binary protocol])
dnl Start ovsdb-server, connect two clients:
dnl   client1: binary monitor
dnl   client2: JSON monitor
dnl Both should receive correct initial snapshots
AT_CHECK([...], [0], [ignore])
AT_CLEANUP

AT_SETUP([binary protocol - partial reads])
AT_KEYWORDS([ovsdb binary protocol])
AT_CHECK([ovsdb-binary-test partial], [0], [ignore])
AT_CLEANUP

AT_SETUP([binary protocol - invalid frames])
AT_KEYWORDS([ovsdb binary protocol])
AT_CHECK([ovsdb-binary-test invalid], [0], [ignore])
AT_CLEANUP

dnl ========== Integration Tests (9-21) ==========

AT_SETUP([ovsdb-server --num-workers])
AT_KEYWORDS([ovsdb binary workers])
dnl Start with --num-workers=2, verify via appctl
AT_CHECK([ovsdb-server --num-workers=2 ...])
AT_CHECK([ovs-appctl ovsdb-server/get-num-workers], [0], [2
])
dnl Start with --num-workers=8
AT_CHECK([ovsdb-server --num-workers=8 ...])
AT_CHECK([ovs-appctl ovsdb-server/get-num-workers], [0], [8
])
AT_CLEANUP

AT_SETUP([disk-store startup without warm-up])
AT_KEYWORDS([ovsdb binary disk-store])
dnl Start ovsdb-server with disk-store
dnl Verify no "lazy-load: warm-up" messages in log
dnl Verify cache starts empty (0 atoms via appctl stats)
AT_CLEANUP

AT_SETUP([on-demand cache fill from disk])
AT_KEYWORDS([ovsdb binary cache])
dnl 1. Create disk-store DB, insert 100 rows
dnl 2. Start ovsdb-server (cold cache)
dnl 3. Query single row by UUID → cache miss → disk read → cache insert
dnl 4. Query same row again → cache hit
dnl Verify via log messages or cache stats
AT_CLEANUP

AT_SETUP([bloom filter negative lookup])
AT_KEYWORDS([ovsdb binary bloom])
dnl Query non-existent UUID → bloom filter rejects → no disk I/O
AT_CLEANUP

AT_SETUP([cache shrink after eviction])
AT_KEYWORDS([ovsdb binary cache])
dnl 1. Insert many rows, fill cache to budget
dnl 2. Wait for sweeper to evict
dnl 3. Verify hmap shrinks (via stats command)
AT_CLEANUP

AT_SETUP([binary transport negotiation])
AT_KEYWORDS([ovsdb binary transport])
dnl 1. Start ovsdb-server with --num-workers=2
dnl 2. Connect client with monitor_cond_since + {"format": "binary"}
dnl 3. Verify reply contains {"format": "binary"} acknowledgment
AT_CLEANUP

AT_SETUP([binary initial snapshot - 10K rows])
AT_KEYWORDS([ovsdb binary snapshot])
dnl 1. Create disk-store DB with 10K rows
dnl 2. Start ovsdb-server
dnl 3. Connect JSON client (binary_transport=false) → get baseline
dnl 4. Connect binary client (binary_transport=true) → get snapshot
dnl 5. Compare: same row count, same data
AT_CLEANUP

AT_SETUP([binary incremental updates])
AT_KEYWORDS([ovsdb binary update])
dnl 1. Start with binary monitor
dnl 2. INSERT row via separate connection → verify binary update arrives
dnl 3. MODIFY row → verify binary update
dnl 4. DELETE row → verify binary update
AT_CLEANUP

AT_SETUP([JSON fallback for old clients])
AT_KEYWORDS([ovsdb binary fallback])
dnl 1. Connect client WITHOUT {"format":"binary"}
dnl 2. Verify standard JSON monitor reply
dnl 3. Insert row → verify JSON update notification
AT_CLEANUP

AT_SETUP([mixed binary and JSON clients])
AT_KEYWORDS([ovsdb binary mixed])
dnl 1. Connect client A with binary monitor
dnl 2. Connect client B with JSON monitor (same DB)
dnl 3. Insert row
dnl 4. Verify A gets binary update, B gets JSON update
AT_CLEANUP

AT_SETUP([worker streaming - incremental batch delivery])
AT_KEYWORDS([ovsdb binary streaming])
dnl 1. Create large table (10K rows)
dnl 2. Connect binary client
dnl 3. Verify multiple ROW_BATCH frames arrive (not one giant batch)
dnl 4. Verify log shows "sending binary initial batch" multiple times
AT_CLEANUP

AT_SETUP([session disconnect during binary streaming])
AT_KEYWORDS([ovsdb binary disconnect])
dnl 1. Create large table
dnl 2. Connect binary client
dnl 3. Immediately disconnect before INITIAL_END
dnl 4. Verify no crash, no leak (valgrind or ASAN)
AT_CLEANUP

AT_SETUP([concurrent transactions during binary streaming])
AT_KEYWORDS([ovsdb binary concurrent])
dnl 1. Create large table
dnl 2. Connect binary client (starts streaming)
dnl 3. While streaming, INSERT/DELETE rows via separate connection
dnl 4. Verify streaming completes without error
dnl 5. Verify subsequent incremental updates reflect the changes
AT_CLEANUP
```

### Build System Changes

**`tests/automake.mk`:**
- Add `ovsdb-binary-test` to `noinst_PROGRAMS` or `check_PROGRAMS`
- Add `ovsdb-binary-test.c` to sources
- Link against `lib/libopenvswitch.la` and `ovsdb/libovsdb.la`

**`tests/testsuite.at`:**
- Add `m4_include([tests/ovsdb-binary.at])` 

---

## Files to Modify

| File | Changes |
|------|---------|
| `lib/ovsdb-cs.c` | Default `binary_transport = true` |
| `ovsdb/monitor.h` | Add `ovsdb_monitor_for_each_table()` callback API |
| `ovsdb/monitor.c` | Implement `ovsdb_monitor_for_each_table()` |
| `ovsdb/column.h` | Add `ovsdb_column_set_clone()` |
| `ovsdb/column.c` | Implement `ovsdb_column_set_clone()` |
| `ovsdb/jsonrpc-server.c` | New `cache_through_stream_worker_fn()` + `disk_cursor_stream_worker_fn()`, rewrite `send_binary_initial()` to use monitor table iterator + worker selection based on monitor scope |
| `tests/ovsdb-binary-test.c` | **New** — unit test helper program (codec + protocol) |
| `tests/ovsdb-binary.at` | **New** — all 21+ autotest cases |
| `tests/testsuite.at` | Include `ovsdb-binary.at` |
| `tests/automake.mk` | Add test program + test source |

## Implementation Order

1. **Part B**: Table filtering fix (monitor accessor + column clone + update send_binary_initial)
2. **Part C**: New `disk_cursor_stream_worker_fn` (eliminates cache thrashing)
3. **Part A**: Enable binary by default (one-line change, flips the switch)
4. **Part D**: Tests — unit tests first (codec, protocol), then integration tests
5. **Compile + run tests**: `make -j4 && make check TESTSUITEFLAGS="-j4 -k binary"`

## Verification

1. `make -j4` — zero errors/warnings
2. `make check TESTSUITEFLAGS="-j4 -k binary"` — all new tests pass
3. `make check TESTSUITEFLAGS="-j4 -k ovsdb"` — no regressions
4. Manual: `ovn-nbctl list Logical_Router` on large disk-store DB:
   - Main thread stays responsive (other clients work during scan)
   - Binary frames visible in server log
   - Complete data returned to client
5. Manual: cache atom count does NOT spike during full-table scans
