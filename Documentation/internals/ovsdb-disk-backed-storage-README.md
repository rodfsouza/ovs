# OVSDB Disk-Backed Storage, Binary Streaming, and Three-Layer Architecture

This document describes the disk-backed storage engine, row cache, worker
pool, binary streaming protocol, and three-layer data access architecture
introduced in OVS branches 3.3-multithread and 3.3-three-layer-refactor.

## Overview

Prior to these changes, ovsdb-server held the entire database in memory and
serialized it synchronously on the main thread during snapshot/compaction.
For databases exceeding 2 GB, this caused:

- 30-60 second main thread stalls (no client requests processed)
- 4 GB+ peak memory usage (in-memory database + serialized copy)
- 60-120 second startup times (full JSON log replay)
- Clients downloading all rows even when querying a single row by UUID

The infrastructure addresses these problems through multiple phases:

- **Phase 0 -- Non-blocking snapshots**: Standalone databases now use
  background threads for snapshot serialization (previously only clustered
  mode did this).

- **Phase 1 -- Disk store and row cache**: A binary on-disk row format
  (BINARYV1) with a clock-sweep LRU cache enables bounded memory usage
  and on-demand row loading.

- **Phase 2 -- I/O worker pool**: A configurable thread pool (`--num-workers`)
  with async row loading allows the server to accept connections immediately
  and load rows on demand.

- **Phase 3 -- Binary streaming protocol**: A binary wire protocol
  (`0xDB` magic byte) coexists with JSON-RPC on the same TCP connection.
  Worker threads stream initial snapshots as binary ROW_BATCH frames,
  fully unblocking the main thread.  Clients auto-negotiate binary via
  `monitor_cond_since` V3 extension.

- **Phase 4 -- Three-layer data access**: Storage engine (pure disk I/O),
  index engine (BLOOM + HASH with clustered index model), and query engine
  (plan + execute + EXPLAIN) replace the fragmented direct access pattern.
  All callers go through the query engine.  Index configuration files
  declare additional indexes beyond the schema.

## Getting Started

### Current Status

- **Phase 0** (non-blocking snapshots): Fully active for all standalone
  databases.  No configuration required -- takes effect immediately on
  upgrade.

- **Phase 1** (binary disk store + row cache): Available via
  `ovsdb-tool convert-format` CLI and the C library.  Clock-sweep LRU
  cache with configurable atom budget and background sweeper thread.

- **Phase 2** (I/O worker pool + lazy loading): Fully wired.  Worker
  pool configurable via `--num-workers=N` (default 4).  Triggers park
  while waiting for row data and resume automatically.

- **Phase 3** (binary streaming protocol): Binary transport enabled by
  default for all IDL clients.  Worker threads stream initial snapshots
  as binary ROW_BATCH frames.  Conditioned monitors (UUID/name lookup)
  use the JSON path with index pushdown.

- **Phase 4** (three-layer architecture): Storage engine, index engine,
  and query engine replace direct disk-store/bloom/cache calls.  HASH
  indexes auto-detected from schema.  Optional `--index-config` for
  declaring additional indexes.  Single-pass index build at startup.

- **Native binary serving**: ovsdb-server can open and serve BINARYV1
  databases directly using the `--disk-store` flag.  The storage layer
  auto-detects binary format.

- **Clustered (Raft) mode is NOT supported** with binary format.  See
  the "Limitations" section for details.

### Quick Start: Non-Blocking Snapshots (Phase 0)

No action needed.  After upgrading to this branch, standalone
ovsdb-server instances automatically use background threads for
snapshot compaction.  You can verify this by checking the server log
for the "Compaction thread" message:

```bash
# Start ovsdb-server as usual:
ovsdb-server --remote=punix:/var/run/openvswitch/db.sock \
             --pidfile --detach conf.db

# Trigger a compaction:
ovs-appctl -t ovsdb-server ovsdb-server/compact

# Check the log -- you should see:
#   "Compaction thread started."
#   "Compaction thread finished in <N> ms."
#   "Database compaction took <N>ms (init: <N>ms, write: <N>ms, thread: <N>ms)"
grep "Compaction thread" /var/log/openvswitch/ovsdb-server.log
```

The server remains fully responsive during compaction.  Clients can
read and write the database while the snapshot is being serialized in
the background.

### Converting Databases with ovsdb-tool (Phase 1)

Use `ovsdb-tool convert-format` to convert between JSON and binary:

```bash
# Check the current format of a database:
ovsdb-tool db-format /etc/openvswitch/conf.db
# Output: "json"

# Convert JSON -> Binary:
ovsdb-tool convert-format /etc/openvswitch/conf.db binary

# Verify:
ovsdb-tool db-format /etc/openvswitch/conf.db
# Output: "binaryv1"

# Convert Binary -> JSON (schema file is optional):
ovsdb-tool convert-format /etc/openvswitch/conf.db json

# Verify:
ovsdb-tool db-format /etc/openvswitch/conf.db
# Output: "json"
```

**Important**: Stop ovsdb-server before converting.  The conversion
uses atomic rename -- the original file is untouched until the new
file is fully written and fsynced.

### Creating Binary Databases Directly

Use the `--format` flag with `ovsdb-tool create` to create a new
database in binary format without a separate conversion step:

```bash
# Create a binary database:
ovsdb-tool create --format binary conf.db vswitch.ovsschema

# Verify:
ovsdb-tool db-format conf.db
# Output: "binaryv1"

# Create a JSON database (default, unchanged):
ovsdb-tool create conf.db vswitch.ovsschema
```

### ovsdb-tool Commands and Binary Format

All `ovsdb-tool` commands now fully support binary databases.
Commands that write output (`compact`, `convert`) preserve the input
format -- a binary database stays binary after compaction or schema
conversion.

```bash
# These all work on both JSON and binary databases:
ovsdb-tool db-name /etc/openvswitch/conf.db
ovsdb-tool db-version /etc/openvswitch/conf.db
ovsdb-tool db-cksum /etc/openvswitch/conf.db
ovsdb-tool db-is-standalone /etc/openvswitch/conf.db
ovsdb-tool needs-conversion /etc/openvswitch/conf.db vswitch.ovsschema

# Compact a binary database (preserves binary format):
ovsdb-tool compact /etc/openvswitch/conf.db

# Convert schema on a binary database (preserves binary format):
ovsdb-tool convert /etc/openvswitch/conf.db new-schema.ovsschema

# Inspect a binary database (use -m for row UUIDs, -mm for row data):
ovsdb-tool show-log /etc/openvswitch/conf.db
ovsdb-tool -m show-log /etc/openvswitch/conf.db
ovsdb-tool -mm show-log /etc/openvswitch/conf.db
```

The `show-log` command adapts to the database format.  For JSON
databases it shows the sequential transaction log.  For binary
databases (which have no transaction log) it dumps the current state
of each table, with increasing detail at higher verbosity levels.

### Running ovsdb-server with Binary Databases

ovsdb-server natively opens and serves BINARYV1 databases.  Use the
`--disk-store` flag to enable the row cache and async loading:

```bash
# 1. Create and populate a JSON database.
ovsdb-tool create conf.db vswitch.ovsschema
# ... insert data via ovs-vsctl or ovsdb-client ...

# 2. Stop the server.
ovs-appctl -t ovsdb-server exit

# 3. Convert to binary.
ovsdb-tool convert-format conf.db binary

# 4. Start the server with binary support.
ovsdb-server --disk-store \
    --remote=punix:/var/run/openvswitch/db.sock \
    --pidfile --detach conf.db

# 5. Use normally — all ovs-vsctl / ovsdb-client commands work.
ovs-vsctl show
ovsdb-client list-dbs unix:/var/run/openvswitch/db.sock
```

**What `--disk-store` does**:

- Attaches a per-table LRU row cache (1M atoms budget by default)
- Populates the cache index from the disk store's UUID index (no
  I/O -- reads only the in-memory index built at open time)
- Rows are loaded from disk **on demand** when first accessed
- The I/O worker pool (4 background threads) loads rows
  asynchronously -- the main thread is not blocked
- Client requests that reference unloaded rows are **parked**
  (not rejected) and automatically retried once the row loads

**Without `--disk-store`**: the storage layer still auto-detects
binary format and opens it, but without the row cache and async
loading.  All rows are accessible via synchronous disk reads.

**Reverting to JSON format**:

```bash
ovs-appctl -t ovsdb-server exit
ovsdb-tool convert-format conf.db json
ovsdb-server --remote=punix:db.sock --pidfile --detach conf.db
```

### How Row Lookup Works (Three-Layer Architecture)

After the three-layer refactoring, `ovsdb_table_get_row` uses the
query engine for all disk-backed row access:

```
1. Client sends transact/query
2. ovsdb_table_get_row(table, uuid):
   a. Check in-memory hmap (transaction modifications) → found? return
   b. If storage_engine + index_set available:
      → ovsdb_query_engine_lookup_uuid(storage, indexes, cache, table, uuid)
        i.   BLOOM index: bloom.contains(uuid) → false? return NULL
        ii.  CACHE: cache_lookup(uuid) → hit? return cached row
        iii. STORAGE: storage_engine_read_row(uuid) → pread at offset
        iv.  CACHE: cache_insert(row) → return cached pointer
   c. Not found → return NULL
3. On cache miss: <1ms latency (single pread)
4. On cache hit: zero allocations, zero disk I/O
```

For monitor queries with conditions (UUID, name), the query engine's
planning selects the optimal path:

- **UUID condition** → `POINT_LOOKUP` (bloom + cache + pread)
- **Name condition + HASH index** → `INDEX_LOOKUP` (index → entry → pread)
- **No condition** → `FULL_SCAN` (binary streaming via worker threads)

### Binary Streaming for Monitors

When a client connects with binary transport (auto-negotiated by
default), unconditioned monitors use worker-based streaming:

```
1. Client sends monitor_cond_since with {"format":"binary"}
2. Server sends 4-element V3 ack: [found, txn_id, {}, {"format":"binary"}]
3. Worker threads open disk cursors and serialize rows to binary batches
4. Main thread drains batches via mutex+seq and sends as binary frames
5. When all workers done → send INITIAL_END with txn_id
6. Client accumulates ROW_BATCH events until INITIAL_END
7. IDL processes complete snapshot → has_ever_connected = true
```

The main thread is **never blocked** by disk I/O during binary streaming.
Conditioned monitors (UUID/name lookup) use the synchronous JSON path
with index pushdown via `ovsdb_table_query`.

### Using the Binary Disk Store C API (Phase 1)

The disk store is also available as a C library for programmatic use:

```c
#include "ovsdb/disk-store.h"

/* Create or open a binary database. */
struct ovsdb_disk_store *ds = ovsdb_disk_store_open(
    "mydb.bin", schema);

/* Write rows. */
ovsdb_disk_store_write_row(ds, row);

/* Read a row by UUID. */
struct ovsdb_row *row = ovsdb_disk_store_read_row(
    ds, table, &uuid);

/* Iterate all rows of a table. */
struct ovsdb_disk_store_cursor *c;
c = ovsdb_disk_store_cursor_open(ds, "mytable");
while ((row = ovsdb_disk_store_cursor_next(c, table))) {
    /* process row */
}
ovsdb_disk_store_cursor_close(c);

/* Close. */
ovsdb_disk_store_close(ds);
```

### Using the Worker Pool (Phase 2)

The I/O worker pool starts automatically when ovsdb-server launches.
It creates 4 background threads (`OVSDB_IO_WORKER_THREADS`) for async
disk I/O.  The pool integrates with the main loop:

- `ovsdb_worker_pool_run()` is called each iteration to deliver
  completed job results to the main thread.
- `ovsdb_worker_pool_wait()` registers with `poll_block()` so the
  main loop wakes up when worker results are ready.

To submit work to the pool from application code:

```c
#include "ovsdb/worker-pool.h"

/* Define a worker function (runs in background thread). */
static void *
my_load_row(void *arg)
{
    struct uuid *uuid = arg;
    /* ... load from disk ... */
    return row;
}

/* Define a completion callback (runs on main thread). */
static void
my_row_loaded(void *result, void *aux)
{
    struct ovsdb_row *row = result;
    struct ovsdb_table *table = aux;
    /* Insert into cache, resume parked triggers, etc. */
}

/* Submit the job. */
ovsdb_worker_pool_submit(io_worker_pool,
                         my_load_row, uuid_copy,
                         my_row_loaded, table);
```

### Verifying the New Features

After building OVS with these changes, run the full test suite:

```bash
# Build:
./boot.sh && ./configure --enable-Werror && make -j4

# Run all tests:
make check TESTSUITEFLAGS="-j4"

# Run only the new tests:
make check TESTSUITEFLAGS="-k disk-store"
make check TESTSUITEFLAGS="-k row-cache"
make check TESTSUITEFLAGS="-k worker-pool"
make check TESTSUITEFLAGS="-k snapshot"
make check TESTSUITEFLAGS="-k migration"
make check TESTSUITEFLAGS="-k binary-serve"
make check TESTSUITEFLAGS="-k lazy-load"

# Or run the C unit tests directly:
tests/ovstest test-disk-store
tests/ovstest test-row-cache
tests/ovstest test-worker-pool
```

Each test prints `test-<name>: ok` on success.

### Verifying Non-Blocking Snapshots in Production

To confirm that standalone snapshots are truly non-blocking in a
running system:

```bash
# 1. Start ovsdb-server with a large database.
ovsdb-server --remote=punix:db.sock --detach --pidfile conf.db

# 2. In one terminal, trigger compaction:
ovs-appctl -t ovsdb-server ovsdb-server/compact &

# 3. In another terminal, immediately run a query:
ovsdb-client list-dbs unix:db.sock

# The query should return instantly, even if the database is large.
# Previously, it would block for 30-60 seconds during compaction.

# 4. Check timing in the log:
grep "Database compaction" ovsdb-server.log
# Output: "Database compaction took 5200ms (init: 50ms, write: 200ms, thread: 4950ms)"
#
# "init" = time spent cloning on main thread (fast)
# "thread" = time spent serializing in background (does not block)
# "write" = time spent writing to disk on main thread (fast)
```

### Migration Path for Existing Deployments

**Upgrade (JSON to binary)**:

```bash
# 1. Stop ovsdb-server.
ovs-appctl -t ovsdb-server exit

# 2. Back up the database.
cp /etc/openvswitch/conf.db /etc/openvswitch/conf.db.bak

# 3. Convert.
ovsdb-tool convert-format /etc/openvswitch/conf.db binary

# 4. Verify.
ovsdb-tool db-format /etc/openvswitch/conf.db   # "binaryv1"
ovsdb-tool db-version /etc/openvswitch/conf.db  # schema version

# 5. Start with binary support.
ovsdb-server --disk-store \
    --remote=punix:/var/run/openvswitch/db.sock \
    --pidfile --detach /etc/openvswitch/conf.db
```

**Downgrade (binary to JSON for rollback)**:

```bash
# 1. Stop ovsdb-server.
ovs-appctl -t ovsdb-server exit

# 2. Convert back to JSON.
ovsdb-tool convert-format /etc/openvswitch/conf.db json

# 3. Restart without --disk-store.
ovsdb-server --remote=punix:db.sock --pidfile --detach conf.db
```

**Important notes**:

- Always back up the database before converting.
- The conversion uses atomic rename -- the original file is replaced
  only after the new file is fully written and fsynced.
- If the process crashes during conversion, the original file is
  untouched.
- **Do NOT use binary format with Raft clustered databases.**  The
  binary format is standalone only.  See "Limitations" section.
- Old OVS versions that do not understand `BINARYV1` will refuse to
  open the file with a clear error message.  Downgrade with
  `ovsdb-tool convert-format <db> json` before rolling back.

## New Components

### Binary Disk Store (`ovsdb/disk-store.c`)

A storage engine that writes OVSDB rows in a compact binary format.

**File format**:

```
+------------------------------------------------------+
| File Header (36 bytes)                               |
|   magic:          8 bytes  "BINARYV1"                |
|   format_version: 4 bytes  uint32_t (native byte order)  |
|   schema_hash:   20 bytes  SHA-1 of schema JSON      |
|   reserved:       4 bytes  (zero)                    |
+------------------------------------------------------+
| Row 0                                                |
|   Row Header (24 bytes)                              |
|     uuid:       16 bytes                             |
|     total_len:   4 bytes  (entire record incl hdr)   |
|     n_columns:   2 bytes                             |
|     flags:       2 bytes  (0x01 = deleted)           |
|   Table Name                                         |
|     name_len:    2 bytes                             |
|     name:        variable (UTF-8)                    |
|   Column Data (repeated per column)                  |
|     type:        1 byte   (OVSDB_TYPE_*)             |
|     datum_n:     4 bytes  (number of atoms)          |
|     keys:        variable                            |
|     values:      variable (maps only)                |
+------------------------------------------------------+
| Row 1 ...                                            |
+------------------------------------------------------+
```

**Atom encoding**:

| OVSDB Type     | On-Disk Size                       |
|----------------|------------------------------------|
| INTEGER        | 8 bytes (int64_t, native byte order)   |
| REAL           | 8 bytes (double, IEEE 754)         |
| BOOLEAN        | 1 byte                             |
| STRING         | 4 bytes length + UTF-8 bytes       |
| UUID           | 16 bytes (raw)                     |

**Key properties**:

- Append-only writes with in-memory UUID-to-offset index
- Index rebuilt from sequential scan on crash recovery
- Lazy deletion (flag in index, compaction reclaims space)
- Compaction via atomic rename (write temp file, rename over original)

**API**:

```c
/* Open / close. */
struct ovsdb_disk_store *ovsdb_disk_store_open(filename, schema);
void ovsdb_disk_store_close(ds);

/* Row operations. */
ovsdb_disk_store_write_row(ds, row);       /* Append to file. */
ovsdb_disk_store_delete_row(ds, uuid);     /* Lazy delete.    */
ovsdb_disk_store_read_row(ds, table, uuid);/* Read by UUID.   */

/* Cursor iteration (for snapshots). */
ovsdb_disk_store_cursor_open(ds, table_name);
ovsdb_disk_store_cursor_next(cursor, table);
ovsdb_disk_store_cursor_close(cursor);

/* Maintenance. */
ovsdb_disk_store_compact(ds);              /* Rewrite file.   */
```

### Row Cache (`ovsdb/row-cache.c`)

An LRU cache bounded by total atom count, with row pinning to prevent
eviction during transactions.

**Features**:

- O(1) lookup by UUID via hmap
- Atom-weighted eviction: rows with more atoms (larger maps/sets) are
  costlier. The cache evicts least-recently-used unpinned rows first.
- Pin/unpin: Rows referenced by an active transaction are pinned. Pinned
  rows cannot be evicted. Unpin happens at transaction commit.
- Lazy-load state tracking: Each cache entry tracks whether its row is
  `UNLOADED` (on disk only), `LOADING` (worker job submitted), or
  `CACHED` (data in memory).

**API**:

```c
ovsdb_row_cache_create(max_atoms);
ovsdb_row_cache_destroy(cache);
ovsdb_row_cache_lookup(cache, uuid);       /* NULL on miss. */
ovsdb_row_cache_insert(cache, row, n_atoms);
ovsdb_row_cache_remove(cache, uuid);
ovsdb_row_cache_pin(cache, uuid);
ovsdb_row_cache_unpin(cache, uuid);

/* Lazy-load state (Phase 2). */
ovsdb_row_cache_get_state(cache, uuid);
ovsdb_row_cache_set_state(cache, uuid, state);
ovsdb_row_cache_add_unloaded(cache, uuid);

/* Stats. */
ovsdb_row_cache_n_atoms(cache);
ovsdb_row_cache_count(cache);
ovsdb_row_cache_hits(cache);
ovsdb_row_cache_misses(cache);
```

### Worker Pool (`ovsdb/worker-pool.c`)

A thread pool that integrates with the OVS poll-based main loop via
`seq_change()` / `seq_wait()`.

**How it works**:

```
Main Thread                       Worker Threads (N)
    |                                   |
    +-- submit(fn, arg) ----------> [pending queue]
    |                                   |
    |                               fn(arg) executes
    |                                   |
    |   [done queue] <------ result + seq_change()
    |       |
    +-- pool_run()
    |   done_fn(result, aux)
    |
    +-- pool_wait()
        seq_wait(done_seq, seqno)
```

Workers dequeue jobs under a mutex, execute outside the lock, enqueue
results, and signal the main thread via `seq_change()`.  The main thread
drains results in `pool_run()` and registers for wakeup in `pool_wait()`.

**API**:

```c
ovsdb_worker_pool_create(n_threads, name);
ovsdb_worker_pool_destroy(pool);
ovsdb_worker_pool_submit(pool, fn, arg, done_fn, aux);
ovsdb_worker_pool_run(pool);     /* Call from main loop. */
ovsdb_worker_pool_wait(pool);    /* Register with poll.  */
ovsdb_worker_pool_has_pending(pool);
```

The pool is created in `main()` of `ovsdb-server.c` with
`OVSDB_IO_WORKER_THREADS` (default 4) threads.  `pool_run()` and
`pool_wait()` are called in the main loop alongside trigger and storage
processing.

## Changes to Existing Code

### Non-Blocking Standalone Snapshots (`ovsdb/ovsdb.c`)

`ovsdb_snapshot()` previously had two code paths:

- Standalone (`applied_index == 0`): Synchronous -- called
  `ovsdb_to_txn_json()` inline, blocking the main thread.
- Clustered (`applied_index > 0`): Asynchronous -- cloned the database,
  spawned `compaction_thread()`, signaled completion via `seq`.

The standalone path has been removed.  Both modes now use the same
three-step async pattern:

1. **Launch**: `ovsdb_clone_data()` + `ovs_thread_create("compaction")`
2. **Poll**: Main loop checks `ovsdb_snapshot_ready()` each iteration
3. **Harvest**: `xpthread_join()` + `ovsdb_storage_store_snapshot()`

### Table Integration (`ovsdb/table.h`, `ovsdb/table.c`)

`struct ovsdb_table` now has these disk-backed fields:

```c
struct ovsdb_row_cache *cache;                /* Clock-sweep LRU cache.   */
struct ovsdb_disk_store *disk_store;          /* Shared disk store ptr.   */
struct ovsdb_bloom_filter *bloom;             /* UUID existence filter.   */
struct ovsdb_storage_engine *storage_engine;  /* Three-layer: disk I/O.   */
struct ovsdb_index_set *index_set;            /* Three-layer: BLOOM+HASH. */
```

`ovsdb_table_get_row()` uses the query engine when available:

1. In-memory `rows` hmap (transaction modifications)
2. `ovsdb_query_engine_lookup_uuid()` → bloom → cache → pread → cache insert

`ovsdb_table_create()` initializes all to NULL.
`ovsdb_table_destroy()` cleans up all resources.

### Transaction Integration (`ovsdb/transaction.c`)

- **`ovsdb_txn_row_commit()`**: After updating indexes, writes modified
  rows to the disk store (if enabled) and unpins them from the cache.
  Disk store errors are logged but do not fail the transaction (the
  in-memory commit is already final).

- **`ovsdb_txn_row_modify()`**: Pins the row in the cache to prevent
  eviction while the transaction is in progress.

### Trigger Extension (`ovsdb/trigger.h`, `ovsdb/trigger.c`)

`struct ovsdb_trigger` has a new `waiting_for_data` boolean.  When a
trigger references rows that are not yet loaded from disk, it can be
parked by setting this flag.  `ovsdb_trigger_run()` retries parked
triggers on each main-loop iteration, alongside the existing checks for
timeout, progress, and forwarding.

### Row Atom Counting (`ovsdb/row.h`, `ovsdb/row.c`)

New helper `ovsdb_row_count_atoms()` returns the total number of atoms
across all columns of a row (counting both keys and values for map
types).  Used by the cache for atom-weighted eviction costing.

## Building

The new modules are compiled as part of `ovsdb/libovsdb.la`.  No new
external dependencies are required.

```bash
./boot.sh
./configure [--enable-Werror]
make -j4
```

To build with sanitizers for testing:

```bash
# Address Sanitizer (memory errors, leaks)
./configure CFLAGS="-g -O2 -fsanitize=address -fno-omit-frame-pointer"
make -j4

# Thread Sanitizer (data races)
./configure CFLAGS="-g -O2 -fsanitize=thread -fno-omit-frame-pointer"
make -j4
```

## Testing

### Unit Tests

Three C test programs are compiled into `tests/ovstest`:

```bash
# Run all three test suites:
ovstest test-disk-store     # 9 tests: CRUD, iteration, compaction,
                            #          persistence, edge cases
ovstest test-row-cache      # 12 tests: LRU eviction, pinning,
                            #           state tracker, atom counting
ovstest test-worker-pool    # 5 tests: job dispatch, callbacks,
                            #          stress (10K jobs/8 threads),
                            #          destroy-with-pending, 0-thread edge
```

Each prints `test-<name>: ok` on success.

### Integration Tests (Autotest)

Five `.at` test modules registered in `tests/ovsdb.at`:

```bash
# Run all OVSDB tests:
make check TESTSUITEFLAGS="-k ovsdb"

# Run specific test categories:
make check TESTSUITEFLAGS="-k snapshot"     # Non-blocking snapshot
make check TESTSUITEFLAGS="-k disk-store"   # Disk store unit tests
make check TESTSUITEFLAGS="-k row-cache"    # Row cache unit tests
make check TESTSUITEFLAGS="-k worker-pool"  # Worker pool tests
make check TESTSUITEFLAGS="-k integration"  # Cross-component tests
```

Test descriptions:

| Test Module            | Tests | Coverage                                    |
|------------------------|------:|---------------------------------------------|
| `ovsdb-snapshot.at`    |     2 | Server responsive during snapshot; data      |
|                        |       | integrity after restart                      |
| `ovsdb-disk-store.at`  |     1 | Runs `ovstest test-disk-store`               |
| `ovsdb-row-cache.at`   |     1 | Runs `ovstest test-row-cache`                |
| `ovsdb-lazy-load.at`   |     1 | Runs `ovstest test-worker-pool`              |
| `ovsdb-migration.at`   |     5 | convert-format, db-format, round-trip,       |
|                        |       | db-is-standalone, forward compat             |
| `ovsdb-binary-serve.at`|     5 | --disk-store startup, auto-detect, query     |
|                        |       | data, insert, lazy-load C tests              |
| `ovsdb-integration.at` |     2 | Disk store + row cache together              |

### Running Under Sanitizers

The test infrastructure already supports ASAN and UBSAN via
`tests/atlocal.in`:

```bash
# ASAN (catches use-after-free, buffer overflow, leaks):
ASAN_OPTIONS='detect_leaks=1' make check TESTSUITEFLAGS="-j4"

# With MALLOC_PERTURB_ (fills freed memory with 0xA5 pattern):
# Already enabled by default in atlocal.in.
```

TSAN can be enabled for the worker pool tests by building with
`-fsanitize=thread`.

## Memory Safety Guarantees

| Guarantee              | Mechanism                                    |
|------------------------|----------------------------------------------|
| No double ownership    | Cache does not own rows in `table->rows`;    |
|                        | only rows loaded from disk are cache-owned   |
| No use-after-free      | Rows pinned during transactions; unpinned at  |
|                        | commit; eviction skips pinned rows            |
| No NULL dereference    | Eviction guards `entry->row` before calling  |
|                        | `ovsdb_row_destroy()`; disk-store path guarded|
|                        | by `table->cache && table->disk_store`       |
| Thread safety          | Worker pool uses `ovs_mutex` for queues;     |
|                        | `seq_change()`/`seq_wait()` for signaling;   |
|                        | no shared mutable state between main and     |
|                        | worker threads                               |
| Crash safety           | Disk store is append-only with `fdatasync()`;|
|                        | index rebuilt from sequential scan on         |
|                        | recovery; compaction uses atomic rename       |

## File Inventory

### New Files

| File                    | Lines | Description                             |
|-------------------------|------:|-----------------------------------------|
| `ovsdb/disk-store.h`   |    65 | Disk store public API                   |
| `ovsdb/disk-store.c`   | 1,244 | Binary row storage engine               |
| `ovsdb/row-cache.h`    |    68 | Row cache public API                    |
| `ovsdb/row-cache.c`    |   400 | LRU cache with pinning and state        |
| `ovsdb/lazy-load.h`    |    45 | Lazy-load subsystem public API          |
| `ovsdb/lazy-load.c`    |   120 | Async row loading via worker pool       |
| `ovsdb/worker-pool.h`  |    65 | Worker pool public API                  |
| `ovsdb/worker-pool.c`  |   322 | Thread pool with seq signaling          |
| `tests/test-disk-store.c`  | 515 | Disk store unit tests (9 tests)     |
| `tests/test-row-cache.c`   | 450 | Row cache unit tests (12 tests)     |
| `tests/test-lazy-load.c`    | 280 | Lazy-load unit tests (5 tests)  |
| `tests/test-worker-pool.c` | 310 | Worker pool unit tests (5 tests)    |
| `tests/ovsdb-snapshot.at`  |  49 | Snapshot integration tests          |
| `tests/ovsdb-disk-store.at` |  7 | Disk store test harness             |
| `tests/ovsdb-row-cache.at`  |  7 | Row cache test harness              |
| `tests/ovsdb-lazy-load.at`  |  7 | Worker pool test harness            |
| `tests/ovsdb-integration.at`| 12 | Cross-component integration         |

**Binary Streaming and Three-Layer (Phase 3-4):**

| File                          | Lines | Description                        |
|-------------------------------|------:|------------------------------------|
| `lib/binary-codec.h/c`       |   530 | Atom/datum/row serialization       |
| `lib/binary-protocol.h/c`    |    70 | Binary wire frame format           |
| `ovsdb/storage-engine.h/c`   |   200 | Pure disk I/O facade               |
| `ovsdb/index-engine.h/c`     |   620 | BLOOM + HASH clustered indexes     |
| `ovsdb/query-engine.h/c`     |   690 | Plan + execute + EXPLAIN           |
| `ovsdb/index-config.h/c`     |   290 | INI config file parser             |
| `ovsdb/index-ovn-nb.conf`    |    79 | OVN NB index config (30 tables)    |
| `ovsdb/index-ovn-sb.conf`    |    95 | OVN SB index config (34 tables)    |
| `ovsdb/index-ovn-ic-nb.conf` |    15 | OVN IC-NB config (4 tables)        |
| `ovsdb/index-ovn-ic-sb.conf` |    34 | OVN IC-SB config (9 tables)        |
| `tests/test-binary-codec.c`  |   900 | Codec tests + 10K stress           |
| `tests/test-index-engine.c`  |   472 | HASH index tests + 10K stress      |
| `tests/test-query-engine.c`  |   339 | Plan selection tests               |
| `tests/test-storage-engine.c`|   148 | Storage engine lifecycle tests      |
| `tests/test-index-config.c`  |   312 | Config parsing tests               |

### Modified Files

| File                     | Change                                       |
|--------------------------|----------------------------------------------|
| `ovsdb/ovsdb.c`         | Unified standalone/clustered snapshot path    |
| `ovsdb/table.h`         | Added `cache` and `disk_store` fields         |
| `ovsdb/table.c`         | Cache/disk-store lookup, create/destroy       |
| `ovsdb/transaction.c`   | Disk write-back, cache pin/unpin              |
| `ovsdb/trigger.h`       | Added `waiting_for_data` flag                 |
| `ovsdb/trigger.c`       | Retry data-waiting triggers                   |
| `ovsdb/row.h`           | Declared `ovsdb_row_count_atoms()`            |
| `ovsdb/row.c`           | Implemented `ovsdb_row_count_atoms()`         |
| `ovsdb/ovsdb-server.c`  | Worker pool init/run/wait/destroy             |
| `ovsdb/ovsdb-tool.c`    | Full binary format support: create --format,  |
|                         | format-preserving compact/convert, binary      |
|                         | show-log, db-name, needs-conversion fixes      |
| `ovsdb/storage.c`       | Binary format detection in storage open       |
| `ovsdb/log.h`           | OVSDB_BINARY_MAGIC constant                   |
| `ovsdb/automake.mk`     | Added new source files to libovsdb            |
| `tests/automake.mk`     | Added test files, libovsdb to ovstest LDADD   |
| `tests/ovsdb.at`        | Registered new test modules                   |

## Limitations

### Raft Clustered Mode Not Supported

The binary disk store format is **standalone only**.  Clustered (Raft)
databases cannot use `--disk-store`.  The reasons:

1. **Raft replication is JSON-based.**  `raft_command_execute()` takes
   `struct json *` and replicates it as JSON to all peers.
   `raft_install_snapshot_request` sends the full database as a JSON
   blob.  The Raft protocol has no concept of binary rows.

2. **Binary replaces the log, not complements it.**  The current design
   uses binary as the primary storage format, replacing the JSON log
   entirely.  In clustered mode, the Raft log is the primary storage
   and cannot be replaced.

3. **A future "local cache" architecture** could use binary as a local
   optimization under the Raft layer — Raft continues to replicate
   JSON; each node materializes its local copy into binary.  This is
   a different architecture and is not yet implemented.

**If you attempt to use `--disk-store` with a clustered database**,
the server will open it but the Raft protocol will not function.
Only use `--disk-store` with standalone databases.

### Transactions in Binary Mode

Transactions submitted via `ovsdb-client transact` while running in
binary disk-store mode execute against the in-memory cache and write
through to the binary file on disk.  However:

- **No write-ahead log (WAL)**:  If the server crashes mid-transaction,
  the binary file may contain a partial write.  On restart, the index
  is rebuilt from a sequential scan, and incomplete records are
  discarded.
- **No transaction history**:  Binary mode does not maintain the
  transaction log used for incremental monitor updates.  Monitors
  connected to a binary-mode database receive full snapshots, not
  incremental diffs.

### Other Limitations

- **Multi-column indexes** are not yet supported by the index engine.
  Schema entries like `indexes:[["datapath","tunnel_key"]]` are skipped.
  Only single-column indexes are auto-detected.

- **Compound HASH indexes** (future OVSDB_IDX_BTREE) for range queries
  are designed but not implemented.

- **Index configuration via `--index-config`** is not yet wired into
  the `ovsdb-server` CLI option parser.  Use `ovsdb_index_set_from_schema_with_config()`
  programmatically or via the C API.

- **Row cache budget is configurable** but defaults to 1,000,000 atoms.
  Use `--cache-max-atoms` for runtime tuning (if wired).

### Related Documentation

- `ovsdb-multithread-architecture.md` — Comprehensive reference covering
  all components: cache, workers, binary streaming, three-layer architecture.

- `ovsdb-disk-store-architecture.md` — Disk store internals: file format,
  indexes, compaction, data flow diagrams.

- `ovsdb-three-layer-architecture.md` — Three-layer refactoring reference:
  storage engine, index engine, query engine, clustered index model,
  index configuration, single-pass build.

- `plan-three-layer-data-access.md` — Original design plan (future tense,
  implementation guidance) for the three-layer refactoring.
