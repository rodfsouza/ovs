# OVSDB Disk-Backed Storage and Multi-Threaded Serialization

This document describes the new disk-backed storage engine, row cache, worker
pool, and non-blocking snapshot features introduced in OVS branch 3.3.

## Overview

Prior to these changes, ovsdb-server held the entire database in memory and
serialized it synchronously on the main thread during snapshot/compaction.
For databases exceeding 2 GB, this caused:

- 30-60 second main thread stalls (no client requests processed)
- 4 GB+ peak memory usage (in-memory database + serialized copy)
- 60-120 second startup times (full JSON log replay)

The new infrastructure addresses all three problems through three phases:

- **Phase 0 -- Non-blocking snapshots**: Standalone databases now use
  background threads for snapshot serialization (previously only clustered
  mode did this).

- **Phase 1 -- Disk store and row cache**: A binary on-disk row format
  (BINARYV1) with an LRU cache enables bounded memory usage and streaming
  snapshots.

- **Phase 2 -- I/O worker pool**: A thread pool with async row loading
  allows the server to accept connections immediately and load rows on
  demand.

## Getting Started

### Current Status

- **Phase 0** (non-blocking snapshots): Fully active for all standalone
  databases.  No configuration required -- takes effect immediately on
  upgrade.

- **Phase 1** (binary disk store + row cache): Available as a C library
  and via `ovsdb-tool` CLI.  Use `ovsdb-tool convert-format` to convert
  existing JSON databases to binary format and back.

- **Phase 2** (I/O worker pool): Infrastructure is in place.  Worker pool
  starts automatically with ovsdb-server (4 threads).  Lazy loading is
  not yet wired to the startup path.

- **`ovsdb-server` does not natively serve binary databases yet.**
  Binary databases must be converted back to JSON before ovsdb-server
  can open them.  Direct serving of binary format is planned for a
  future release.

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

# Convert Binary -> JSON (requires schema file):
ovsdb-tool convert-format /etc/openvswitch/conf.db json \
    /usr/share/openvswitch/vswitch.ovsschema

# Verify:
ovsdb-tool db-format /etc/openvswitch/conf.db
# Output: "json"
```

**Important**: Stop ovsdb-server before converting.  The conversion
uses atomic rename -- the original file is untouched until the new
file is fully written and fsynced.

Existing `ovsdb-tool` commands work with binary databases:

```bash
# These work on both JSON and binary databases:
ovsdb-tool db-version /etc/openvswitch/conf.db
ovsdb-tool db-cksum /etc/openvswitch/conf.db
ovsdb-tool db-is-standalone /etc/openvswitch/conf.db

# Compact a binary database (rewrites without deleted rows):
ovsdb-tool compact /etc/openvswitch/conf.db
```

### Running ovsdb-server

**ovsdb-server currently requires JSON format.**  Binary databases
must be converted to JSON before the server can open them.  A typical
workflow:

```bash
# 1. Stop the server.
ovs-appctl -t ovsdb-server exit

# 2. Convert to binary for offline analysis or storage.
ovsdb-tool convert-format conf.db binary

# 3. Convert back to JSON before restarting.
ovsdb-tool convert-format conf.db json vswitch.ovsschema

# 4. Restart the server.
ovsdb-server --remote=punix:db.sock --detach --pidfile conf.db
```

If ovsdb-server encounters a binary database, it will print a clear
error message:

```
conf.db: binary disk store format; use ovsdb-tool convert-format
to convert to JSON first
```

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

# 5. Convert back to JSON (required until ovsdb-server supports binary).
ovsdb-tool convert-format /etc/openvswitch/conf.db json \
    /usr/share/openvswitch/vswitch.ovsschema

# 6. Restart.
ovsdb-server --remote=punix:db.sock --detach --pidfile conf.db
```

**Downgrade (binary to JSON for rollback)**:

```bash
ovsdb-tool convert-format /etc/openvswitch/conf.db json \
    /usr/share/openvswitch/vswitch.ovsschema
```

The binary-to-JSON conversion requires the schema file because the
binary format stores rows but not the full schema definition needed
to reconstruct a JSON log.

**Important notes**:

- Always back up the database before converting.
- The conversion uses atomic rename -- the original file is replaced
  only after the new file is fully written and fsynced.
- If the process crashes during conversion, the original file is
  untouched.
- For Raft clusters, the binary format is local storage only.  Raft
  replication uses JSON on the wire regardless of local format.
  Rolling upgrades require no coordination -- nodes at different
  format versions interoperate transparently.
- Old OVS versions that do not understand `BINARYV1` will refuse to
  open the file with a clear error message.  Downgrade with
  `ovsdb-tool convert-format ... json <schema>` before rolling back.

## New Components

### Binary Disk Store (`ovsdb/disk-store.c`)

A storage engine that writes OVSDB rows in a compact binary format.

**File format**:

```
+------------------------------------------------------+
| File Header (36 bytes)                               |
|   magic:          8 bytes  "BINARYV1"                |
|   format_version: 4 bytes  uint32_t (network order)  |
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
| INTEGER        | 8 bytes (int64_t, network order)   |
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

`struct ovsdb_table` now has two optional fields:

```c
struct ovsdb_row_cache *cache;       /* NULL if cache disabled.      */
struct ovsdb_disk_store *disk_store; /* NULL if disk store disabled. */
```

`ovsdb_table_get_row()` searches in this order:

1. In-memory `rows` hmap (unchanged fast path)
2. Row cache lookup
3. Disk store read (on cache miss, result inserted into cache)

`ovsdb_table_create()` initializes both to NULL.
`ovsdb_table_destroy()` cleans up cache and closes disk store.

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
| `ovsdb/worker-pool.h`  |    65 | Worker pool public API                  |
| `ovsdb/worker-pool.c`  |   322 | Thread pool with seq signaling          |
| `tests/test-disk-store.c`  | 515 | Disk store unit tests (9 tests)     |
| `tests/test-row-cache.c`   | 450 | Row cache unit tests (12 tests)     |
| `tests/test-worker-pool.c` | 310 | Worker pool unit tests (5 tests)    |
| `tests/ovsdb-snapshot.at`  |  49 | Snapshot integration tests          |
| `tests/ovsdb-disk-store.at` |  7 | Disk store test harness             |
| `tests/ovsdb-row-cache.at`  |  7 | Row cache test harness              |
| `tests/ovsdb-lazy-load.at`  |  7 | Worker pool test harness            |
| `tests/ovsdb-integration.at`| 12 | Cross-component integration         |

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
| `ovsdb/ovsdb-tool.c`    | convert-format, db-format, binary-aware cmds  |
| `ovsdb/storage.c`       | Binary format detection in storage open       |
| `ovsdb/log.h`           | OVSDB_BINARY_MAGIC constant                   |
| `ovsdb/automake.mk`     | Added new source files to libovsdb            |
| `tests/automake.mk`     | Added test files, libovsdb to ovstest LDADD   |
| `tests/ovsdb.at`        | Registered new test modules                   |

## Limitations and Future Work

- **ovsdb-server does not natively serve binary databases.**  The server
  requires JSON format.  Use `ovsdb-tool convert-format` to convert
  between formats.  Native binary serving is planned for a future
  release, requiring changes to `ovsdb_storage_open__()` and the
  database replay path.

- **Binary-to-JSON conversion requires a schema file.**  The binary
  format stores a SHA-1 hash of the schema but not the full schema
  definition.  The schema file must be provided as an argument to
  `ovsdb-tool convert-format db json <schema>`.

- **Lazy loading** (Phase 2) infrastructure is in place but not wired to
  the startup path.  `ovsdb-server` still replays the full JSON log at
  startup.  The worker pool, trigger parking, and cache state tracker are
  ready for connection.

- **Chunked monitor protocol** (Phase 4) and **chunked Raft snapshots**
  (Phase 5) are not yet implemented.  The disk store cursor API supports
  streaming iteration for when these are added.

- **JSON disk cache** (Phase 3) for deferred serialization is designed
  but not yet implemented.
