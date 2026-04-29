# OVSDB Multithread Architecture Reference

This document describes all custom changes on `branch-3.3-multithread`,
covering disk-backed storage, clock-sweep caching, worker pools, binary
streaming protocol, and client-side binary transport.

---

## 1. Overview

Standard OVSDB holds all data in memory as JSON.  For databases exceeding
available RAM (e.g., OVN NB with millions of rows), this branch adds:

- **Disk-backed storage** (`ovsdb/disk-store.c`) — rows stored on disk in a
  compact binary format; only actively-used rows reside in memory.
- **Clock-sweep row cache** (`ovsdb/row-cache.c`) — bounded LRU cache with
  configurable atom budget and background eviction.
- **Worker thread pool** (`ovsdb/worker-pool.c`) — N-threaded async I/O for
  disk reads, serialization, and streaming.
- **Binary wire protocol** (`lib/binary-protocol.c`, `lib/binary-codec.c`) —
  replaces JSON for monitor data transport; ~10-40% bandwidth reduction.
- **Binary streaming** (`ovsdb/jsonrpc-server.c`) — worker-based initial
  snapshot delivery; main thread never blocked by disk I/O.

### Key Design Principles

1. **Cache starts cold** — no startup warm-up; rows loaded on demand.
2. **Workers do all disk I/O** — main thread only drains binary batches.
3. **Binary frames coexist with JSON** on the same TCP connection.
4. **Clients auto-negotiate binary** via `monitor_cond_since` V3 extension.

---

## 2. Disk-Backed Storage

**Files:** `ovsdb/disk-store.h`, `ovsdb/disk-store.c`

### File Format (BINARYV1)

```
[Header]
  magic:   "BINARYV1"
  schema:  JSON schema (length-prefixed)

[Row records] (append-only)
  Per row:
    uuid:         16 bytes
    total_len:    uint32
    n_columns:    uint16
    flags:        uint16 (bit 0 = deleted)
    column_data:  binary-codec encoded datums
```

### In-Memory Indexes

Built at startup by scanning the file once:

| Index | Structure | Purpose |
|-------|-----------|---------|
| UUID -> offset | `hmap` of `disk_store_index_entry` | O(1) row lookup by UUID |
| Bloom filter | `ovsdb_bloom_filter` (atomic bits) | Fast negative UUID check |
| Name -> UUID | `ovsdb_name_index` (hmap) | O(1) name-based lookup |

### Key Operations

| Operation | Function | I/O |
|-----------|----------|-----|
| Read row | `ovsdb_disk_store_read_row(store, table, uuid)` | Single `pread` |
| Write row | `ovsdb_disk_store_write_row(store, table, row)` | Append `pwrite` |
| Delete row | `ovsdb_disk_store_delete_row(store, uuid)` | Index flag only |
| Cursor scan | `cursor_open` / `cursor_next` / `cursor_close` | Sequential `pread` |
| Compaction | `ovsdb_disk_store_compact(store)` | Full rewrite |

### Cursor

`ovsdb_disk_store_cursor_open(store, table_name)` builds a filtered
array of index entries for the requested table at open time.  Iteration
via `cursor_next` walks this pre-filtered array — O(table_rows) with
direct `pread` per entry.

### Compaction

Rewrites the file ordered by table name then UUID.  Eliminates deleted
entries and reclaims space.  Rebuilds bloom filter and name index.

---

## 3. Row Cache

**Files:** `ovsdb/row-cache.h`, `ovsdb/row-cache.c`

### Architecture

Clock-sweep LRU bounded by **atom count** (not row count), since rows
vary in size.  Each row's cost = number of `ovsdb_datum` atoms.

```
                    ┌──────────────────────┐
                    │   Clock Buffer       │
                    │  [slot0] [slot1] ... │
                    │     ↑                │
                    │  clock_hand          │
                    └──────────────────────┘
                              │
            ┌─────────────────┼─────────────────┐
            │                 │                 │
     ┌──────┴──────┐  ┌──────┴──────┐  ┌──────┴──────┐
     │ Entry       │  │ Entry       │  │ Entry       │
     │ uuid        │  │ uuid        │  │ uuid        │
     │ row*        │  │ row*        │  │ row*        │
     │ n_atoms     │  │ n_atoms     │  │ n_atoms     │
     │ usage_count │  │ usage_count │  │ usage_count │
     │ state       │  │ state       │  │ state       │
     └─────────────┘  └─────────────┘  └─────────────┘
```

### Row States

```
UNLOADED ──load_request──→ LOADING ──success──→ CACHED
                               │                    │
                          failure (≤3)          eviction
                               │                    │
                               ↓                    ↓
                           UNLOADED             (removed)
                               │
                          failure (>3)
                               │
                               ↓
                             ERROR
```

### Eviction

- **Inline eviction:** `evict_one__()` after each insert when over budget.
  Scans clock buffer for entry with `usage_count == 0`.
- **Background sweeper:** Thread runs every 30s, decays usage counts
  under rdlock, evicts under wrlock.  Gradually shrinks burst budget.
- **Query burst:** `enter_query_burst()` temporarily raises budget to
  2x `base_max_atoms` for table scan accommodation.

### Key Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `base_max_atoms` | 1,000,000 | Normal atom budget |
| `high_water_atoms` | 10,000,000 | Maximum burst budget |
| `SWEEP_INTERVAL_MS` | 30,000 | Background sweep period |

---

## 4. Worker Pool

**Files:** `ovsdb/worker-pool.h`, `ovsdb/worker-pool.c`

### Architecture

```
Main Thread                    Worker Threads (N)
    │                               │
    ├── submit(fn, arg) ──────→ [pending queue]
    │                               │
    │                         fn(arg) → result
    │                               │
    │   [done queue] ◄──────── result
    │       │
    ├── pool_run()
    │   done_fn(result, aux) ← on main thread
    │
    └── pool_wait() ← wake main loop
```

### Configuration

```
ovsdb-server --num-workers=N    (default: OVSDB_IO_WORKER_THREADS)
```

Validate: `ovs-appctl ovsdb-server/get-num-workers`

---

## 5. Lazy-Load Subsystem

**Files:** `ovsdb/lazy-load.h`, `ovsdb/lazy-load.c`

Bridges the cache state machine and the worker pool for on-demand
row loading.

### Flow

1. `ovsdb_table_get_row()` finds row not in cache
2. Bloom filter says "may exist" → submit `ovsdb_lazy_load_request()`
3. Worker thread: `ovsdb_disk_store_read_row()` → deserialize
4. Main thread (done_fn): `ovsdb_row_cache_insert()` or record failure
5. Sets `db->run_triggers = true` → wakes parked triggers

### Trigger Parking

When a transaction references a row that is LOADING:
- Trigger is parked with `waiting_for_data = true`
- When the load completes, `db->run_triggers` wakes the trigger
- Trigger re-executes; row is now CACHED

---

## 6. Bloom Filter

**Files:** `ovsdb/bloom-filter.h`, `ovsdb/bloom-filter.c`

Thread-safe probabilistic set membership test for UUIDs.

- **7 hash functions**, 10 bits per key
- **Atomic bit operations** — no locks needed for concurrent lookups
- Built at startup from disk-store index
- Rebuilt after compaction

Used by `ovsdb_table_get_row()` to reject non-existent UUIDs without
any disk I/O (fast negative).

---

## 7. Secondary Name Index

**Files:** `ovsdb/disk-store.h` (name_index functions)

O(1) name→UUID lookup for string columns with `n_max == 1`.

```
"neutron-router-abc" ──hash──→ name_index hmap ──→ UUID
                                                     │
                                               ovsdb_table_get_row()
```

Built at startup by scanning the name column from disk.  Updated on
row insert/delete.  Used by query Path 2 (see below).

---

## 8. Query Execution Paths

**File:** `ovsdb/table.c` — `ovsdb_table_query()`

Three paths, selected by condition type:

### Path 1: UUID Exact Match

```
condition: [["_uuid","==","<uuid>"]]

ovsdb_table_get_row(table, uuid)
  → table->rows hmap (in-memory mods)
  → row_cache lookup
  → bloom filter (fast negative)
  → disk_store_read_row (pread)
  → cache insert
```

### Path 2: Name Index Lookup

```
condition: [["name","==","<string>"]]

ovsdb_name_index_find(name)
  → UUID
  → Path 1
```

### Path 3: Full Table Scan

```
condition: NULL or complex

Step A: yield rows from table->rows (in-memory)
Step B: open disk cursor, iterate all table rows
  → for each: check condition, yield match, optionally cache
```

Query burst mode is entered for unconditioned scans.  Conditioned
scans use the scan ring (temporary, no eviction of hot entries).

---

## 9. Binary Codec

**Files:** `lib/binary-codec.h`, `lib/binary-codec.c`

Serialization library for atoms, datums, and rows.  Supports both
native byte order (disk format) and network byte order (wire format)
via the `nbo` parameter.

### Atom Format

| Type | Size | Encoding |
|------|------|----------|
| INTEGER | 8 bytes | int64 (optionally big-endian) |
| REAL | 8 bytes | IEEE 754 double |
| BOOLEAN | 1 byte | 0 or 1 |
| STRING | 4+N bytes | uint32 length + UTF-8 bytes |
| UUID | 16 bytes | Raw UUID bytes |

### Datum Format

```
uint32   n          (number of key/value pairs)
atom[n]  keys       (each per key_type)
atom[n]  values     (only if value_type != VOID)
```

### Row Format (Network Transport)

```
uuid[16]         row UUID
uint16           n_columns
Per column:
  uint16+bytes   column name (length-prefixed)
  uint8          key_type (enum ovsdb_atomic_type)
  uint8          val_type (VOID for scalars/sets)
  datum          column data
```

### Sanity Limits

| Limit | Value |
|-------|-------|
| `BINARY_CODEC_MAX_STRING` | 16 MB |
| `BINARY_CODEC_MAX_DATUM_N` | 1,000,000 elements |

---

## 10. Binary Wire Protocol

**Files:** `lib/binary-protocol.h`, `lib/binary-protocol.c`

### Frame Format

```
Offset  Size  Field
------  ----  ----------------------------------
0       1     magic: 0xDB (never valid JSON first byte)
1       1     version: 0x01
2       1     msg_type (enum ovsdb_binary_msg_type)
3       1     flags (reserved, 0x00)
4       4     payload_len (uint32, network byte order)
8       N     payload (N = payload_len)
```

### Message Types

| Type | Value | Direction | Payload |
|------|-------|-----------|---------|
| INITIAL_BEGIN | 0x01 | S→C | monitor_id + table_name + n_rows |
| ROW_BATCH | 0x02 | S→C | table_name + n_rows + binary rows |
| INITIAL_END | 0x03 | S→C | txn_id (UUID) |
| UPDATE | 0x04 | S→C | update_type + table_name + row |
| UPDATE_BATCH | 0x05 | S→C | JSON-in-binary (transitional) |

### Coexistence with JSON-RPC

Between JSON messages, `jsonrpc_recv()` peeks the first byte:
- `{` or `[` (0x7B, 0x5B) → JSON parser
- `0xDB` → binary frame parser

Both protocols share the same TCP connection.

### Payload Size Limit

`OVSDB_BINARY_MAX_PAYLOAD = 64 MB`.  `ovsdb_binary_frame_decode()`
rejects frames exceeding this limit.

---

## 11. JSON-RPC Binary Extensions

**Files:** `lib/jsonrpc.h`, `lib/jsonrpc.c`

### Extended Message Structure

```c
struct jsonrpc_msg {
    /* Existing JSON fields... */

    /* Binary extensions: */
    bool        is_binary;
    uint8_t     binary_msg_type;
    uint8_t    *binary_payload;      /* Owned. */
    size_t      binary_payload_len;
};
```

### Binary Frame Reception

In `jsonrpc_recv()`, between messages:

```c
peek first byte:
  0xDB → accumulate 8-byte header
       → read payload_len bytes
       → create jsonrpc_msg with is_binary=true
  else → JSON parser (existing path)
```

### Binary Frame Send

```c
jsonrpc_send_binary(rpc, msg_type, payload, len);
jsonrpc_session_send_binary(session, msg_type, payload, len);
```

Builds 8-byte header + payload into `ofpbuf`, queues on output.
Respects backlog thresholds (same as JSON send).

---

## 12. Binary Streaming for Monitors

**File:** `ovsdb/jsonrpc-server.c`

### Negotiation (V3 Monitor)

Client sends `monitor_cond_since` with 5th parameter:
```json
{"format": "binary"}
```

Server replies with 4-element array:
```json
[found, last_txn_id, {}, {"format": "binary"}]
```

Element [2] is empty (rows come via binary).  Element [3] is the ack.

### Worker-Based Initial Snapshot

```
CLIENT                    MAIN THREAD                  WORKER THREAD(s)
  │                           │                              │
  │── monitor_cond_since ──→  │                              │
  │   + {"format":"binary"}   │                              │
  │                           │                              │
  │                     monitor_create()                     │
  │                     detect binary=true                   │
  │                     skip get_initial (no row scan!)      │
  │                     send JSON ack reply                  │
  │◄── [found,id,{},ack] ──  │                              │
  │                           │                              │
  │                     send_binary_initial()                │
  │                     for_each_table(dbmon):               │
  │                       only MONITORED tables              │
  │                       only MONITORED columns             │
  │                       submit worker job ──────────────→  │
  │                                                          │
  │                     return to event loop                 │
  │                     (main thread FREE)                   │
  │                           │                              │
  │                           │     disk_cursor_stream_worker:
  │                           │       cursor_open(table)     │
  │                           │       while cursor_next:     │
  │                           │         pread → row          │
  │                           │         serialize(binary)    │
  │                           │         destroy row          │
  │                           │         batch → mutex → seq  │
  │                           │                              │
  │                     session_run():                       │
  │                     drain_batches():                     │
  │◄── ROW_BATCH ────────────│                              │
  │                           │       (more rows...)         │
  │◄── ROW_BATCH ────────────│                              │
  │                           │       done=true, seq_change  │
  │                           │                              │
  │                     all_done → INITIAL_END               │
  │◄── INITIAL_END ──────────│                              │
  │                           │                              │
  │                     init change_set_head                 │
  │                     (for future incremental updates)     │
```

### Key Design Decisions

1. **Workers bypass cache** — `disk_cursor_stream_worker_fn` does
   `pread → serialize → destroy`.  Zero cache interaction, zero lock
   contention with the main thread.

2. **Single shared seq** — all jobs signal the same `struct seq` on
   the monitor.  Avoids the per-job seqno mismatch that caused
   infinite poll spin.

3. **Skip `get_initial`** — the expensive synchronous row scan that
   populated the change set is bypassed entirely in the binary path.
   Workers read directly from disk.

4. **Table and column filtering** — `ovsdb_monitor_for_each_table()`
   iterates only monitored tables with only monitored columns.

### Job Lifecycle

```
                    ┌─── submit ───┐
                    │              │
                    ▼              │
              [RUNNING]           │
                    │              │
           ┌───────┴───────┐     │
           │               │     │
     normal completion  disconnect
           │               │
           ▼               ▼
     session_run:     cleanup_jobs:
     job_destroy()    cancelled=true
                      monitor=NULL
                           │
                           ▼
                      done_fn:
                      job_destroy()
```

On disconnect during streaming, `cleanup_jobs` sets the atomic
`cancelled` flag and detaches the job.  Workers check `cancelled`
each iteration and abort early.  `done_fn` frees the job after the
worker returns.

### Incremental Updates

After INITIAL_END, `ovsdb_monitor_get_change_set_head()` initializes
the change set tracking pointer.  Subsequent transaction commits
create new change sets; `monitor_flush_all()` detects the change and
sends binary UPDATE_BATCH frames (transitional: JSON-in-binary).

---

## 13. Client-Side Binary Transport

**Files:** `lib/ovsdb-cs.c`, `lib/ovsdb-cs.h`, `lib/ovsdb-idl.h`

### Opt-In

Binary transport is enabled by default in `ovsdb_cs_create()`:
```c
cs->binary_transport = true;
```

Override: `ovsdb_idl_set_binary_transport(idl, false)`.

### Client State Machine

```
CS_S_DATA_MONITOR_COND_SINCE_REQUESTED
    │
    ├── reply has 4 elements + {"format":"binary"} in [3]
    │   → set binary_initial_pending = true
    │   → parse found + last_id from [0] and [1]
    │   → skip empty table_updates [2]
    │   → transition to CS_S_MONITORING
    │
    └── reply has 3 elements (no binary ack)
        → parse normally (JSON path)
        → transition to CS_S_MONITORING

While binary_initial_pending:
    │
    ├── ROW_BATCH frames arrive
    │   → ovsdb_cs_process_binary_row_batch()
    │   → converts to JSON table-updates2 format
    │   → creates synthetic update notification
    │   → events ACCUMULATE in cs->data.events
    │   → NOT flushed to IDL (held back)
    │
    ├── ovsdb_cs_run():
    │   → events NOT flushed while pending
    │   → has_ever_connected stays false
    │   → ovn-nbctl keeps polling
    │
    └── INITIAL_END arrives
        → binary_initial_pending = false
        → next ovsdb_cs_run() flushes ALL events
        → IDL processes complete snapshot
        → has_ever_connected = true
        → ovn-nbctl reads data
```

### Reconnect Safety

`ovsdb_cs_restart_fsm()` clears `binary_initial_pending`.  Without
this, events would be suppressed forever after reconnect.

### Transaction Guard

`ovsdb_cs_may_send_transaction()` returns false while
`binary_initial_pending` is true, preventing writes against
incomplete client state.

### Backward Compatibility

| Client | Server | Behavior |
|--------|--------|----------|
| New | New | Binary negotiated, streaming works |
| New | Old | Server ignores 5th param, returns 3-element JSON reply, client handles normally |
| Old | New | No 5th param sent, server uses JSON path |
| Old | Old | Unchanged |

---

## 14. Thread Safety Model

### What Runs Where

| Thread | Operations |
|--------|-----------|
| Main | Event loop, trigger processing, session_run, drain batches, send frames |
| Worker (N) | disk pread, row deserialization, binary serialization, batch enqueue |
| Sweeper | Cache usage decay (rdlock), eviction (wrlock) |

### Shared State and Protection

| Data | Accessed By | Protection |
|------|------------|------------|
| `table->schema` | Main + Workers | Immutable after startup |
| `table->disk_store` | Main + Workers | pread is thread-safe (no shared offset) |
| `table->cache` | Main + Sweeper | rwlock (rdlock for lookup, wrlock for insert/evict) |
| `job->batches` | Worker + Main | `ovs_mutex` per job |
| `job->done` | Worker + Main | Under `job->mutex` |
| `job->cancelled` | Main + Worker | `ATOMIC(bool)` relaxed |
| `m->stream_seq` | Worker + Main | `struct seq` (internally synchronized) |
| `table->bloom` | Main + Workers | Atomic bit operations |
| `table->rows` | Main only | Not accessed by workers (disk-cursor path) |

### Lock Ordering

1. `cache->rwlock` — never held while acquiring other locks
2. `job->mutex` — held briefly for batch enqueue/dequeue
3. `store->index_rwlock` — held by cursor_open for snapshot

---

## 15. Data Flow Diagrams

### Startup (Disk-Store Mode)

```
disk file
    │
    ├── scan headers ──→ UUID→offset index (hmap)
    │                  → bloom filter (atomic bits)
    │                  → name index (reads name column)
    │
    └── cache starts EMPTY (no warm-up)
        sweeper thread started
```

### On-Demand Row Read

```
bloom filter ── may exist? ──→ cache lookup ── hit? ──→ return row
     │ no                            │ miss
     └→ return NULL                  └→ disk_store index
                                          │
                                     offset → pread
                                          │
                                     deserialize
                                          │
                                     cache insert
                                          │
                                     return row
```

### Binary Initial Snapshot

```
Client                    Server                     Worker
  │                         │                          │
  │ monitor_cond_since      │                          │
  │ + {"format":"binary"}   │                          │
  │────────────────────────→│                          │
  │                         │ skip get_initial         │
  │                         │ send ack reply           │
  │◄────────────────────────│                          │
  │                         │ submit jobs ────────────→│
  │                         │ (main thread free)       │
  │                         │                          │ cursor_open
  │                         │                     ┌──→ │ pread → serialize
  │                    drain│◄── batch (mutex) ───┘    │ → destroy row
  │◄── ROW_BATCH ──────────│                          │
  │                         │                          │ (repeat)
  │◄── ROW_BATCH ──────────│◄── batch ────────────────│
  │                         │                          │ done=true
  │◄── INITIAL_END ────────│                          │
  │                         │ init change_set_head     │
  │                         │ (incremental updates     │
  │                         │  work from here)         │
```

---

## 16. Configuration

### CLI Parameters

| Parameter | Range | Default | Description |
|-----------|-------|---------|-------------|
| `--num-workers=N` | 1-64 | `OVSDB_IO_WORKER_THREADS` | Worker thread count |

### Runtime Inspection

```bash
ovs-appctl ovsdb-server/get-num-workers
```

### Binary Streaming Batch Limits

| Constant | Value | Description |
|----------|-------|-------------|
| `BINARY_BATCH_MAX_BYTES` | 64 KB | Max payload per ROW_BATCH frame |
| `BINARY_BATCH_MAX_ROWS` | 256 | Max rows per ROW_BATCH frame |

---

## 17. Testing

### Unit Tests (`tests/test-binary-codec.c`)

| Test | Description |
|------|-------------|
| `atoms` | Round-trip every atom type x {native, nbo} |
| `datums` | Scalar int, string set, string→int map, empty |
| `rows` | 3-column schema from JSON, serialize + deserialize |
| `rows_stress` | 10,000 row round-trip |
| `large_datums` | 100-element set, 50-pair map |
| `frames` | Frame encode/decode for all message types |
| `invalid` | Wrong magic, version, oversized payload |
| `magic` | Magic byte detection (0xDB vs 0x7B) |
| `edges` | INT64_MIN/MAX, empty string, zero UUID |

### Autotest (`tests/ovsdb-binary.at`)

10 test cases run via `make check TESTSUITEFLAGS='-k "binary codec" -k "binary protocol"'`.

### Running Tests

```bash
# Unit tests directly
tests/test-binary-codec

# Via autotest
make check TESTSUITEFLAGS='-k "binary codec" -k "binary protocol"'

# Full OVSDB test suite
make check TESTSUITEFLAGS="-j4 -k ovsdb"
```

---

## Appendix: Commit History

| Commit | Description |
|--------|-------------|
| `34361e684` | Multi-threaded serialization infrastructure (Phases 0-2) |
| `ed243e408` | Clock-sweep cache with scan-aware ring buffer |
| `373896afd` | Background sweeper thread |
| `71e14d5c7` | Secondary name index |
| `2c2df17de` | Bloom filter with atomic operations |
| `3f1f9a75e` | Binary streaming protocol |
| `b055d8b52` | Remove startup warm-up (cold cache) |
| `257e9c2db` | Incremental batch delivery (mutex + seq) |
| `aba85d791` | Worker table filtering + binary default + tests |
| `410e77cae` | Use-after-free fix (cancelled flag lifecycle) |
| `4b3f52782` | Binary ack reply format + message ordering |
| `dae89ea2f` | Client binary_initial_pending state machine |
| `88986a01a` | Remove cache-through worker (all bypass cache) |
| `37092b3b4` | Skip synchronous initial scan + shared seq |
| `0815b5880` | Change set head for incremental updates |
