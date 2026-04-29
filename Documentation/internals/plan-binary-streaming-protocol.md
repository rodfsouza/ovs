# Plan B: Binary Streaming Protocol — Detailed Implementation

## Context

The current monitor pipeline wastes CPU on both sides:
```
SERVER: ovsdb_datum → datum_to_json() → json_to_string() → TCP    [expensive]
CLIENT: TCP → json_parser_feed() → datum_from_json() → ovsdb_datum [expensive]
```

The disk-store already has a compact binary codec (`disk-store.c:295-473`) for atom/datum serialization. We extract it, add network byte order, and create a binary transport mode where server sends binary row data in small batches and the client IDL deserializes directly to `ovsdb_datum`.

**Key wire-level insight:** JSON-RPC has no framing — messages are self-delimiting via JSON syntax. Between messages the parser is `NULL` (`jsonrpc.c:362`). We detect binary frames by peeking the first byte: `{` → JSON, `0xDB` → binary frame. This allows both protocols on the same TCP connection.

**Cache philosophy:** Data reads go through the row cache (like PostgreSQL's buffer cache for sequential scans). The cache fills on-demand and pollution is limited by the clock-sweep eviction with query-burst caps. No startup warm-up — the cache starts cold and warms naturally.

---

## Phase 1: `--num-workers` CLI Parameter

**Files:** `ovsdb/ovsdb-server.c`

### Changes

1. Add to enum (~line 2678):
   ```c
   OPT_NUM_WORKERS,
   ```

2. Add to `long_options[]` (~line 2708):
   ```c
   {"num-workers", required_argument, NULL, OPT_NUM_WORKERS},
   ```

3. Add file-static variable (~line 74):
   ```c
   static size_t n_io_workers = OVSDB_IO_WORKER_THREADS;
   ```

4. Add case in `parse_options()` switch (~line 2826):
   ```c
   case OPT_NUM_WORKERS: {
       unsigned long long int value;
       if (!str_to_ullong(optarg, 10, &value)
           || value == 0 || value > 64) {
           ovs_fatal(0, "--num-workers: must be 1..64, got \"%s\"", optarg);
       }
       n_io_workers = (size_t) value;
       break;
   }
   ```

5. Replace at line 835:
   ```c
   io_worker_pool = ovsdb_worker_pool_create(n_io_workers, "io-worker");
   ```

6. Add unixctl command (~line 929):
   ```c
   unixctl_command_register("ovsdb-server/get-num-workers", "", 0, 0,
                            ovsdb_server_get_num_workers, NULL);
   ```

7. Add handler:
   ```c
   static void
   ovsdb_server_get_num_workers(struct unixctl_conn *conn,
                                int argc OVS_UNUSED,
                                const char *argv[] OVS_UNUSED,
                                void *aux OVS_UNUSED)
   {
       char *reply = xasprintf("%"PRIuSIZE, n_io_workers);
       unixctl_command_reply(conn, reply);
       free(reply);
   }
   ```

8. Update `usage()` to include `--num-workers`.

**Verification:** `--num-workers=2` and `--num-workers=8` startup, `ovs-appctl ovsdb-server/get-num-workers` returns correct value.

---

## Phase 2: Simplify Startup + Fix Cache Shrinking

**Files:** `ovsdb/ovsdb.c`, `ovsdb/row-cache.c`, `ovsdb/row-cache.h`, `ovsdb/table.c`

### 2a. Remove startup warm-up from `ovsdb_attach_disk_store()`

The current startup in `ovsdb_attach_disk_store()` (`ovsdb.c:768-848`) does 4 passes. We remove Pass 1 (UNLOADED population) and Pass 4 (proactive warm-up), keeping only the index builds:

**Before (`ovsdb.c:782-848`):**
```
Pass 1: ovsdb_disk_store_for_each_uuid → add_unloaded_cb   [REMOVE]
Pass 2: ovsdb_disk_store_for_each_uuid → add_bloom_cb       [KEEP]
Pass 3: ovsdb_disk_store_build_name_index                    [KEEP]
Pass 4: ovsdb_row_cache_enter_burst + lazy_load_bulk_request [REMOVE]
```

**After:**
```c
void
ovsdb_attach_disk_store(struct ovsdb *db, size_t cache_max_atoms)
{
    struct ovsdb_disk_store *ds = ovsdb_storage_get_disk_store(db->storage);
    if (!ds) return;

    struct shash_node *node;
    SHASH_FOR_EACH (node, &db->tables) {
        struct ovsdb_table *table = node->data;
        table->disk_store = ds;
        table->cache = ovsdb_row_cache_create(cache_max_atoms);

        /* Pass 1: Build bloom filter from all indexed UUIDs.
         * Enables fast negative lookups without touching cache or disk. */
        size_t n_rows = ovsdb_disk_store_count(ds, node->name);
        table->bloom = ovsdb_bloom_filter_create(n_rows);
        ovsdb_disk_store_for_each_uuid(
            ds, node->name, add_bloom_cb, table->bloom);

        /* Pass 2: Build secondary name index (if applicable).
         * Reads rows from disk to extract indexed column value. */
        for (size_t i = 0; i < table->schema->n_indexes; i++) {
            const struct ovsdb_column_set *idx = &table->schema->indexes[i];
            if (idx->n_columns == 1
                && idx->columns[0]->type.key.type == OVSDB_TYPE_STRING
                && idx->columns[0]->type.n_max == 1) {
                table->name_index = ovsdb_name_index_create(
                    idx->columns[0]->name, idx->columns[0]->index);
                ovsdb_disk_store_build_name_index(ds, table->name_index);
                break;
            }
        }

        /* No warm-up. Cache starts cold and fills on demand.
         * Start sweeper for ongoing eviction management. */
        ovsdb_row_cache_start_sweeper(table->cache);

        VLOG_DBG("%s: table %s: %"PRIuSIZE" rows indexed from disk store",
                 db->name, node->name, n_rows);
    }
    db->disk_store_mode = true;
}
```

**What's removed:**
- `add_unloaded_cb` / UNLOADED cache population (Pass 1) — the bloom filter + disk-store index handle existence checks
- `ovsdb_row_cache_enter_burst()` / `exit_burst()` at startup
- `ovsdb_lazy_load_bulk_request_until_full()` (Pass 4)
- Startup dependency on the worker pool being created before `open_db()`

### 2b. Simplify row cache — remove UNLOADED state from hot path

The UNLOADED state was needed because the cache was the primary existence check. Now the bloom filter + disk-store index serve that role. The cache is purely a **performance cache** — miss = go to disk.

In `ovsdb_table_get_row()` (or equivalent lookup path):
```c
const struct ovsdb_row *
ovsdb_table_get_row(const struct ovsdb_table *table, const struct uuid *uuid)
{
    /* Fast path: check in-memory rows hmap (for in-memory tables). */
    if (!table->disk_store) {
        return ovsdb_table_get_row__(table, uuid);
    }

    /* Disk-store path: cache check → bloom → index → pread. */

    /* 1. Cache hit? */
    const struct ovsdb_row *row = ovsdb_row_cache_lookup(table->cache, uuid);
    if (row) {
        return row;  /* Hot path. */
    }

    /* 2. Bloom filter: does this UUID exist at all? */
    if (!ovsdb_bloom_filter_may_contain(table->bloom, uuid)) {
        return NULL;  /* Definite miss — no disk I/O. */
    }

    /* 3. Disk-store index lookup + pread. */
    row = ovsdb_disk_store_read_row(table->disk_store, table, uuid);
    if (row) {
        /* Insert into cache (evicts cold entries via clock-sweep). */
        size_t n_atoms = ovsdb_row_count_atoms(row);
        ovsdb_row_cache_insert(table->cache, row, n_atoms);
    }
    return row;
}
```

### 2c. Add hmap shrink after eviction

In `ovsdb_row_cache_evict__()` after line 432 (`ovsdb_row_cache_compact__`):
```c
if (hmap_count(&cache->entries) > 0
    && cache->n_entries < hmap_count(&cache->entries) / 4) {
    hmap_shrink(&cache->entries);
}
```

In `ovsdb_row_cache_sweep_deferred__()` after line 233:
```c
if (hmap_count(&cache->entries) > 0
    && cache->n_entries < hmap_count(&cache->entries) / 4) {
    hmap_shrink(&cache->entries);
}
```

### 2d. Cap query-driven burst mode

Add `ovsdb_row_cache_enter_query_burst()` in `row-cache.c`:
```c
void
ovsdb_row_cache_enter_query_burst(struct ovsdb_row_cache *cache)
{
    ovs_rwlock_wrlock(&cache->rwlock);
    cache->burst_refcount++;
    size_t query_cap = cache->base_max_atoms * 2;
    if (cache->max_atoms < query_cap) {
        cache->max_atoms = query_cap;
    }
    ovs_rwlock_unlock(&cache->rwlock);
}
```

Update `ovsdb_table_query()` in `table.c` line 702:
```c
ovsdb_row_cache_enter_query_burst(table->cache);
```

### 2e. Remove or deprecate lazy-load startup path

Files that can be simplified:
- `ovsdb/lazy-load.c` — remove `ovsdb_lazy_load_bulk_request_until_full()` and startup-specific code. Keep `ovsdb_lazy_load_request()` if individual on-demand loads still use it, or replace with direct `ovsdb_disk_store_read_row()` + cache insert.
- `ovsdb/row-cache.c` — the `OVSDB_ROW_UNLOADED` and `OVSDB_ROW_LOADING` states can be removed since the cache no longer tracks existence. Simplify `ovsdb_row_cache_entry` to only have `OVSDB_ROW_CACHED` state.
- `ovsdb/jsonrpc-server.c` — remove the deferred monitor path for bulk loading (`lines 1620-1644`). The binary streaming path replaces it.

**Verification:**
- Start ovsdb-server with disk-store, verify no warm-up log messages
- First query hits disk, subsequent queries hit cache
- 50K rows → trigger query scan → verify cache fills and shrinks via sweeper
- Bloom filter correctly rejects non-existent UUIDs

---

## Phase 3: Extract Binary Codec Library

**New files:** `ovsdb/binary-codec.h`, `ovsdb/binary-codec.c`
**Modified:** `ovsdb/disk-store.c`, `ovsdb/automake.mk`

### 3a. `binary-codec.h` — Public API

```c
#ifndef OVSDB_BINARY_CODEC_H
#define OVSDB_BINARY_CODEC_H 1

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include "ovsdb-data.h"

/* Serialization buffer. */
struct ovsdb_binary_buf {
    uint8_t *data;
    size_t size;
    size_t allocated;
};

void ovsdb_binary_buf_init(struct ovsdb_binary_buf *);
void ovsdb_binary_buf_destroy(struct ovsdb_binary_buf *);
void ovsdb_binary_buf_put(struct ovsdb_binary_buf *, const void *, size_t);
void ovsdb_binary_buf_put_uint8(struct ovsdb_binary_buf *, uint8_t);
void ovsdb_binary_buf_put_uint16(struct ovsdb_binary_buf *, uint16_t, bool nbo);
void ovsdb_binary_buf_put_uint32(struct ovsdb_binary_buf *, uint32_t, bool nbo);
void ovsdb_binary_buf_put_int64(struct ovsdb_binary_buf *, int64_t, bool nbo);
void ovsdb_binary_buf_put_double(struct ovsdb_binary_buf *, double, bool nbo);
void ovsdb_binary_buf_put_uuid(struct ovsdb_binary_buf *, const struct uuid *);
void ovsdb_binary_buf_put_string(struct ovsdb_binary_buf *, const char *, bool nbo);

/* Deserialization reader. */
struct ovsdb_binary_reader {
    const uint8_t *data;
    size_t size;
    size_t pos;
};

void ovsdb_binary_reader_init(struct ovsdb_binary_reader *, const uint8_t *, size_t);
bool ovsdb_binary_reader_get(struct ovsdb_binary_reader *, void *, size_t);
bool ovsdb_binary_reader_get_uint8(struct ovsdb_binary_reader *, uint8_t *);
bool ovsdb_binary_reader_get_uint16(struct ovsdb_binary_reader *, uint16_t *, bool nbo);
bool ovsdb_binary_reader_get_uint32(struct ovsdb_binary_reader *, uint32_t *, bool nbo);
bool ovsdb_binary_reader_get_int64(struct ovsdb_binary_reader *, int64_t *, bool nbo);
bool ovsdb_binary_reader_get_double(struct ovsdb_binary_reader *, double *, bool nbo);
bool ovsdb_binary_reader_get_uuid(struct ovsdb_binary_reader *, struct uuid *);
bool ovsdb_binary_reader_get_string(struct ovsdb_binary_reader *, char **, bool nbo);

/* Atom/Datum codec. */
void ovsdb_binary_serialize_atom(struct ovsdb_binary_buf *, const union ovsdb_atom *,
                                  enum ovsdb_atomic_type, bool nbo);
struct ovsdb_error *ovsdb_binary_deserialize_atom(struct ovsdb_binary_reader *,
    union ovsdb_atom *, enum ovsdb_atomic_type, bool nbo);

void ovsdb_binary_serialize_datum(struct ovsdb_binary_buf *, const struct ovsdb_datum *,
                                   const struct ovsdb_type *, bool nbo);
struct ovsdb_error *ovsdb_binary_deserialize_datum(struct ovsdb_binary_reader *,
    struct ovsdb_datum *, const struct ovsdb_type *, bool nbo);

/* Row-level codec for network transport.
 *
 * Wire format per row:
 *   uuid:        16 bytes
 *   n_columns:   uint16
 *   Per column:  uint16 name_len + name + uint8 key_type + uint8 val_type + datum
 */
void ovsdb_binary_serialize_row(struct ovsdb_binary_buf *, const struct uuid *,
                                 const struct ovsdb_datum *datums,
                                 const struct ovsdb_column_set *columns, bool nbo);
struct ovsdb_error *ovsdb_binary_deserialize_row(struct ovsdb_binary_reader *,
    struct uuid *, struct ovsdb_datum *datums, size_t n_datums,
    const struct ovsdb_table_schema *, bool nbo);

#endif /* ovsdb/binary-codec.h */
```

### 3b. Implementation

Extract from `disk-store.c` (lines 139-473) into `binary-codec.c`. Add `bool nbo` (network byte order) parameter using `htons/htonl/htobe64` when true. Refactor `disk-store.c` to call `ovsdb_binary_*(..., false)`.

**Verification:** All disk-store tests pass. New round-trip tests for each type with network byte order.

---

## Phase 4: Binary Wire Protocol

**New files:** `lib/binary-protocol.h`, `lib/binary-protocol.c`
**Modified:** `lib/jsonrpc.c`, `lib/automake.mk`

### 4a. Frame format

```
[Binary Frame — starts with 0xDB, never valid JSON first byte]
Offset  Size  Field
------  ----  ----------------------------------
0       1     magic: 0xDB
1       1     version: 0x01
2       1     msg_type (OVSDB_BIN_* enum)
3       1     flags (reserved, 0x00)
4       4     payload_len (uint32_t, network byte order)
8       N     payload (N = payload_len)
```

### 4b. Message types

```c
enum ovsdb_binary_msg_type {
    OVSDB_BIN_INITIAL_BEGIN  = 0x01,
    OVSDB_BIN_ROW_BATCH      = 0x02,
    OVSDB_BIN_INITIAL_END    = 0x03,
    OVSDB_BIN_UPDATE         = 0x04,
    OVSDB_BIN_UPDATE_BATCH   = 0x05,
};
```

### 4c. Payload formats

**INITIAL_BEGIN:** `monitor_id(16) + table_name(uint16+str) + n_rows(uint32)`
**ROW_BATCH:** `monitor_id(16) + table_name(uint16+str) + n_rows(uint16) + rows[]`
**INITIAL_END:** `monitor_id(16) + txn_id(16)`
**UPDATE:** `monitor_id(16) + txn_id(16) + table_name(uint16+str) + type(uint8) + row`

Target batch: ~64KB (~64 rows with 20 columns).

### 4d. Receive path — `lib/jsonrpc.c`

Add to `struct jsonrpc`:
```c
bool binary_frame_active;
uint8_t binary_frame_hdr[8];
size_t binary_frame_hdr_read;
uint8_t *binary_frame_payload;
size_t binary_frame_payload_read;
size_t binary_frame_payload_len;
```

In `jsonrpc_recv()` (~line 362), between messages peek first byte:
- `{` or `[` → JSON parser (existing)
- `0xDB` → binary frame parser (new `jsonrpc_recv_binary_frame()`)

### 4e. Send path

```c
int jsonrpc_send_binary(struct jsonrpc *rpc, uint8_t msg_type,
                        const void *payload, size_t payload_len);
```

Builds 8-byte header + payload into one `ofpbuf`, queues on `rpc->output`.

### 4f. Extend `struct jsonrpc_msg`

```c
bool is_binary;
uint8_t binary_msg_type;
uint8_t *binary_payload;       /* Owned. */
size_t binary_payload_len;
```

**Verification:** Frame round-trip, interleaved JSON+binary, partial reads.

---

## Phase 5: Server-Side Binary Monitor

**Files:** `ovsdb/jsonrpc-server.c`, `ovsdb/monitor.c`

### 5a. Negotiation

In `ovsdb_jsonrpc_monitor_create()`, detect `"format": "binary"` in params.

### 5b. Extend `struct ovsdb_jsonrpc_monitor`

```c
bool binary_transport;
bool binary_initial_streaming;
struct ovsdb_binary_stream_job *stream_job;
```

### 5c. Binary initial snapshot — worker reads through cache

When `binary_requested && !conditional`:

1. **Main thread:** Send JSON-RPC ack: `{"result": {"format": "binary"}}`
2. **Main thread:** Collect list of row UUIDs from disk-store index (in-memory, no I/O)
3. **Submit `binary_stream_job` to worker pool:**

```c
struct ovsdb_binary_stream_job {
    /* Inputs. */
    struct ovsdb_table *table;            /* For cache + disk-store access. */
    struct uuid *uuids;                   /* Array of row UUIDs to stream. */
    size_t n_uuids;
    const struct ovsdb_column_set *columns;
    const char *table_name;
    struct uuid monitor_id;

    /* Output: completed batch queue. */
    struct ovs_list completed_batches;    /* List of ofpbuf. */
    struct ovs_mutex batch_mutex;
    struct seq *batch_seq;                /* Signals main thread. */
    bool done;
    struct uuid txn_id;
};
```

4. **Worker thread:**
```c
void *
ovsdb_binary_stream_worker(void *arg)
{
    struct ovsdb_binary_stream_job *job = arg;
    struct ovsdb_binary_buf batch;
    ovsdb_binary_buf_init(&batch);
    size_t rows_in_batch = 0;

    for (size_t i = 0; i < job->n_uuids; i++) {
        /* Read through cache: hit = fast, miss = pread + cache insert.
         * Like PostgreSQL buffer cache for sequential scans —
         * cache fills naturally, clock-sweep limits pollution. */
        const struct ovsdb_row *row =
            ovsdb_table_get_row(job->table, &job->uuids[i]);
        if (!row) {
            continue;  /* Deleted between UUID list and read. */
        }

        ovsdb_binary_serialize_row(&batch, &job->uuids[i],
                                    row->fields, job->columns, true);
        rows_in_batch++;

        if (batch.size >= 64 * 1024 || rows_in_batch >= 256) {
            enqueue_batch(job, &batch, rows_in_batch);
            ovsdb_binary_buf_init(&batch);
            rows_in_batch = 0;
        }
    }
    if (rows_in_batch > 0) {
        enqueue_batch(job, &batch, rows_in_batch);
    }

    job->done = true;
    seq_change(job->batch_seq);
    return NULL;
}
```

**Cache interaction:** The worker calls `ovsdb_table_get_row()` which goes through the cache (cache hit = memory read, cache miss = `pread()` + insert). The clock-sweep evicts cold entries naturally. The query-burst cap from Phase 2d prevents budget explosion.

5. **Main thread (session run loop):** Dequeue completed batches, send via `jsonrpc_send_binary()`, send `INITIAL_END` when done.

### 5d. Binary incremental updates

When `m->binary_transport`, replace JSON compose with binary:
```c
static void
ovsdb_jsonrpc_monitor_send_binary_update(struct ovsdb_jsonrpc_monitor *m,
                                          struct ovsdb_monitor_change_set *mcs)
{
    struct ovsdb_binary_buf buf;
    ovsdb_binary_buf_init(&buf);

    struct ovsdb_monitor_change_set_for_table *mcst;
    LIST_FOR_EACH (mcst, list_in_change_set, &mcs->change_set_for_tables) {
        struct ovsdb_monitor_row *row;
        HMAP_FOR_EACH_SAFE (row, hmap_node, &mcst->rows) {
            uint8_t type = row->old ? (row->new ? 1 : 2) : 0;
            /* Serialize: monitor_id + txn_id + table + type + row data */
            jsonrpc_send_binary(s->js->rpc, OVSDB_BIN_UPDATE,
                                buf.data, buf.size);
            ovsdb_binary_buf_clear(&buf);
        }
    }
    ovsdb_binary_buf_destroy(&buf);
}
```

### 5e. Fallback

Clients without `"format": "binary"` get the existing JSON path unchanged.

---

## Phase 6: Client-Side Binary Support

**Files:** `lib/ovsdb-idl.c`, `lib/ovsdb-idl.h`, `lib/ovsdb-cs.c`

### 6a. Public API

```c
void ovsdb_idl_set_binary_transport(struct ovsdb_idl *, bool enable);
```

### 6b. Binary monitor request

In `ovsdb_cs_send_monitor_request()`, add `"format": "binary"` to params when enabled.

### 6c. Binary message routing in ovsdb-cs

In `ovsdb_cs_process_msg()`, route `msg->is_binary` to `ovsdb_cs_process_binary_msg()` which creates `OVSDB_CS_EVENT_TYPE_BINARY_UPDATE` events.

### 6d. IDL binary row processing

```c
static void
ovsdb_idl_process_binary_row(struct ovsdb_idl_table *table,
                              const struct uuid *uuid,
                              struct ovsdb_datum *datums)
{
    struct ovsdb_idl_row *row = ovsdb_idl_get_row(table, uuid);
    if (!row) {
        row = ovsdb_idl_row_create(table, uuid);
        hmap_insert(&table->rows, &row->hmap_node, uuid_hash(uuid));
    }

    for (size_t i = 0; i < table->class_->n_columns; i++) {
        if (datums[i].n > 0) {
            ovsdb_datum_swap(&row->old_datum[i], &datums[i]);
            if (table->modes[i] & OVSDB_IDL_ALERT) {
                row->change_seqno[OVSDB_IDL_CHANGE_INSERT] =
                    ++table->idl->change_seqno;
            }
        }
    }
    ovsdb_idl_row_parse(row);
    ovsdb_idl_add_to_indexes(table, row);
}
```

**Performance win:** `ovsdb_binary_deserialize_datum()` → `ovsdb_datum` directly. No JSON parser, no `ovsdb_datum_from_json()`. Integers = memcpy(8 bytes), UUIDs = memcpy(16 bytes), strings = length-prefixed read.

### 6e. OVN opt-in

One-liner in each OVN daemon's `main()`:
```c
ovsdb_idl_set_binary_transport(idl, true);
```

---

## Phase 7: Testing

### Unit Tests
1. Binary codec round-trip: each atom type × {native, network} byte order
2. Datum round-trip: scalar, set, map × various types
3. Row round-trip: multi-column, verify all columns
4. Edge cases: empty string, max/min int64, NaN, empty set/map

### Wire Protocol Tests
5. Frame round-trip: build → parse → verify
6. Interleaved: JSON → binary → JSON on same stream
7. Partial reads: byte-at-a-time feed
8. Invalid frames: truncated header, wrong magic, oversized payload

### Integration Tests
9. `--num-workers` — various values, verify via unixctl
10. Startup without warm-up — verify no lazy-load, cache starts empty
11. On-demand cache fill — first access hits disk, second hits cache
12. Bloom filter negative — non-existent UUID returns NULL without disk I/O
13. Cache shrink — large scan, verify hmap shrinks after eviction
14. Binary negotiation — client requests binary, server accepts
15. Binary initial snapshot — 10K rows, compare with JSON baseline
16. Binary incremental — INSERT/MODIFY/DELETE, verify IDL state
17. JSON fallback — old client gets standard JSON
18. Mixed clients — binary + JSON monitoring same DB
19. Worker streaming batches — arrive incrementally
20. Session disconnect during streaming — no crash/leak
21. Concurrent transactions during streaming

### Performance Benchmarks
22. Time to first IDL row: binary vs. JSON for 10K/50K/100K rows
23. Startup time: cold (no warm-up) vs. old warm-up
24. Peak server RSS during initial snapshot
25. Network bytes: binary vs. JSON for same dataset

```bash
make -j4 && make check TESTSUITEFLAGS="-j4 -k ovsdb"
```

---

## Critical Files

| File | Changes | Risk |
|------|---------|------|
| `ovsdb/ovsdb-server.c` | `--num-workers` CLI | Low |
| `ovsdb/ovsdb.c` | Simplified `attach_disk_store` — remove warm-up | Medium |
| `ovsdb/row-cache.c` | hmap_shrink, query-burst cap, remove UNLOADED state | Medium |
| `ovsdb/row-cache.h` | Remove UNLOADED/LOADING states, add query_burst API | Medium |
| `ovsdb/lazy-load.c` | Remove startup bulk load, possibly deprecate | Medium |
| `ovsdb/table.c` | query-burst cap, simplified get_row path | Medium |
| `ovsdb/binary-codec.h` | **New** — serialize/deserialize API | Low |
| `ovsdb/binary-codec.c` | **New** — extracted from disk-store + nbo | Low |
| `ovsdb/disk-store.c` | Refactor to use binary-codec | Medium |
| `lib/binary-protocol.h` | **New** — frame format, msg types | Low |
| `lib/binary-protocol.c` | **New** — frame encode/decode | Low |
| `lib/jsonrpc.c` | Binary frame detect/recv/send | **High** |
| `ovsdb/jsonrpc-server.c` | Binary monitor, worker streaming, remove deferred bulk-load | **High** |
| `ovsdb/monitor.c` | Binary compose variant for updates | Medium |
| `lib/ovsdb-cs.c` | Binary msg routing, event creation | **High** |
| `lib/ovsdb-idl.c` | Binary row processing, transport API | **High** |
| `lib/ovsdb-idl.h` | Public API: `set_binary_transport()` | Low |
| `ovsdb/automake.mk` | New source files | Low |
| `lib/automake.mk` | New source files | Low |

## Implementation Order

```
Phase 1: --num-workers CLI                    [standalone]
Phase 2: Simplify startup + cache fix         [standalone]
Phase 3: Binary codec extraction              [refactor only]
   ↓
Phase 4: Wire protocol (lib/)                 [new code]
   ↓
Phase 5: Server binary monitor (ovsdb/)       [integration]
Phase 6: Client binary support (lib/)         [integration]
   ↓
Phase 7: Tests                                [throughout]
```

Phases 1-3 are independent. Phase 4 gates 5 and 6. Phases 5 and 6 can develop in parallel.

## Data Flow Diagrams

```
STARTUP (simplified):
  disk file ──read headers──→ UUID→offset index (in memory)
                            → bloom filter (in memory)
                            → name index (reads name column from disk)
  Cache starts EMPTY. No warm-up.

ON-DEMAND READ:
  bloom filter ──may exist?──→ cache lookup ──hit?──→ return row
       │ no                          │ miss
       └→ return NULL                └→ disk_store index ──offset──→ pread()
                                                                      ↓
                                                            deserialize → cache insert → return

BINARY INITIAL SNAPSHOT (server → client):
  disk_store index ──UUIDs──→ [worker thread] ──get_row()──→ cache hit/miss
                                    ↓                            ↓
                           binary_serialize_row()         pread → cache insert
                                    ↓
                           BINARY_ROW_BATCH (64KB)
                                    ↓
                               TCP socket
                                    ↓
  client IDL ←── binary_deserialize_datum() ←── ROW_BATCH
                  (no JSON parsing)

JSON FALLBACK (old clients):
  unchanged — existing JSON-RPC monitor path
```

## Thread Safety

| Data | Worker access | Safe? | Why |
|------|--------------|-------|-----|
| Row cache | Yes (read + insert) | Yes | Protected by rwlock |
| Disk-store file | Yes (pread) | Yes | pread is thread-safe |
| Disk-store index | Yes (read) | Yes | Immutable after startup |
| Bloom filter | Yes (read) | Yes | Immutable after startup |
| Batch queue | Worker writes, main reads | Yes | Mutex + seq signal |
| TCP output | Main thread only | Yes | Worker produces batches, main sends |
