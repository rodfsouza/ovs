# OVSDB Binary Transport Architecture

This document describes the binary transport protocol for OVSDB, which
replaces JSON serialization with a compact binary wire format for monitor
traffic. This optimization is implemented in the OVS IDL and CS layers.

## Motivation

Large OVSDB databases (1 GB+) suffer from significant serialization overhead
with the traditional JSON-based monitor protocol:

- JSON parsing and generation is CPU-intensive for large table snapshots
- JSON encoding inflates data size by 2-3x compared to binary
- Initial snapshot delivery is single-threaded and blocks the server main loop

Binary transport addresses all three problems:

- Eliminates JSON serialization overhead on the critical monitor path
- Reduces wire bandwidth by ~10-40%
- Uses worker threads to parallelize initial snapshot encoding
- Is fully backward compatible (old servers ignore the binary format request)

## IDL API

```c
void ovsdb_idl_set_binary_transport(struct ovsdb_idl *idl, bool enable);
```

This function sets a boolean flag that propagates to the OVSDB Client Session
(CS) layer. The CS layer sets `binary_transport = true` in `ovsdb_cs_create()`,
so binary transport is enabled by default for all IDL instances.

The function can be called at any time. It sets a flag that affects the next
`monitor_cond_since` request. It does not force reconnection or invalidate
existing data. The flag is preserved across reconnections.

## Protocol Negotiation

When binary transport is enabled, the client adds a 5th parameter to its
`monitor_cond_since` request:

```json
["monitor_cond_since", "db", "monitor_id", {"...conds..."},
 "last_txn_id", {"format": "binary"}]
```

**New server (supports binary)**:

1. Returns a 4-element JSON reply: `[found, last_txn_id, {}, {"format": "binary"}]`
2. Streams initial snapshot as binary frames (ROW_BATCH)
3. Sends INITIAL_END frame with txn_id
4. Subsequent updates sent as binary UPDATE_BATCH frames

**Old server (no binary support)**:

1. Ignores the 5th parameter
2. Returns standard 3-element JSON reply: `[found, last_txn_id, {table_updates}]`
3. All communication remains JSON — no errors, fully transparent

## Binary Wire Protocol

Binary frames coexist with JSON-RPC on the same TCP connection. The receiver
distinguishes them by the first byte: `0xDB` (binary magic) vs. JSON
characters (`{`, `[`, `"`, digits, `t`, `f`, `n`).

Frame header (8 bytes):

```
[0]    magic      0xDB
[1]    version    0x01
[2]    msg_type   (see below)
[3]    flags      0x00 (reserved)
[4..7] payload_len  uint32_t, network byte order
```

Message types:

| Name | Value | Description |
|------|-------|-------------|
| INITIAL_BEGIN | 0x01 | Start of initial snapshot |
| ROW_BATCH | 0x02 | Batch of serialized rows |
| INITIAL_END | 0x03 | End of initial snapshot (carries txn_id) |
| UPDATE | 0x04 | Single incremental update |
| UPDATE_BATCH | 0x05 | Batch of incremental updates |

Maximum payload: 64 MB (`OVSDB_BINARY_MAX_PAYLOAD`).

## Binary Codec

The binary codec (`lib/binary-codec.c`) serializes OVSDB data types:

- **Atoms**: INTEGER (8B), REAL (8B IEEE 754), BOOLEAN (1B), STRING (4B len +
  UTF-8), UUID (16B raw)
- **Datums**: uint32 count + N keys + N values (if value_type != VOID)
- **Rows (network)**: uuid[16] | n_columns (uint16) | per column: name_len +
  name + key_type + val_type + datum

All multi-byte integers use network byte order (big-endian) on the wire.

Sanity limits: 16 MB max string, 1M max datum elements.

## Client-Side Processing

The client (`lib/ovsdb-cs.c`) processes binary frames as follows:

1. `jsonrpc_recv()` detects binary frame by magic byte
2. ROW_BATCH frames: `ovsdb_cs_process_binary_row_batch()` deserializes
   rows directly into `ovsdb_datum` values and emits `BINARY_UPDATE` events
3. Events accumulate but are NOT flushed while `binary_initial_pending`
4. INITIAL_END: flush all events, IDL processes complete snapshot,
   `has_ever_connected = true`

Transactions are blocked while `binary_initial_pending = true`.

### Direct Binary-to-Datum Path

The IDL processes `OVSDB_CS_EVENT_TYPE_BINARY_UPDATE` events via
`ovsdb_idl_process_binary_update()`, which writes pre-deserialized
`ovsdb_datum` values directly into `row->old_datum[]` without any JSON
intermediate representation. This eliminates the binary→JSON→datum
round-trip that existed in the initial implementation.

Key functions:
- `ovsdb_idl_binary_row_change()` — applies datums with same tracking
  semantics as the JSON path (seqno, bitmap, track_list)
- `ovsdb_idl_binary_insert_row()` — initializes row with binary datums
- `ovsdb_idl_binary_modify_row()` — updates existing row from binary

## Server-Side Architecture

### Disk-Backed Storage

The server uses a BINARYV1 file format for on-disk storage:

- Header: magic "BINARYV1" + version + SHA-1 schema hash + schema JSON
- Rows: append-only records (UUID + length + column data)
- Indexes: UUID->offset hmap, bloom filter, name->UUID hmap

Three query paths:

1. **Point lookup**: `_uuid == X` -> hmap -> cache -> pread (O(1))
2. **Name lookup**: `name == "foo"` -> name_index -> UUID -> point lookup (O(1))
3. **Full scan**: complex conditions -> disk cursor + condition filter

### Row Cache

- Clock-sweep LRU bounded by atom count (configurable `--cache-max-atoms`)
- States: UNLOADED -> LOADING -> CACHED (or ERROR)
- Query burst mode temporarily raises budget to 2x for table scans

### Worker Pool

For binary initial snapshots:

1. Main thread submits `disk_cursor_stream_worker_fn` jobs
2. Workers: pread -> deserialize -> serialize(binary) -> batch -> signal
3. Main thread: drain batches -> send ROW_BATCH frames
4. After all tables: send INITIAL_END

Workers bypass cache entirely (pread -> serialize -> destroy), avoiding lock
contention with the main thread.

### Three-Layer Query Engine

```
CALLERS (monitor.c, jsonrpc-server.c, transaction.c, lazy-load.c)
    |
QUERY ENGINE (plan, execute, lookup_uuid)
    |
+-- INDEX ENGINE (bloom for UUID, hash for column lookups)
+-- STORAGE ENGINE (pread, cursor_open/next, count, contains)
+-- CACHE (clock-sweep LRU, independent lifecycle)
```

Plan types:

- **POINT_LOOKUP**: `_uuid == uuid` -> bloom -> cache -> pread
- **INDEX_LOOKUP**: `column == value` + hash index -> index -> cache -> pread
- **FULL_SCAN**: complex or NULL conditions -> cursor + filter

## Files

| File | Description |
|------|-------------|
| `lib/ovsdb-cs.h` | `OVSDB_CS_EVENT_TYPE_BINARY_UPDATE`, binary update structs |
| `lib/ovsdb-cs.c` | `ovsdb_cs_process_binary_row_batch()`, binary event emission |
| `lib/ovsdb-idl.c` | `ovsdb_idl_binary_row_change/insert_row/modify_row/process_binary_update` |
| `lib/binary-codec.c` | Binary serialization/deserialization codec |
| `ovsdb/relay.c` | Binary event handling in relay mode |
