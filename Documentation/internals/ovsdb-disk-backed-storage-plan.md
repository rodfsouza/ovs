# Impact Analysis: Disk-Backed Storage Engine for OVSDB

## Context

OVSDB currently holds the entire database in memory as C structs (`struct ovsdb_row` with `ovsdb_datum fields[]` in `hmap`-based tables). When the database exceeds ~2GB, JSON serialization for snapshots, monitor replies, and Raft replication becomes a bottleneck — `ovsdb_to_txn_json()` builds the entire DB as one JSON object in memory, then `json_to_ds()` serializes it into another contiguous buffer, causing 2x+ memory amplification and blocking the main thread.

The proposal is to replace the in-memory storage with a disk-backed engine (e.g., SQLite or a custom B-tree/chunk structure) that loads data partially and transmits it in pieces.

---

## Impact Assessment

### 1. Core Data Layer — HIGH IMPACT

**Files:** `ovsdb/row.h`, `ovsdb/row.c`, `ovsdb/table.h`, `ovsdb/table.c`, `ovsdb/ovsdb.c`

Current design:
- `struct ovsdb_table` holds all rows in `struct hmap rows` (in-memory hash map by UUID)
- `struct ovsdb_row` is a variable-length struct with `ovsdb_datum fields[]` inline + embedded hmap_nodes for secondary indexes
- Rows accessed by UUID via `ovsdb_table_get_row()` (O(1) hash lookup) or full-table iteration via `HMAP_FOR_EACH`
- Secondary indexes are additional hmaps embedded in the row allocation itself

What changes:
- Replace `hmap rows` with disk-backed page/block storage (SQLite tables or B-tree pages)
- Row access becomes I/O-bound — need a page cache / buffer pool for hot rows
- Secondary indexes must move to the storage engine (SQLite indexes or on-disk hash indexes)
- The variable-length row struct with embedded index nodes cannot exist as-is on disk — need a serialization format

Risk: This is the foundation of the entire OVSDB. Every component (transactions, queries, monitors, replication) touches rows through these structs.

### 2. Transaction System — HIGH IMPACT

**File:** `ovsdb/transaction.c`

Current design:
- Copy-on-write: `ovsdb_txn_row_modify()` clones a row, swaps it into the live hmap
- Single-threaded, no locking — relies on main-loop exclusivity
- Commit updates all secondary indexes atomically via `ovsdb_txn_row_commit()`
- Monitor notifications fired synchronously after commit

What changes:
- COW semantics must map to the storage engine's transaction model (SQLite uses WAL + MVCC, custom engine needs explicit journaling)
- Must preserve atomicity: a transaction either fully applies or doesn't
- `ovsdb_txn_row_modify()` can no longer just `hmap_replace()` — needs storage engine write + cache invalidation
- Commit must flush to disk before notifying monitors (durability guarantee)

Risk: Transaction semantics are central to OVSDB correctness. Subtle bugs here could cause data corruption or lost updates.

### 3. Query System — MEDIUM IMPACT

**Files:** `ovsdb/query.c`, `ovsdb/condition.c`

Current design:
- Only optimization: UUID exact-match via hash lookup
- Everything else: linear scan of all rows with in-memory predicate evaluation

What changes:
- SQLite would bring a real query planner (B-tree indexes, cost-based optimization)
- Custom engine: could implement range queries, composite indexes
- `ovsdb_query()` callback pattern would need adaptation — currently iterates rows in memory, callback receives `struct ovsdb_row *`

Opportunity: This is actually an improvement area. Disk-backed indexes could make complex queries faster than the current full-scan approach, especially for large tables.

### 4. Monitor System — HIGH IMPACT

**File:** `ovsdb/monitor.c`, `ovsdb/jsonrpc-server.c`

Current design:
- Initial snapshot: iterates ALL rows, builds one JSON object, sends in single message
- Incremental updates: diffs computed per-transaction, cached as `JSON_SERIALIZED_OBJECT`
- `ovsdb_monitor_compose_update()` builds complete JSON in memory before sending

What changes:
- Initial snapshots for 2GB+ databases cannot be built as one JSON object
- Need chunked snapshot delivery: send tables/rows in batches
- Incremental updates are typically small (per-transaction) — less affected
- JSON cache must be aware of partial serialization

Protocol impact: **This is the hardest part.** See section 6.

### 5. Snapshot/Compaction — HIGH IMPACT

**File:** `ovsdb/ovsdb.c` (lines 629-682)

Current design:
- Standalone: synchronous — `ovsdb_to_txn_json()` serializes entire DB, blocks main thread
- Clustered: background thread clones DB via `ovsdb_clone_data()` (deep copy of all rows), then serializes — 2x memory during compaction

What changes:
- Disk-backed engine eliminates the need for in-memory cloning
- Snapshot = consistent read of the storage engine (SQLite: `BEGIN IMMEDIATE` + read; custom: COW pages)
- Can stream snapshot to log file in chunks instead of building full JSON first
- `malloc_trim()` call at end of compaction becomes unnecessary

Opportunity: This directly addresses the 2GB bottleneck. Largest potential win.

### 6. Wire Protocol (RFC 7047) — VERY HIGH IMPACT

**Files:** `lib/jsonrpc.c`, `ovsdb/jsonrpc-server.c`, `python/ovs/db/idl.py`, `lib/ovsdb-idl.c`

Current constraints:
- RFC 7047 mandates each JSON-RPC message is a complete, self-contained JSON object
- Both Python and C IDL clients expect complete messages — no buffering for partial data
- Python IDL: `__clear()` then `__parse_update(msg.result)` — expects full snapshot in one `msg`
- C IDL: `ovsdb_cs_update_event.table_updates` is a complete JSON object

What changes for chunked transmission:
- **Option A: Protocol extension** — Add chunking metadata (sequence number, is_final flag). Clients buffer chunks and reassemble. Breaks backward compatibility.
- **Option B: Keep protocol, change internals** — Stream JSON construction on the server side but still send complete messages. Limits the benefit for very large databases since the full message still must be assembled.
- **Option C: New protocol version** — Define `monitor_cond_since_v2` with explicit streaming support. Old clients use old protocol, new clients opt in.

Risk: Any protocol change affects every OVSDB client in the ecosystem (ovs-vsctl, ovn-controller, CMS integrations, third-party controllers). This is the primary adoption barrier.

### 7. Raft Clustering — HIGH IMPACT

**Files:** `ovsdb/raft.c`, `ovsdb/storage.c`

Current design:
- Log entries stored in memory array (`raft->entries`)
- Snapshots sent as full JSON blobs via `raft_install_snapshot_request`
- `raft_send_append_request()` references in-memory entries directly
- `ovsdb_storage_store_snapshot()` writes full JSON to log file

What changes:
- Raft log entries could reference disk-backed data instead of in-memory JSON
- Snapshot InstallSnapshot RPC would need chunked transfer (2GB+ JSON blob over RPC is impractical)
- Write ordering becomes critical: must ensure Raft log persistence and storage engine persistence stay in sync
- `raft_entry_get_serialized_data()` would need to read from disk

Risk: Raft consistency guarantees depend on specific ordering of log persistence and commit index advancement. Disk-backed storage adds I/O latency that could affect election timeouts and heartbeat intervals.

### 8. Relay Mode — MEDIUM IMPACT

**File:** `ovsdb/relay.c`

- Relay uses monitor protocol to replicate from remote
- Would inherit all monitor protocol changes (section 4/6)
- `ovsdb_txn_replay_commit()` would write to disk-backed storage instead of memory

### 9. Python Bindings — MEDIUM IMPACT

**Files:** `python/ovs/db/idl.py`, `python/ovs/db/`

- Client-side IDL maintains its own in-memory copy — not directly affected by server storage change
- But protocol changes (chunked responses) would require Python IDL updates
- `__parse_update()` assumes complete message — needs buffering layer

---

## Performance Trade-offs

| Aspect | Current (In-Memory) | Disk-Backed |
|--------|---------------------|-------------|
| Row lookup by UUID | O(1) hash, ~ns | O(log n) B-tree + page cache, ~us |
| Full table scan | In-memory iteration, fast | Sequential I/O or cached pages |
| Transaction commit | In-memory swap, ~us | Disk write + fsync, ~ms |
| Snapshot generation | 2x memory, blocks thread | Streaming, no memory spike |
| Memory usage | Entire DB in RAM | Working set only |
| Startup time | Read + parse full log | Open DB file, lazy load |
| Max DB size | Limited by RAM (~2-8GB practical) | Limited by disk (TB+) |

**Key trade-off:** Typical OVSDB operations (flow updates, port changes) are latency-sensitive at the microsecond scale. Moving to disk-backed storage adds milliseconds per transaction (fsync). This could impact control plane convergence time for large OVS deployments.

---

## OVSDB Data Characteristics (Informs Engine Choice)

Before evaluating engines, key facts about how OVSDB actually uses data:

- **Row layout**: Contiguous allocation — `struct ovsdb_row` header + `ovsdb_datum fields[]` (one per column) + `hmap_node` per index. Single `malloc`.
- **Datum model**: `ovsdb_datum` = sorted `keys[]` + optional `values[]` arrays with refcounting. Sets = keys only. Maps = keys + values. Scalars = n=0 or 1.
- **Transaction size**: Typically **1-10 rows** per transaction. Bulk inserts are rare. `n_atoms` tracks memory footprint.
- **Write pattern**: COW — clone row, modify clone, swap into hmap, update indexes. All single-threaded.
- **On-disk format**: Text header (`OVSDB <magic> <len> <sha1>\n`) + JSON body. One transaction per record. No binary format exists.
- **Change tracking**: Monitors iterate only changed rows from the transaction (via `ovsdb_txn_for_each_change()`), not full table scans. Already efficient.
- **Serialization**: `ovsdb_file_txn_add_row()` selectively serializes only non-default, persistent, changed columns. Supports column-level diffs via `ovsdb_datum_diff()`.

---

## Feasibility Assessment: Storage Engine Options

### Option A: RocksDB / LSM Tree

**How it maps to OVSDB:**
- Key: `<table_name>:<uuid>` (or `<table_id><uuid_bytes>` for binary keys)
- Value: Serialized `ovsdb_datum fields[]` (binary or JSON)
- Column families: one per table (natural partitioning, independent compaction)
- Secondary indexes: prefix-scanned column families or separate key spaces

**Pros:**
- Write-optimized (LSM): fast for OVSDB's small, frequent transactions (1-10 rows)
- Built-in snapshots via `rocksdb_checkpoint` — consistent point-in-time reads without cloning the entire DB in memory. **Directly solves the 2GB snapshot bottleneck.**
- Built-in compression (LZ4/Snappy) — 2GB JSON could shrink to ~500MB on disk
- Mature WAL, crash recovery, and atomic WriteBatch (maps to OVSDB transactions)
- Column families enable per-table iteration without scanning unrelated data
- `rocksdb_iterator` enables **streaming reads** — can serialize snapshot in chunks without holding entire DB in memory
- Widely deployed at scale (used by CockroachDB, TiKV, MyRocks)

**Cons:**
- **Read amplification**: Point lookups go through memtable → L0 → L1 → ... levels. Bloom filters mitigate but add memory. For UUID lookups this matters — current hmap is O(1) ~ns, RocksDB is ~5-20us even cached.
- **Write amplification**: LSM compaction rewrites data multiple times (typically 10-30x). For a 2GB database with frequent small writes, background compaction I/O could be significant.
- **External dependency**: ~15MB library. OVS currently has zero external DB dependencies. Build system and packaging impact.
- **Complex type mapping**: `ovsdb_datum` with sorted key/value arrays for sets and maps needs a serialization format. Options:
  - Encode as MessagePack/FlatBuffers (fast, compact)
  - Encode as JSON (compatible but slower, larger)
  - Custom binary: `[n][key1][key2]...[val1][val2]...` matching in-memory layout
- **Tuning complexity**: LSM performance depends heavily on configuration (block cache size, bloom filter bits, compaction strategy, write buffer size).
- **Memory overhead**: Block cache + memtables + bloom filters. At minimum ~100-200MB for good performance on a 2GB database.

**Fit assessment:** Good for write-heavy workloads and the snapshot problem. The read amplification is acceptable because OVSDB queries are simple (UUID lookup or full scan) and the hot working set would be in the block cache. The main concern is the dependency footprint and tuning burden.

### Option B: Custom LSM / B-Tree (No External Dependency)

**Pros:**
- Can match OVSDB's datum model exactly (no serialization layer needed for in-memory access)
- No external dependency
- Full control over page layout, caching strategy

**Cons:**
- Building a correct, crash-safe storage engine is a multi-year effort
- Must implement: page management, buffer pool, WAL, crash recovery, compaction, iterator, snapshot isolation
- Every bug is a potential data corruption issue
- Not realistic for this project scope

**Verdict:** Not recommended unless this becomes a dedicated sub-project with dedicated storage engine engineers.

### Option C: Hybrid — Disk File + In-Memory Row Cache (Recommended)

**Concept:** Keep `struct ovsdb_row` and the hmap-based access pattern for hot rows, but back the database with a disk-resident file in a format that supports row-level random access. The cache holds the working set; cold rows are evicted and loaded on demand.

**Architecture:**
```
┌─────────────────────────────┐
│  ovsdb_table (unchanged API) │
│  ├── hmap rows (cache only)  │  ← Hot rows, same struct ovsdb_row
│  └── disk_store *store       │  ← Backing file
└─────────────────┬───────────┘
                  │
    ┌─────────────▼──────────────┐
    │   Row Cache (LRU/ARC)      │
    │   - Tracks cached UUIDs    │
    │   - Evicts cold rows       │
    │   - n_atoms-based sizing   │
    ├────────────────────────────┤
    │   Disk Store               │
    │   - Binary row format      │
    │   - UUID → file offset     │  ← On-disk hash index or B-tree
    │   - Append-only + compact  │
    │   - mmap or pread access   │
    └────────────────────────────┘
```

**How it works:**
1. **Startup**: Open disk file, load the on-disk index (UUID → offset). Do NOT load all rows — only load rows as accessed.
2. **Row access** (`ovsdb_table_get_row()`): Check cache first. Cache miss → `pread()` from disk file at indexed offset → deserialize into `struct ovsdb_row` → insert into cache.
3. **Transaction commit**: Write modified rows to cache AND to disk (append to WAL or update-in-place). Evict from cache if over budget.
4. **Snapshot/compaction**: Iterate disk file sequentially in chunks. Stream to log or Raft. No need to hold entire DB in memory.
5. **Monitor initial snapshot**: Iterate disk file in chunks, serialize N rows at a time, send multiple messages (requires protocol extension for chunking).

**On-disk row format (binary, row-level addressable):**
```
[row_header: 16B uuid | 4B total_len | 2B n_columns | 2B flags]
[column_0: 2B type | 4B datum_len | datum_bytes...]
[column_1: ...]
...
```
- Each row is self-contained and independently readable
- UUID at the start enables sequential scan without full deserialization
- `total_len` enables skipping rows during iteration

**Cache eviction policy:**
- Use `n_atoms` (already tracked per-row) as the cost metric
- LRU with atom-weighted eviction: evict rows with most atoms first when cache is full
- Configurable cache size via `ovsdb-server` command-line option (e.g., `--row-cache-size=512MB`)

**Pros:**
- **Minimal API disruption**: `ovsdb_table_get_row()` returns the same `struct ovsdb_row *`. Callers don't know if it came from cache or disk.
- **No external dependency**: Custom binary format, simple implementation
- **Directly addresses the bottleneck**: Snapshots stream from disk, no 2x memory amplification
- **Incremental adoption**: Can start with cache=unlimited (all rows in memory, same behavior as today) and gradually reduce
- **Transaction path unchanged for cached rows**: COW still works — clone, modify, write-back
- **Existing `n_atoms` tracking** provides built-in cache sizing metric

**Cons:**
- **Cache miss latency**: Disk read + deserialize on miss. ~100us-1ms depending on row size and storage.
- **Must implement**: Binary format, on-disk index, WAL for crash safety, compaction for reclaiming space
- **Row lifetime management**: Currently rows are just malloc'd. With cache eviction, need to ensure no dangling pointers — rows referenced by active transactions or monitors must be pinned.
- **Full table iteration** (queries, monitor initial snapshot): Must iterate disk, not just cache. Sequential I/O is fast but slower than memory.
- **Still need protocol changes** for chunked monitor snapshots (same as other options)

**Row pinning strategy:**
- `ovsdb_txn_row_modify()` pins old + new rows (prevent eviction during transaction)
- Monitor change sets pin their `old`/`new` datum copies (already separate allocations)
- `ovsdb_query()` iteration: pin rows during callback, unpin after
- Reference counting already exists (`n_refs`) — extend it for cache pinning

### Option D: RocksDB with In-Memory Row Facade

**Concept:** Use RocksDB as the disk engine (Option A) but maintain a thin cache of deserialized `struct ovsdb_row` pointers for recently accessed rows. Best of both worlds.

- RocksDB handles: persistence, crash recovery, snapshots, compression, compaction
- Row cache handles: zero-copy access for hot rows, same API as today
- On cache miss: `rocksdb_get()` → deserialize → insert into cache

**Pros over pure RocksDB**: Avoids deserialization overhead for hot-path operations
**Pros over pure hybrid**: RocksDB's proven crash recovery, WAL, and snapshot support instead of custom implementation
**Cons**: External dependency + custom cache layer — more moving parts

---

## Recommended Approach: Option C (Hybrid) or Option D (RocksDB + Facade)

**For minimal risk and no new dependencies:** Option C (Hybrid disk + cache)
**For faster time-to-production with proven storage:** Option D (RocksDB + cache facade)

Both options share the same integration pattern:
1. `ovsdb_table_get_row()` checks cache → falls back to disk
2. Transaction commit writes to both cache and disk
3. Snapshots stream from disk in chunks
4. Wire protocol needs chunked snapshot extension (unavoidable for any disk-backed approach)

---

## Summary of Blast Radius

| Component | Files | Impact | Effort |
|-----------|-------|--------|--------|
| Row/Table storage | `ovsdb/row.{c,h}`, `ovsdb/table.{c,h}` | Rewrite | Very High |
| Transactions | `ovsdb/transaction.c` | Major refactor | Very High |
| Queries | `ovsdb/query.c`, `ovsdb/condition.c` | Moderate refactor | Medium |
| Monitors | `ovsdb/monitor.c`, `ovsdb/jsonrpc-server.c` | Protocol change | Very High |
| Snapshots | `ovsdb/ovsdb.c` | Rewrite compaction | High |
| Raft replication | `ovsdb/raft.c`, `ovsdb/storage.c` | Chunked snapshots | Very High |
| Wire protocol | `lib/jsonrpc.c` | Extension or new version | Very High |
| C IDL client | `lib/ovsdb-idl.c`, `lib/ovsdb-cs.c` | Chunking support | High |
| Python IDL client | `python/ovs/db/idl.py` | Chunking support | High |
| Relay | `ovsdb/relay.c` | Inherits monitor changes | Medium |

**Estimated scope:** This is a multi-quarter project touching ~30+ files across the OVSDB core, protocol layer, and all client implementations. The wire protocol change alone requires coordination with the entire OVS/OVN ecosystem.

---

## Proposal: Producer/Consumer Query Queue with Lazy Loading

### Current Initialization Problem

At startup, `main_loop()` calls `read_db()` which replays log entries via `ovsdb_storage_read()` one transaction at a time. For a 2GB database with thousands of transactions, this blocks the server from accepting client connections for minutes. During Raft cluster sync, `raft_install_snapshot_request` delivers the full DB as a JSON blob that must be parsed and applied before the node is operational.

### Proposed Architecture: Request Queue + Async Row Loading

```
                    ┌─────────────────────────────┐
  Client requests → │   Request Queue (per-table)  │
                    │   ┌─────────────────────┐   │
                    │   │ Pending: get row X   │   │
                    │   │ Pending: query T     │   │
                    │   │ Pending: monitor M   │   │
                    │   └─────────┬───────────┘   │
                    └─────────────┼───────────────┘
                                  │
                    ┌─────────────▼───────────────┐
                    │   Row State Tracker          │
                    │   UUID → { UNLOADED |        │
                    │            LOADING  |        │
                    │            CACHED }          │
                    └─────────────┬───────────────┘
                                  │
               ┌──────────────────┼──────────────────┐
               │                  │                   │
    ┌──────────▼────┐  ┌─────────▼──────┐  ┌────────▼────────┐
    │  Row Cache    │  │  Disk Store    │  │  Loader Thread   │
    │  (hot rows)   │  │  (all rows)   │  │  Pool (N workers)│
    │  hmap-based   │  │  binary file  │  │  pread + deser   │
    └───────────────┘  └───────────────┘  └──────────────────┘
```

**How it works:**

1. **Startup**: Open disk file, load UUID index (small — just UUID→offset pairs). Mark all rows as `UNLOADED`. Start accepting connections **immediately**.

2. **Client sends request** (transact, monitor, query):
   - For each row needed: check state tracker
   - If `CACHED`: serve immediately from hmap (same as today)
   - If `UNLOADED`: mark as `LOADING`, enqueue load request to loader pool, **park the client request**
   - If `LOADING`: another request already triggered the load — **park this request too** (coalesce)

3. **Loader thread pool** (new, N worker threads):
   - Dequeues load requests
   - `pread()` row from disk at indexed offset → deserialize to `struct ovsdb_row`
   - Insert into cache, update state to `CACHED`
   - **Signal main thread** via `seq_change()` (same pattern as compaction)
   - Main loop's `poll_block()` wakes, resumes parked requests

4. **Monitor initial snapshot**: Instead of building full snapshot in one shot, iterate UUID index in batches. Load N rows → serialize → send chunk → load next N. Client sees progressive loading.

5. **Raft snapshot receive**: Instead of parsing full JSON blob, write chunks to disk store directly. Index incrementally. Node becomes partially operational as rows load.

### Integration with Current Architecture

**The key insight**: The current main loop already supports async patterns. Raft commands use `ovsdb_write` with `ovsdb_write_is_complete()` polling. Triggers (`ovsdb/trigger.c`) already park client transactions and resume them later. The producer/consumer pattern extends this existing mechanism.

**Mapping to existing code:**

| Current Pattern | New Pattern |
|----------------|-------------|
| `ovsdb_trigger` parks a transaction waiting for Raft commit | `ovsdb_trigger` parks a transaction waiting for row load |
| `ovsdb_write_is_complete()` polls Raft | `ovsdb_row_is_loaded()` polls cache state |
| `seq_change()` signals compaction done | `seq_change()` signals row batch loaded |
| `poll_block()` wakes on Raft events | `poll_block()` wakes on loader events |
| `ovsdb_storage_read()` replays one txn per loop | Loader threads replay rows in parallel |

**Changes to `ovsdb_trigger`** (`ovsdb/trigger.c`):
- Currently: trigger executes transaction → if storage write pending, park → resume when write completes
- New: trigger executes transaction → if rows not loaded, park → resume when rows loaded
- The trigger infrastructure already handles parking/resuming — just add a new "waiting for data" state

---

## Proposal: Multi-Threaded Serialization

### Current Single-Thread Bottleneck

OVSDB uses a **single-threaded reactor pattern** (`ovsdb-server.c:main_loop()`):
- All client I/O, Raft consensus, transaction execution, and serialization run on one thread
- `poll_block()` (wraps `poll()`/`epoll()`) multiplexes all connections
- No mutexes in OVSDB core — correctness relies on single-threaded execution
- Only exception: `compaction_thread()` for clustered snapshots (operates on a cloned DB)

**What blocks the main thread today:**
- `ovsdb_to_txn_json()` — serializes entire DB for snapshots (standalone mode)
- `json_to_ds()` — converts JSON tree to string (called for every client response)
- `ovsdb_monitor_compose_update()` — builds monitor update JSON
- Large `jsonrpc_send()` — serializes and queues large messages

### Proposed Threading Model

**Do NOT make the transaction engine multi-threaded.** The COW semantics, monitor notifications, and reference counting all assume single-threaded access. Making this concurrent would require pervasive locking and is extremely error-prone.

**Instead, offload these specific CPU-bound operations to worker threads:**

#### Thread 1: Main Loop (unchanged role, but lighter)
- Client connection accept/dispatch
- Transaction execution (single-threaded, no locking needed)
- Raft consensus protocol
- Monitor change tracking (already efficient — iterates only changed rows)
- **Delegates** serialization to worker pool

#### Thread Pool: Serialization Workers (NEW)
- **JSON serialization**: `json_to_ds()` for large objects
- **Snapshot generation**: `ovsdb_to_txn_json()` for standalone mode (currently blocks main thread)
- **Monitor initial snapshots**: serialize table chunks in parallel (one worker per table)
- **Raft snapshot encoding**: serialize snapshot data for InstallSnapshot RPC

#### Thread Pool: Disk I/O Workers (NEW, from producer/consumer proposal)
- Row loading from disk on cache miss
- Background prefetching of likely-needed rows
- WAL writes (can be async with `fdatasync()` in background)

### Serialization Offload Pattern

```
Main Thread                          Serialization Worker Pool
    │                                        │
    ├─ Transaction commits                   │
    │  (modifies rows in-place,              │
    │   single-threaded, fast)               │
    │                                        │
    ├─ Monitor detects changes               │
    │  Builds lightweight change list        │
    │  (list of row UUIDs + old/new ptrs)    │
    │                                        │
    ├─ Enqueue serialization job ──────────► │
    │  {change_list, client_id, format}      ├─ json_to_ds() on change_list
    │                                        ├─ Produces serialized buffer
    │  ... handles other clients ...         │
    │                                        │
    │  ◄─── seq_change() signals done ──────┤
    ├─ Dequeue result buffer                 │
    ├─ jsonrpc_send(pre_serialized_buffer)   │
    │                                        │
```

**Key constraint — data immutability window:**
- When a serialization job is enqueued, the referenced row data must not be modified until serialization completes
- This is naturally satisfied because:
  - Transactions create new row versions (COW) — old versions are stable
  - Monitor change sets already snapshot old/new datum values
  - The serialization worker reads immutable data

### What MUST Stay Single-Threaded

| Component | Why |
|-----------|-----|
| Transaction execution | COW with `hmap_replace()` — no concurrent writers |
| Monitor change tracking | Iterates `txn_rows` after commit — must be atomic with commit |
| Raft log append | Leadership state, commit index — must be linearizable |
| Reference counting (`n_refs`) | Not atomic — would need `atomic_fetch_add` if shared |
| Secondary index updates | Concurrent index modification would corrupt hmaps |
| Cache eviction decisions | Must coordinate with pinning — single-threaded is simpler |

### What CAN Be Parallelized

| Operation | Current Cost | Parallelization Strategy |
|-----------|-------------|-------------------------|
| `json_to_ds()` for large objects | O(n) CPU, blocks main thread | Worker thread, signal via `seq` |
| Snapshot for standalone DB | Blocks main thread for seconds | Worker thread on COW snapshot (like clustered mode already does) |
| Monitor initial snapshot | One big JSON blob | Chunk by table, parallelize across workers |
| Raft InstallSnapshot encoding | Large JSON serialization | Worker thread, async send |
| Disk row loading | I/O bound, blocks on cache miss | I/O worker pool |
| WAL/log writes | `fwrite()` + `fflush()` | Async write with `fdatasync()` worker |

### Concrete Changes Required

**New files:**
- `ovsdb/worker-pool.c` — Generic thread pool with `seq`-based signaling
- `ovsdb/serialize-worker.c` — JSON serialization job queue

**Modified files:**
- `ovsdb/ovsdb-server.c` — Initialize worker pool in `main()`, poll worker completion in `main_loop()`
- `ovsdb/ovsdb.c` — Standalone snapshot uses worker thread (extend `compaction_thread` pattern to all modes)
- `ovsdb/jsonrpc-server.c` — `ovsdb_jsonrpc_session_run()` enqueues serialization instead of inline `json_to_ds()`
- `ovsdb/monitor.c` — Initial snapshot generation delegates to worker pool
- `lib/jsonrpc.c` — Accept pre-serialized buffers (`ofpbuf`) to skip inline serialization

**Unchanged files (critical):**
- `ovsdb/transaction.c` — Stays single-threaded
- `ovsdb/row.c`, `ovsdb/table.c` — No locking added
- `ovsdb/raft.c` — Stays in main loop (except snapshot encoding)

---

## Revised Phased Implementation Strategy

### Phase 0: Standalone Snapshot Threading (Quick Win)
- Extend the `compaction_thread()` pattern (already exists for clustered mode) to standalone databases
- Currently standalone snapshots call `ovsdb_to_txn_json()` synchronously on the main thread
- Change: clone DB → serialize in background → signal via `seq` → write to log
- **Zero architectural risk** — identical to what clustered mode already does
- Files: `ovsdb/ovsdb.c` only
- Benefit: Standalone DB snapshots no longer block the main loop

### Phase 1: Disk Store + Row Cache + UUID Index
- Implement binary row format and disk store
- Implement row cache with LRU eviction and pinning
- Load UUID→offset index at startup (small, fast)
- **Rows still loaded eagerly at startup** (behavior unchanged), but evictable after
- Files: new `ovsdb/disk-store.c`, `ovsdb/row-cache.c`, modify `ovsdb/row.c`, `ovsdb/table.c`

### Phase 2: I/O Worker Pool + Lazy Loading
- Add worker thread pool for disk I/O
- Startup: load only the UUID index, accept connections immediately
- Row cache misses → enqueue to I/O worker → park request via trigger mechanism
- Extend `ovsdb_trigger` with "waiting for data" state
- Files: new `ovsdb/worker-pool.c`, modify `ovsdb/trigger.c`, `ovsdb/ovsdb-server.c`

### Phase 3: Serialization Worker Pool
- Offload `json_to_ds()` for large payloads to worker threads
- Monitor initial snapshots built in parallel (one table per worker)
- `jsonrpc_send()` accepts pre-serialized buffers
- Files: new `ovsdb/serialize-worker.c`, modify `ovsdb/jsonrpc-server.c`, `ovsdb/monitor.c`, `lib/jsonrpc.c`

### Phase 4: Chunked Monitor Protocol (V4)
- Define `monitor_cond_since` V4 with chunked initial snapshot
- Server sends N rows per chunk, client assembles
- Backward compatible: old clients use V1-V3
- Files: `ovsdb/monitor.c`, `ovsdb/jsonrpc-server.c`, `lib/ovsdb-idl.c`, `python/ovs/db/idl.py`

### Phase 5: Chunked Raft Snapshots
- `raft_install_snapshot_request` sends data in chunks
- Receiver writes chunks to disk store incrementally
- Node becomes partially operational during sync
- Files: `ovsdb/raft.c`, `ovsdb/storage.c`

### Phase 6: Tuning and Hardening
- Cache sizing heuristics based on `n_atoms`
- Worker pool sizing (CPU cores vs I/O parallelism)
- Crash recovery testing for disk store
- Performance regression testing for control plane latency

---

## Overall Impact Computation

### Lines of Code in Scope

**Directly modified files (by phase):**

| File | Lines | Phase | Change Type |
|------|------:|-------|-------------|
| `ovsdb/ovsdb.c` | 758 | 0,1 | Extend compaction to standalone; integrate disk store |
| `ovsdb/row.c` | 552 | 1 | Add serialization/deserialization, cache pinning |
| `ovsdb/row.h` | 206 | 1 | Add row state enum, pin/unpin API |
| `ovsdb/table.c` | 427 | 1 | Replace hmap with cache-backed access |
| `ovsdb/table.h` | 91 | 1 | Add `disk_store *` field, cache metadata |
| `ovsdb/trigger.c` | 464 | 2 | Add "waiting for data" state |
| `ovsdb/ovsdb-server.c` | 3,174 | 2,3 | Init worker pools, poll completion in main_loop |
| `ovsdb/jsonrpc-server.c` | 1,936 | 3,4 | Async serialization, chunked monitor sends |
| `ovsdb/monitor.c` | 1,874 | 3,4 | Chunked initial snapshot, parallel table serialization |
| `lib/jsonrpc.c` | 1,362 | 3 | Accept pre-serialized buffers |
| `lib/ovsdb-idl.c` | 4,516 | 4 | Handle chunked monitor V4 responses |
| `lib/ovsdb-cs.c` | 2,388 | 4 | Chunk reassembly in client session |
| `python/ovs/db/idl.py` | 2,394 | 4 | Buffer and reassemble chunked updates |
| `ovsdb/raft.c` | 5,300 | 5 | Chunked InstallSnapshot, disk-backed entry refs |
| `ovsdb/raft-rpc.c` | 1,071 | 5 | Chunk framing in Raft RPCs |
| `ovsdb/storage.c` | 671 | 5 | Streaming snapshot store/read |
| `ovsdb/log.c` | 1,025 | 1,2 | Streaming write support |
| `ovsdb/file.c` | 634 | 1 | Binary format alongside JSON |
| `ovsdb/transaction.c` | 1,757 | 1,2 | Cache write-back on commit, disk flush |
| `ovsdb/query.c` | 100 | 1 | Disk-aware iteration |
| `ovsdb/condition.c` | 615 | — | Unchanged (evaluates in-memory datums) |
| `ovsdb/relay.c` | 433 | 4 | Inherits chunked monitor changes |
| `lib/json.c` | 1,822 | 3 | Thread-safe serialization entry points |
| **TOTAL MODIFIED** | **33,570** | | |

**New files to create:**

| File | Est. Lines | Phase | Purpose |
|------|------:|-------|---------|
| `ovsdb/disk-store.c` | ~1,500 | 1 | Binary row format, UUID→offset index, pread/pwrite |
| `ovsdb/disk-store.h` | ~150 | 1 | Public API |
| `ovsdb/row-cache.c` | ~800 | 1 | LRU cache, n_atoms-weighted eviction, pinning |
| `ovsdb/row-cache.h` | ~100 | 1 | Public API |
| `ovsdb/worker-pool.c` | ~500 | 2 | Generic thread pool, seq-based signaling |
| `ovsdb/worker-pool.h` | ~80 | 2 | Public API |
| `ovsdb/serialize-worker.c` | ~600 | 3 | JSON serialization job queue |
| `ovsdb/serialize-worker.h` | ~80 | 3 | Public API |
| **TOTAL NEW** | **~3,810** | | |

### Impact by Category

**Scope summary:**
- **Existing code to modify**: ~33,570 lines across 22 files (not all lines change — estimated 30-50% of each file touched)
- **New code to write**: ~3,810 lines across 8 new files
- **Estimated net new + changed lines**: ~12,000-17,000
- **Test code** (not counted above): Each phase needs new `.at` test modules — estimated ~3,000-5,000 lines of tests

### Risk Matrix by Phase

| Phase | Description | Risk | Reversibility | Dependencies |
|-------|-------------|------|---------------|--------------|
| **0** | Standalone snapshot threading | **LOW** | Full — revert one file | None |
| **1** | Disk store + row cache | **MEDIUM** | Moderate — cache=unlimited matches current behavior | None |
| **2** | I/O worker pool + lazy loading | **HIGH** | Moderate — eager load fallback | Phase 1 |
| **3** | Serialization workers | **MEDIUM** | Full — can inline serialize as fallback | Phase 0 |
| **4** | Chunked monitor protocol V4 | **HIGH** | Backward compatible — old clients use V3 | Phase 1,3 |
| **5** | Chunked Raft snapshots | **VERY HIGH** | Low — affects cluster consensus | Phase 1,2 |
| **6** | Tuning + hardening | **LOW** | Full — configuration only | All phases |

### Performance Impact Model

**Latency regression (worst case, per operation):**

| Operation | Current | After Phase 1-2 (cache hit) | After Phase 1-2 (cache miss) |
|-----------|---------|----------------------------|------------------------------|
| Row lookup by UUID | ~50ns (hmap) | ~50ns (same hmap) | ~100-500us (pread + deser) |
| Transaction commit (1-10 rows) | ~1-10us | ~10-50us (+ disk write) | ~1-5ms (+ fsync) |
| Monitor incremental update | ~10-100us | ~10-100us (changed rows in cache) | Same — changes are always cached |
| Monitor initial snapshot (2GB) | ~30-60s (blocks main thread) | ~5-10s (streamed, non-blocking) | Same |
| Standalone snapshot | ~30-60s (blocks main thread) | ~0s main thread (Phase 0 offloads) | Background: 10-30s |
| Startup time (2GB DB) | ~60-120s (full replay) | ~1-5s (index only, Phase 2) | Rows load on demand |
| Raft snapshot transfer | ~60s+ (single JSON blob) | ~60s (unchanged until Phase 5) | ~30-60s (chunked, Phase 5) |

**Throughput impact:**
- Serialization worker pool (Phase 3) frees main thread for ~30-60% more transaction throughput when many clients are connected
- I/O worker pool (Phase 2) enables concurrent row loading — startup goes from sequential to parallel
- Chunked protocol (Phase 4) allows clients to begin operating before full snapshot received

### Memory Impact Model

| Scenario | Current | After All Phases |
|----------|---------|-----------------|
| 2GB database, idle | 2GB resident | ~200MB (index + hot cache) |
| 2GB database, snapshot | 4GB+ peak (clone + serialize) | ~200MB (streaming from disk) |
| 2GB database, 100 monitors | 2GB + update cache | ~200MB + update cache |
| Raft cluster join (2GB) | 4GB+ (receive + parse + apply) | ~200MB (chunked receive to disk) |

### Ecosystem / External Impact

| Affected Party | Impact | When |
|----------------|--------|------|
| ovs-vswitchd (C IDL client) | Must upgrade to support monitor V4 | Phase 4 |
| ovn-controller (C IDL client) | Must upgrade to support monitor V4 | Phase 4 |
| Python IDL clients (CMS plugins, etc.) | Must upgrade `ovs.db.idl` | Phase 4 |
| Third-party OVSDB clients | Must implement V4 or stay on V3 | Phase 4 |
| Raft cluster peers | All nodes must upgrade together for Phase 5 | Phase 5 |
| OVS packaging (RPM, DEB, etc.) | New files in build, optional RocksDB dep (Option D) | Phase 1 |
| OVS CI / test infrastructure | New test suites, perf benchmarks | All phases |

### Effort Estimate by Phase

| Phase | Estimated Effort | Deliverable |
|-------|-----------------|-------------|
| **0**: Standalone snapshot threading | 1-2 weeks | Non-blocking snapshots for standalone mode |
| **1**: Disk store + row cache | 6-8 weeks | Binary format, cache, eviction, pinning |
| **2**: I/O workers + lazy loading | 4-6 weeks | Instant startup, request parking |
| **3**: Serialization workers | 4-6 weeks | Non-blocking JSON serialization |
| **4**: Chunked monitor V4 | 6-8 weeks | Protocol extension + all IDL clients |
| **5**: Chunked Raft snapshots | 6-8 weeks | Cluster sync without full-DB transfer |
| **6**: Tuning + hardening | 4-6 weeks | Production readiness |
| **TOTAL** | **~8-11 months** | Full disk-backed OVSDB with lazy load + threading |

### Cumulative Value Delivery

```
Phase 0 (week 2)    ████░░░░░░░░░░░░░░░░  Standalone snapshots non-blocking
Phase 1 (week 10)   ████████░░░░░░░░░░░░  Memory capped, snapshots stream from disk
Phase 2 (week 16)   ████████████░░░░░░░░  Instant startup, parallel row loading
Phase 3 (week 22)   ██████████████░░░░░░  Main thread unblocked for serialization
Phase 4 (week 30)   ████████████████░░░░  Clients handle 2GB+ databases
Phase 5 (week 38)   ██████████████████░░  Cluster sync without OOM
Phase 6 (week 44)   ████████████████████  Production-hardened
```

Phase 0 alone delivers immediate relief for the most common complaint (standalone snapshot blocking). Phases 0-2 together solve the memory problem without any protocol changes.
