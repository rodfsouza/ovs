# OVSDB Disk-Backed Storage Architecture

## Overview

This document describes the disk-backed storage system added to OVSDB
as part of the branch-3.3-multithread effort.  It covers 41 commits
across 59 files (~14,600 lines) that transform OVSDB from a purely
in-memory database into a disk-backed system with bounded memory,
async I/O, and O(1) indexed lookups.

## Motivation

Production OVN deployments with 10,000+ chassis can have Southbound
databases exceeding 1 GB.  The stock OVSDB server loads everything
into memory at startup and keeps it there.  This causes:

- Multi-gigabyte RSS for ovsdb-server
- Slow startup (full JSON parse of the entire database)
- CLI tools (ovn-nbctl, ovs-vsctl) downloading all rows even when
  querying a single row by UUID or name

## Architecture Diagram

```
┌──────────────────────────────────────────────────────────────────┐
│                        CLIENT LAYER                              │
│                                                                  │
│  ovn-nbctl / ovs-vsctl                    ovn-controller         │
│  ┌──────────────────────┐                 ┌──────────────────┐   │
│  │ db-ctl-base.c        │                 │ IDL (ovsdb-idl.c)│   │
│  │                      │                 │                  │   │
│  │ pre_cmd_list/get()   │                 │ monitor_cond     │   │
│  │   ├─ UUID arg?       │                 │ with conditions  │   │
│  │   │  → UUID condition│                 │                  │   │
│  │   └─ name arg?       │                 │ Client-side      │   │
│  │      → name condition│                 │ skiplist indexes  │   │
│  │                      │                 │                  │   │
│  │ ovsdb_idl_select()   │                 │                  │   │
│  │   (transact/select)  │                 │                  │   │
│  └──────────┬───────────┘                 └────────┬─────────┘   │
│             │ JSON-RPC                              │ JSON-RPC    │
└─────────────┼───────────────────────────────────────┼────────────┘
              │ monitor_cond / transact               │
              ▼                                       ▼
┌──────────────────────────────────────────────────────────────────┐
│                        SERVER LAYER                              │
│                                                                  │
│  jsonrpc-server.c                                                │
│  ┌──────────────────────────────────────────────────────────┐    │
│  │ monitor_cond handler                                     │    │
│  │   ├─ Conditioned?  ──→ skip bulk-load                    │    │
│  │   │                    ovsdb_monitor_get_initial_         │    │
│  │   │                    conditioned()                      │    │
│  │   └─ Unconditioned ──→ deferred bulk-load                │    │
│  │                        ovsdb_monitor_get_initial()        │    │
│  └──────────────────────────────────────────────────────────┘    │
│                                                                  │
│  execution.c (SELECT / UPDATE / DELETE / MUTATE / WAIT)          │
│  ┌──────────────────────────────────────────────────────────┐    │
│  │ All operations ──→ ovsdb_query() ──→ ovsdb_table_query() │    │
│  └──────────────────────────────────────────────────────────┘    │
│                              │                                   │
│                              ▼                                   │
│  ┌──────────────────────────────────────────────────────────┐    │
│  │              ovsdb_table_query()  (table.c)              │    │
│  │                                                          │    │
│  │  Path 1: _uuid == X                                      │    │
│  │    └─→ ovsdb_table_get_row()                             │    │
│  │         ├─→ table->rows (hmap) — in-memory mods          │    │
│  │         ├─→ row cache (LRU)    — CACHED? return          │    │
│  │         ├─→ bloom filter       — NO? skip disk           │    │
│  │         └─→ UUID→offset        — pread() → cache+return  │    │
│  │                                                          │    │
│  │  Path 2: name == "foo"  (indexed column)                 │    │
│  │    └─→ name_index (hmap, hash_string)                    │    │
│  │         └─→ UUID ──→ Path 1                              │    │
│  │                                                          │    │
│  │  Path 3: other conditions                                │    │
│  │    └─→ table->rows scan (HMAP_FOR_EACH_SAFE)             │    │
│  │    └─→ disk cursor scan (condition-filtered,             │    │
│  │         selective caching, no cache pollution)            │    │
│  └──────────────────────────────────────────────────────────┘    │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│                       STORAGE LAYER                              │
│                                                                  │
│  ┌──────────────┐  ┌──────────────┐  ┌────────────────────────┐  │
│  │  Bloom Filter │  │  Row Cache   │  │     Disk Store         │  │
│  │ (bloom-       │  │ (row-cache.c)│  │   (disk-store.c)       │  │
│  │  filter.c)    │  │              │  │                        │  │
│  │              │  │  LRU eviction │  │  BINARYV1 format       │  │
│  │  Probabilis- │  │  Atom-budget  │  │  ┌──────────────────┐  │  │
│  │  tic UUID    │  │  bounded      │  │  │ File on disk     │  │  │
│  │  existence   │  │              │  │  │ ┌──────────────┐ │  │  │
│  │  test        │  │  States:      │  │  │ │  Header      │ │  │  │
│  │              │  │  UNLOADED     │  │  │ │  (36 bytes   │ │  │  │
│  │  10 bits/key │  │  LOADING      │  │  │ │  + schema)   │ │  │  │
│  │  7 hash fns  │  │  CACHED       │  │  │ ├──────────────┤ │  │  │
│  │  ~0.82% FP   │  │  ERROR        │  │  │ │ Row records  │ │  │  │
│  │              │  │              │  │  │ │ (append-only)│ │  │  │
│  │  Thread-safe │  │  Pinning for  │  │  │ │              │ │  │  │
│  │  (atomic ops)│  │  transactions │  │  │ │ UUID|len|    │ │  │  │
│  └──────────────┘  └──────────────┘  │  │ │ cols|data    │ │  │  │
│                                       │  │ └──────────────┘ │  │  │
│  ┌──────────────┐  ┌──────────────┐  │  └──────────────────┘  │  │
│  │  UUID→Offset  │  │  Name→UUID   │  │                        │  │
│  │  Index        │  │  Index       │  │  Ordered compaction    │  │
│  │ (hmap in      │  │ (hmap in     │  │  (group by table,     │  │
│  │  disk-store)  │  │  disk-store) │  │   sort by UUID)       │  │
│  │              │  │              │  │                        │  │
│  │  O(1) lookup  │  │  O(1) lookup │  │  rwlock protection    │  │
│  │  by UUID      │  │  by name     │  │  for concurrent       │  │
│  │              │  │              │  │  access                │  │
│  │  Built at     │  │  Built at    │  │                        │  │
│  │  startup      │  │  startup     │  │                        │  │
│  │  (header scan)│  │  (2nd pass)  │  │                        │  │
│  └──────────────┘  └──────────────┘  └────────────────────────┘  │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐    │
│  │                    Worker Pool                           │    │
│  │                  (worker-pool.c)                         │    │
│  │                                                          │    │
│  │  N threads for async disk I/O                            │    │
│  │  Submit: worker_fn(arg) → result                         │    │
│  │  Complete: done_fn(result, aux) on main thread           │    │
│  │                                                          │    │
│  │  Used by: lazy-load (row loading)                        │    │
│  │           future: async compaction                        │    │
│  └──────────────────────────────────────────────────────────┘    │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

## Data Flow: Query by Name

```
Client: ovn-nbctl list Logical_Router neutron-router-1

  1. pre_cmd_list() detects "neutron-router-1" is not a UUID
  2. ctl_set_row_condition() finds row_id with name_column = "name"
  3. Pushes monitor_cond: ["name","==","neutron-router-1"]
  4. Uses noref columns (skip referenced table monitoring)

  ┌──────────────────────────────────────────────────────┐
  │ monitor_cond request:                                │
  │   Logical_Router: where = [["name","==","neutron-…"]]│
  │   NB_Global:      where = [true]                     │
  └───────────────────────┬──────────────────────────────┘
                          │
                          ▼

Server: ovsdb_monitor_get_initial_conditioned()
  → single-clause OVSDB_F_EQ → delegates to ovsdb_table_query()

  ovsdb_table_query(Logical_Router, name=="neutron-router-1")
  → Path 2: name_index lookup
    │
    ├─ hash_string("neutron-router-1", 0)  →  bucket
    ├─ HMAP_FOR_EACH_WITH_HASH  →  disk_store_index_entry
    ├─ entry->uuid  →  UUID
    │
    └─ ovsdb_table_get_row(UUID)
       ├─ table->rows  (not found — row is on disk)
       ├─ row_cache    (not found — UNLOADED)
       ├─ bloom filter (may_contain → YES)
       ├─ UUID→offset index  →  offset=0x4A20, length=342
       └─ pread(fd, buf, 342, 0x4A20)  →  row data
          └─ deserialize  →  ovsdb_row
             └─ insert into cache (LRU)
                └─ return row to monitor
                   └─ compose JSON, send to client

  Total disk reads: 1 (just the matching row)
  Cache pollution: 0 (only the requested row cached)
```

## Data Flow: Query by UUID

```
Client: ovn-nbctl list Logical_Router 2819efbb-3fb8-4d92-8c5e-6e86f7a656c1

  1. pre_cmd_list() detects UUID format
  2. ctl_set_row_condition() pushes: ["_uuid","==",["uuid","2819…"]]
  3. Uses noref columns

Server: ovsdb_table_query(Logical_Router, _uuid==2819…)
  → Path 1: UUID exact-match
    │
    └─ ovsdb_table_get_row(2819…)
       ├─ table->rows    (miss)
       ├─ row_cache       (miss → UNLOADED)
       ├─ bloom filter    (may_contain → YES)
       ├─ UUID→offset     →  offset, length
       └─ pread()         →  row
          └─ cache + return

  Total disk reads: 1
```

## Component Summary

### Binary File Format (BINARYV1)

```
File Layout:
  ┌───────────────────────────────────┐
  │ Header (36 bytes)                 │
  │   magic: "BINARYV1"              │
  │   version: 1                     │
  │   schema_hash: SHA-1 (20 bytes)  │
  │   schema_json_len                │
  ├───────────────────────────────────┤
  │ Embedded Schema JSON             │
  │   (for self-describing files)    │
  ├───────────────────────────────────┤
  │ Row Record 1                     │
  │   UUID (16) | total_len (4)      │
  │   n_columns (2) | flags (2)      │
  │   table_name_len (2) | name      │
  │   [column: name_len | name |     │
  │    key_type | val_type | datum]…  │
  ├───────────────────────────────────┤
  │ Row Record 2                     │
  │   ...                            │
  ├───────────────────────────────────┤
  │ ...                              │
  └───────────────────────────────────┘

  Append-only writes.
  Lazy deletion (flag in index, not on disk).
  Compaction reclaims space (atomic rename).
```

### In-Memory Indexes

```
UUID→Offset Index (disk_store_index_entry):
  ┌────────┐     ┌──────────────────────────────────┐
  │  hmap  │────→│ entry: uuid, offset, length,     │
  │(by UUID│     │        table_name, deleted,       │
  │  hash) │     │        name_value, name_node,     │
  └────────┘     │        in_name_index              │
                 └──────────────────────────────────┘
                     │                    │
                     │ (same struct)      │
                     ▼                    ▼
              ┌──────────┐       ┌──────────────┐
              │ UUID hmap│       │ Name hmap    │
              │  (Path 1)│       │  (Path 2)    │
              └──────────┘       └──────────────┘

  Linked design: both indexes reference the same
  disk_store_index_entry via different hmap_nodes.
  Mutations update both atomically.
```

### Bloom Filter

```
  10 bits/key, 7 hash functions (double hashing)
  ~0.82% false positive rate
  ~122 KB for 100K keys

  Thread-safe: atomic bit operations (no locks)

  Usage: fast negative check in ovsdb_table_get_row()
    bloom says NO  → skip disk I/O (guaranteed correct)
    bloom says YES → proceed to UUID→offset lookup
                     (may be false positive, ~0.82%)
```

### Row Cache (LRU)

```
  Bounded by atom budget (configurable --cache-max-atoms)
  Default: 1M atoms per table

  States:
    UNLOADED → LOADING → CACHED
                    ↘ ERROR (after 3 retries)

  Eviction: LRU order, deferred when iterating
  Pinning: prevents eviction during transactions
```

### Worker Pool

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
      │   done_fn(result, aux)
      │
      └── pool_wait()
          seq_wait(done_seq)
```

## Commit History (41 commits)

### Phase 1: Multi-Threaded Infrastructure
- Worker pool with configurable thread count
- Thread-safe serialization primitives

### Phase 2: Binary File Format
- BINARYV1 format with embedded schema
- ovsdb-tool convert-format / db-format commands
- Full binary support in all ovsdb-tool subcommands

### Phase 3: Disk-Backed Serving
- `--disk-store` flag for ovsdb-server
- UUID→offset in-memory index built at startup
- LRU row cache with atom budget
- Async lazy-load via worker pool
- Deferred monitor for large databases
- Trigger parking for in-flight row loads

### Phase 4: Bloom Filter & Condition-Aware Query
- Bloom filter for fast negative UUID checks
- Thread-safe atomic bit operations
- Unified `ovsdb_table_query()` with three paths
- Condition-aware disk scan (no cache pollution)

### Phase 5: Client-Side Condition Push
- Pre-sync UUID conditions in CLI prerequisites
- `ovsdb_idl_select()` for one-shot server-side queries
- `ovsdb_idl_set_condition_json()` for raw JSON conditions
- Skip referenced table monitoring for targeted lookups
- Skip deferred bulk-load for conditioned sessions

### Phase 6: Concurrency Fixes & Ordered Compaction
- Compaction locking (wrlock for fd/index swap)
- Write-row lock ordering (wrlock before disk I/O)
- Cache removal before disk deletion
- Zombie cache prevention in lazy-load
- Cursor snapshot under read lock
- Bloom filter rebuild after compaction
- Ordered compaction (group by table, sort by UUID)

### Phase 7: Secondary Name Index
- Linked name→UUID index on disk_store_index_entry
- O(1) name-based lookups via hash_string (MurmurHash3/CRC32)
- Single-pass column extraction during index build
- Transaction-maintained (insert/update/delete)
- CLI name condition push (ctl_set_row_condition)
- Monitor condition widening for indexed columns

## Performance Impact

| Operation | Before | After |
|-----------|--------|-------|
| `list TABLE <uuid>` | O(n) full table scan | O(1) bloom → offset → pread |
| `list TABLE <name>` | O(n) full table scan | O(1) name_index → UUID → pread |
| Server startup (1M rows) | Load all into memory | Header scan + lazy load |
| Memory usage (1M rows) | ~1 GB RSS | ~cache budget (configurable) |
| Cache pollution on lookup | All rows cached | Only matching row cached |
| Compaction file layout | Random order | Grouped by table, sorted by UUID |
