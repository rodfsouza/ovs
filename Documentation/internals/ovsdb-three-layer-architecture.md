# OVSDB Three-Layer Data Access Architecture

This document describes the three-layer data access refactoring
implemented on `branch-3.3-three-layer-refactor`.  It covers the
design rationale, API reference, clustered index model, query
planning, index configuration, and startup optimization.

For the broader context (disk-store, cache, binary streaming), see
`ovsdb-multithread-architecture.md` and `ovsdb-disk-store-architecture.md`.

---

## 1. Problem Statement

Before this refactoring, data access was fragmented across multiple
files with duplicated logic:

| Pattern | Locations | Problem |
|---------|-----------|---------|
| Disk cursor iteration | 5 call sites | Same cursor loop copied everywhere |
| Cache insertion | 3 call sites | Cache policy embedded in callers |
| Bloom filter check | Hardcoded in `table.c` | Callers must know about bloom |
| Name index lookup | Hardcoded in `table.c` Path 2 | Only works for string columns |
| Query path selection | Hardcoded 3-way switch in `table.c` | UUID/name/scan decision in one function |
| Index creation | Hardcoded in `ovsdb.c` startup | Only one name index per table, string-only |
| Startup passes | 3 separate passes | 67M strcmp calls + pread per row |

**Goal:** Single responsibility layers — storage does I/O, indexes do
lookups, query engine does planning — with zero code duplication.

---

## 2. Architecture Overview

```
┌─────────────────────────────────────────────────────┐
│                    CALLERS                           │
│  monitor.c   jsonrpc-server.c   execution.c         │
│  transaction.c   lazy-load.c    ovsdb-tool.c        │
└────────────────────────┬────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────┐
│            QUERY ENGINE  (query-engine.h/c)         │
│                                                     │
│  plan(condition, indexes, table) → execution_plan   │
│  execute(plan, storage, indexes, cache) → iterator  │
│  lookup_uuid(storage, indexes, cache, table, uuid)  │
│  describe(plan) → EXPLAIN string                    │
│                                                     │
│  Orchestrates all three lower layers.               │
│  Decides: POINT_LOOKUP / INDEX_LOOKUP / FULL_SCAN   │
└───────┬──────────────────┬──────────────┬───────────┘
        │                  │              │
        ▼                  ▼              ▼
┌─────────────┐  ┌─────────────┐  ┌─────────────────┐
│ INDEX       │  │ STORAGE     │  │ CACHE            │
│ ENGINE      │  │ ENGINE      │  │ (row-cache.h/c)  │
│             │  │             │  │                  │
│ BLOOM: UUID │  │ read_row    │  │ lookup(uuid)     │
│  existence  │  │ cursor_*    │  │ insert(row)      │
│ HASH: col → │  │ count       │  │ remove(uuid)     │
│  entry *    │  │ contains    │  │ burst mode       │
│             │  │             │  │ sweeper thread   │
│ All point   │  │ Pure disk   │  │                  │
│ to cluster  │  │ I/O only.   │  │ Independent.     │
│ entry       │  │ No cache.   │  │ No disk I/O.     │
│             │  │ No indexes. │  │ No indexes.      │
└─────────────┘  └─────────────┘  └─────────────────┘
```

**Key design decisions:**

1. **Cache is independent** — not embedded in storage.  The query
   engine controls when to check, skip, or populate cache per query
   type (e.g., skip cache entirely for binary streaming).

2. **Indexes return pointers to the clustered entry** — not UUID
   copies.  This saves 16 bytes per secondary index entry and
   provides the disk offset in one pointer dereference.

3. **Query engine uses plan + execute** — inspectable plans allow
   EXPLAIN logging and future optimization without changing callers.

---

## 3. Layer 1: Storage Engine

**File:** `ovsdb/storage-engine.h`, `ovsdb/storage-engine.c`

Pure disk I/O facade over `ovsdb_disk_store`.  No cache, no indexes,
no query logic.

### API

```c
/* Lifecycle */
struct ovsdb_storage_engine *ovsdb_storage_engine_create(
    struct ovsdb_disk_store *);
void ovsdb_storage_engine_destroy(struct ovsdb_storage_engine *);

/* Point read: UUID → clustered index → pread */
struct ovsdb_row *ovsdb_storage_engine_read_row(
    struct ovsdb_storage_engine *, struct ovsdb_table *,
    const struct uuid *);

/* Sequential scan (cursor) */
struct ovsdb_storage_cursor *ovsdb_storage_engine_cursor_open(
    struct ovsdb_storage_engine *, const char *table_name);
struct ovsdb_row *ovsdb_storage_engine_cursor_next(
    struct ovsdb_storage_cursor *, struct ovsdb_table *);
void ovsdb_storage_engine_cursor_close(struct ovsdb_storage_cursor *);

/* Metadata */
size_t ovsdb_storage_engine_count(
    const struct ovsdb_storage_engine *, const char *table_name);
bool ovsdb_storage_engine_contains(
    const struct ovsdb_storage_engine *, const struct uuid *);

/* Escape hatch for transition */
struct ovsdb_disk_store *ovsdb_storage_engine_get_disk_store(
    const struct ovsdb_storage_engine *);
```

### Thread Safety

All read operations use `pread` (thread-safe, no shared file offset).
Write operations are main-thread only.

### Internal Structure

```c
struct ovsdb_storage_engine {
    struct ovsdb_disk_store *ds;  /* Underlying disk store (not owned) */
};
```

Cursors are cast to/from `ovsdb_disk_store_cursor` since the storage
engine adds no cursor state.

---

## 4. Layer 2: Index Engine

**File:** `ovsdb/index-engine.h`, `ovsdb/index-engine.c`

Generic secondary index interface built on the clustered index model.

### Clustered Index Model

The `disk_store_index_entry` (uuid → offset) is the **clustered
index** — like a primary key B-tree in InnoDB.  It is the single
source of truth for where a row lives on disk.

```
┌──────────────────────────────────────────────┐
│  CLUSTERED INDEX (one per row)               │
│  disk_store_index_entry:                     │
│    uuid → offset, length, table_name         │
└──────────────────┬───────────────────────────┘
                   │
      ┌────────────┼────────────┐
      ▼            ▼            ▼
  BLOOM        HASH "name"  HASH "logical_port"
  (uuid)       key→entry*  key→entry*
```

Every secondary index points to the clustered entry via **pointer**,
not a UUID copy.  Benefits:

- **Zero duplication** — saves 16 bytes per entry
- **One pread path** — every lookup ends at `entry->offset`
- **Consistent lifecycle** — one allocation per row

### Secondary Index Node (External, Option B)

```c
struct ovsdb_index_node {
    struct hmap_node hmap_node;            /* In index's hmap */
    struct disk_store_index_entry *entry;  /* → clustered entry */
    union ovsdb_atom key;                  /* Indexed column value */
};
```

Lookup path: `hash(key) → index_node → entry → entry->offset → pread`

This approach doesn't bloat the clustered entry struct, supports
unlimited secondary indexes per table, and keeps index memory
ownership clean.

### Index Types

| Type | Contains | Lookup returns | Use case |
|------|----------|---------------|----------|
| `OVSDB_IDX_BLOOM` | `bool` (probabilistic) | N/A | Fast negative UUID check |
| `OVSDB_IDX_HASH` | Exact match | `disk_store_index_entry *` | Column value → row |

### API

```c
/* Specification (declarative) */
struct ovsdb_index_spec {
    enum ovsdb_index_type type;   /* BLOOM or HASH */
    const char *name;              /* For EXPLAIN logging */
    const char *column_name;       /* HASH only */
    enum ovsdb_atomic_type key_type; /* STRING, INTEGER, UUID */
};

/* Lifecycle */
struct ovsdb_index *ovsdb_index_create(const struct ovsdb_index_spec *);
void ovsdb_index_destroy(struct ovsdb_index *);

/* Mutation */
void ovsdb_index_add(idx, key, entry);
void ovsdb_index_remove(idx, key, entry);

/* Lookup */
bool ovsdb_index_contains(idx, uuid);              /* BLOOM */
struct disk_store_index_entry *ovsdb_index_lookup(
    idx, key);                                      /* HASH */

/* BLOOM-specific */
void ovsdb_index_set_bloom_filter(idx, bloom_filter);

/* Index Set (per-table collection) */
struct ovsdb_index_set *ovsdb_index_set_from_schema(schema, bloom);
struct ovsdb_index_set *ovsdb_index_set_from_schema_with_config(
    schema, bloom, config);
struct ovsdb_index *ovsdb_index_set_find_for_column(set, col_name);
struct ovsdb_index *ovsdb_index_set_find_bloom(set);
```

### Atom Hashing

The `atom_hash` function supports all indexable types:

| Type | Hash function |
|------|--------------|
| INTEGER | `hash_2words(low32, high32)` — full 64-bit |
| REAL | `hash_bytes(&real, 8)` |
| BOOLEAN | `hash_boolean(val)` |
| STRING | `hash_string(json_string(s))` |
| UUID | `uuid_hash(&uuid)` |

---

## 5. Layer 3: Query Engine

**File:** `ovsdb/query-engine.h`, `ovsdb/query-engine.c`

Plans and executes queries.  Orchestrates storage + index + cache.

### Execution Plans

```c
enum ovsdb_plan_type {
    OVSDB_PLAN_POINT_LOOKUP,   /* UUID == <uuid> */
    OVSDB_PLAN_INDEX_LOOKUP,   /* column == <value> via HASH index */
    OVSDB_PLAN_FULL_SCAN,      /* Cursor iteration ± condition filter */
};

struct ovsdb_execution_plan {
    enum ovsdb_plan_type type;
    struct uuid target_uuid;           /* POINT_LOOKUP */
    const struct ovsdb_index *index;   /* INDEX_LOOKUP */
    union ovsdb_atom key;              /* INDEX_LOOKUP */
    const struct ovsdb_condition *condition; /* FULL_SCAN filter */
    bool use_cache;                    /* FULL_SCAN cache policy */
};
```

### Planning Algorithm

```
ovsdb_query_engine_plan(condition, indexes, table):

    if condition is NULL or trivially true:
        return FULL_SCAN (unconditioned, cache=no)

    if condition has _uuid == <uuid> clause:
        return POINT_LOOKUP

    for each EQ clause in condition:
        if HASH index exists for that column:
            return INDEX_LOOKUP

    return FULL_SCAN (with condition filter, cache=yes)
```

### EXPLAIN

Every execution is logged at DBG level (guarded by `VLOG_IS_DBG_ENABLED`
to avoid allocation on the hot path):

```
POINT_LOOKUP uuid=a93b5600-5ac4-448f-9199-a9127f10378a
INDEX_LOOKUP via "idx_name"
FULL_SCAN (unconditioned), cache=no
FULL_SCAN with condition filter, cache=yes
```

### Execution: POINT_LOOKUP

```
bloom.contains(uuid) → false? return NULL (fast negative)
cache_lookup(uuid) → hit? return cached
storage_engine_read_row(uuid) → pread at offset
cache_insert(row) → return cached pointer
  (if cache evicts immediately → return empty)
```

### Execution: INDEX_LOOKUP

```
index_lookup(key) → entry* (clustered)
cache_lookup(entry->uuid) → hit? return cached
storage_engine_read_row(entry->uuid) → pread at offset
cache_insert(row) → return cached pointer
```

### Execution: FULL_SCAN

```
cursor = storage_engine_cursor_open(table_name)
for each row from cursor_next:
    if condition and !matches: destroy, continue
    if cache: insert, return cached pointer
    else: return row directly
```

### Convenience Function

For the hot path (`ovsdb_table_get_row`), a zero-allocation function
that bypasses plan/condition/result machinery:

```c
const struct ovsdb_row *ovsdb_query_engine_lookup_uuid(
    storage, indexes, cache, table, uuid);
```

On cache hit: zero allocations, zero disk I/O.
On cache miss: one `pread`, one cache insert.

### Result Iterator

```c
struct ovsdb_query_result;

const struct ovsdb_row *ovsdb_query_result_next(result);
struct ovsdb_row *ovsdb_query_result_steal_row(result);
void ovsdb_query_result_close(result);
```

`steal_row` transfers ownership for callers that need to keep the row
beyond the result's lifetime.

---

## 6. Index Configuration

**File:** `ovsdb/index-config.h`, `ovsdb/index-config.c`

### File Format

INI-style configuration declaring additional indexes beyond schema:

```ini
# Each section is a table name.
# Schema-declared indexes are always created.
# This file adds EXTRA indexes or overrides bloom.

[Logical_Flow]
hash = logical_datapath    # HASH index on UUID column

[Port_Binding]
hash = datapath            # Beyond schema's "logical_port"

[SB_Global]
bloom = false              # maxRows=1, bloom wasteful
```

### API

```c
struct ovsdb_index_config *ovsdb_index_config_from_file(filename);
void ovsdb_index_config_destroy(config);

size_t ovsdb_index_config_get_hash_columns(
    config, table_name, &column_names);
bool ovsdb_index_config_get_bloom(config, table_name);
```

### Auto-Detection from Schema

`ovsdb_index_set_from_schema(schema, bloom)` creates:

1. **BLOOM** — always (unless config says `bloom = false`)
2. **HASH** — for each single-column schema index where:
   - `n_columns == 1`
   - `n_max == 1` (scalar, not set/map)
   - Type is STRING, INTEGER, or UUID

Multi-column indexes are skipped (future work).

### Pre-Generated OVN Configs

| File | Schema | Tables |
|------|--------|--------|
| `index-ovn-nb.conf` | OVN_Northbound | 30 |
| `index-ovn-sb.conf` | OVN_Southbound | 34 |
| `index-ovn-ic-nb.conf` | OVN_IC_Northbound | 4 |
| `index-ovn-ic-sb.conf` | OVN_IC_Southbound | 9 |

Usage: `ovsdb-server --index-config=index-ovn-sb.conf sb.db`

---

## 7. Single-Pass Index Build

**Function:** `ovsdb_disk_store_build_all_indexes()`

### Before (Multi-Pass)

| Pass | What | Cost (1.68M rows × 40 tables) |
|------|------|-------------------------------|
| 0 | `rebuild_index`: file scan → uuid→offset | 5M pread |
| 1 | `for_each_uuid` × 40: bloom filter | 67M strcmp |
| 2 | `build_name_index`: pread per row | 1.68M pread |

### After (Single-Pass)

```
HMAP_FOR_EACH (entry):
    ctx = shash_find_data(table_ctxs, entry->table_name)  /* O(1) */
    bloom_filter_add(ctx->bloom, &entry->uuid)
    if HASH indexes:
        pread(record) → extract_column_atom() → index_add()
```

- One hmap walk for ALL tables
- `shash_find` per entry (hash + one strcmp) instead of 40 × strcmp
- `pread` only for tables with HASH indexes

### Column Extraction

`disk_store_extract_column_atom()` handles all indexable types:

| Type | Extraction |
|------|-----------|
| STRING | uint32 length + bytes → `json_string_create` |
| INTEGER | 8-byte `memcpy` → `int64_t` |
| UUID | 16-byte `memcpy` → `struct uuid` |

---

## 8. Caller Migration

All callers now use the three-layer engines exclusively.  No direct
`bloom_filter_may_contain`, `disk_store_read_row`, or `name_index_find`
calls from application code.

### `ovsdb_table_get_row` (table.c)

```c
const struct ovsdb_row *
ovsdb_table_get_row(table, uuid)
{
    /* 1. In-memory hmap (transaction modifications). */
    HMAP_FOR_EACH_WITH_HASH → found? return

    /* 2. Query engine (disk-store tables). */
    if (table->storage_engine && table->index_set) {
        return ovsdb_query_engine_lookup_uuid(
            storage_engine, index_set, cache, table, uuid);
    }

    return NULL;
}
```

### Binary Streaming Worker (jsonrpc-server.c)

```c
if (job->table->storage_engine) {
    cursor = ovsdb_storage_engine_cursor_open(
        storage_engine, table_name);
    while (row = storage_engine_cursor_next(cursor, table)) {
        serialize_row → batch → flush
        ovsdb_row_destroy(row);
    }
    storage_engine_cursor_close(cursor);
}
```

### Lazy-Load Worker (lazy-load.c)

```c
row = table->storage_engine
    ? ovsdb_storage_engine_read_row(storage_engine, table, &uuid)
    : ovsdb_disk_store_read_row(disk_store, table, &uuid);
```

### Startup (ovsdb.c)

```c
ovsdb_attach_disk_store(db, cache_max_atoms):
    /* Phase 1: Create per-table structures. */
    for each table:
        cache = ovsdb_row_cache_create(max_atoms)
        bloom = ovsdb_bloom_filter_create(n_rows)
        storage_engine = ovsdb_storage_engine_create(ds)
        index_set = ovsdb_index_set_from_schema(schema, bloom)
        register in build_ctxs shash

    /* Phase 2: Single-pass index build. */
    ovsdb_disk_store_build_all_indexes(ds, &build_ctxs)

    /* Phase 3: Start sweepers. */
    for each table: ovsdb_row_cache_start_sweeper(cache)
```

---

## 9. Thread Safety

| Data | Thread | Protection |
|------|--------|-----------|
| Indexes (BLOOM, HASH) | Main thread only at startup | Built before workers start |
| Storage engine reads | Any thread | `pread` is thread-safe |
| Cache (row-cache) | Main + sweeper | rwlock |
| Query engine | Main thread | No shared state |
| Plan/result structs | Caller thread | Stack-allocated or caller-owned |

---

## 10. Testing

### Unit Tests (33 total)

| Test File | Count | Coverage |
|-----------|-------|---------|
| `test-binary-codec.c` | 9 | Codec round-trip, stress, edge cases |
| `test-index-engine.c` | 9 | HASH string/int/UUID, remove, multi, stress |
| `test-query-engine.c` | 6 | Plan selection, EXPLAIN format |
| `test-storage-engine.c` | 3 | Lifecycle, API surface |
| `test-index-config.c` | 6 | Parse, empty, unknown, config+schema, bloom=false |

### Autotest Cases

```bash
make check TESTSUITEFLAGS='-k "index config" -k "index engine" \
  -k "query engine" -k "storage engine" -k "binary codec"'
```

---

## 11. Files Reference

### New Files (Three-Layer)

| File | Lines | Purpose |
|------|-------|---------|
| `ovsdb/storage-engine.h` | 96 | Storage engine interface |
| `ovsdb/storage-engine.c` | 103 | Disk I/O facade |
| `ovsdb/index-engine.h` | 130 | Index engine interface |
| `ovsdb/index-engine.c` | 490 | BLOOM + HASH + auto-detect |
| `ovsdb/query-engine.h` | 128 | Query engine interface |
| `ovsdb/query-engine.c` | 560 | Plan + execute + EXPLAIN |
| `ovsdb/index-config.h` | 57 | Config parser interface |
| `ovsdb/index-config.c` | 231 | INI parser |

### Modified Files

| File | Changes |
|------|---------|
| `ovsdb/disk-store.h` | Public `disk_store_index_entry` + `build_all_indexes` |
| `ovsdb/disk-store.c` | `extract_column_atom` + `build_all_indexes` + `populate_hash_indexes` |
| `ovsdb/table.h` | `storage_engine` + `index_set` fields |
| `ovsdb/table.c` | `get_row` via query engine, cleanup |
| `ovsdb/ovsdb.c` | `attach_disk_store` two-phase + single-pass |
| `ovsdb/jsonrpc-server.c` | Worker via `storage_engine_cursor` |
| `ovsdb/lazy-load.c` | Worker via `storage_engine_read_row` |

### Configuration Files

| File | Description |
|------|-------------|
| `ovsdb/index.conf.example` | Annotated example with OVN tables |
| `ovsdb/index-ovn-nb.conf` | OVN Northbound (30 tables) |
| `ovsdb/index-ovn-sb.conf` | OVN Southbound (34 tables) |
| `ovsdb/index-ovn-ic-nb.conf` | IC Northbound (4 tables) |
| `ovsdb/index-ovn-ic-sb.conf` | IC Southbound (9 tables) |

---

## 12. Commit History

| Commit | Description |
|--------|-------------|
| `efe24da5d` | Phase 1: Storage engine |
| `03045e98b` | Phase 2: Index engine (BLOOM + HASH + clustered) |
| `327e7acb1` | Phase 3: Query engine (plan + execute + EXPLAIN) |
| `a486d8ac1` | Phase 4a: Wire into table at startup |
| `fb1055df1` | Phase 4b: Migrate get_row |
| `63f483f2c` | Fix dangling pointer + add lookup_uuid |
| `8244b2bf5` | Phase 4c: Migrate streaming worker |
| `de52435c2` | Phase 4e: Migrate lazy-load |
| `a75795140` | Fix 7 review issues (use-after-free, hash, EXPLAIN) |
| `7d467d6cf` | Phase 5: Index auto-detect from schema |
| `9d39bf84d` | Phase A: Single-pass index build |
| `0c67b569a` | Phase B: Index config file + INI parser |
| `49132a86a` | Remove legacy code + OVN configs + int/UUID extraction |
| `6e9fd00d8` | Config tests + bloom=false fix |
| `5be6de36c` | Documentation update |
