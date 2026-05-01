# Plan: Three-Layer Data Access Architecture

## Context

The current codebase has fragmented data access: 5 cursor sites, 3 cache
insertion points, hardcoded index creation, and query path selection embedded
in `table.c`. This refactoring separates concerns into three layers matching
how real databases work.

## Architecture

```
┌─────────────────────────────────────────────────┐
│              CALLERS                             │
│  (monitor.c, jsonrpc-server.c, execution.c,     │
│   transaction.c, ovsdb-tool.c)                  │
└────────────────────┬────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────┐
│           QUERY ENGINE  (query-engine.h/c)      │
│                                                 │
│  plan(condition, columns) → execution_plan      │
│  execute(plan) → row_iterator                   │
│  stream(plan, batch_cb) → stream_job            │
│                                                 │
│  Orchestrates all three lower layers:           │
│    - Consults INDEX ENGINE for available indexes │
│    - Reads via STORAGE ENGINE (disk I/O)        │
│    - Checks/populates CACHE (row cache)         │
│    - Decides: point lookup / index / full scan  │
│    - Produces inspectable plan (EXPLAIN)        │
└──────┬──────────────┬──────────────┬────────────┘
       │              │              │
       ▼              ▼              ▼
┌────────────┐ ┌────────────┐ ┌────────────────┐
│  INDEX     │ │  STORAGE   │ │  CACHE         │
│  ENGINE    │ │  ENGINE    │ │  (row-cache.h) │
│            │ │            │ │                │
│ Generic    │ │ Disk I/O:  │ │ Existing:      │
│ indexes:   │ │  read_row  │ │  lookup(uuid)  │
│  create    │ │  cursor_*  │ │  insert(row)   │
│  lookup    │ │  write_row │ │  remove(uuid)  │
│  insert    │ │  delete    │ │  enter_burst   │
│  remove    │ │            │ │  exit_burst    │
│  contains  │ │ No cache.  │ │  sweeper       │
│            │ │ No indexes.│ │                │
│ All point  │ │ No queries.│ │ Standalone.    │
│ to cluster │ │            │ │ No disk I/O.   │
│ entry      │ │            │ │ No indexes.    │
└────────────┘ └────────────┘ └────────────────┘
```

**Key separations:**
- **Cache** (`row-cache.h/c`) — independent component, not inside storage.
  Query engine controls when to check/populate/skip.
- **Indexes** — all secondary indexes point to the clustered
  `disk_store_index_entry`, not UUID copies.
- **Storage** — pure disk I/O, no intelligence.
- **Query engine** — all intelligence: planning, index selection, caching policy.

---

## Layer 1: Storage Engine (`ovsdb/storage-engine.h/c`)

Pure disk I/O.  No cache, no indexes, no query logic.

```c
struct ovsdb_storage_engine;

/* --- Lifecycle --- */
struct ovsdb_storage_engine *ovsdb_storage_engine_create(
    struct ovsdb_disk_store *);
void ovsdb_storage_engine_destroy(struct ovsdb_storage_engine *);

/* --- Point Read (clustered entry → pread) --- */
struct ovsdb_row *ovsdb_storage_engine_read_row(
    struct ovsdb_storage_engine *,
    struct ovsdb_table *,
    const struct disk_store_index_entry *entry);

/* --- Point Read by UUID (lookup in clustered index first) --- */
struct ovsdb_row *ovsdb_storage_engine_read_row_by_uuid(
    struct ovsdb_storage_engine *,
    struct ovsdb_table *,
    const struct uuid *);

/* --- Sequential Scan (cursor) --- */
struct ovsdb_storage_cursor *ovsdb_storage_engine_cursor_open(
    struct ovsdb_storage_engine *, const char *table_name);
struct ovsdb_row *ovsdb_storage_engine_cursor_next(
    struct ovsdb_storage_cursor *, struct ovsdb_table *);
void ovsdb_storage_engine_cursor_close(struct ovsdb_storage_cursor *);

/* --- Write --- */
void ovsdb_storage_engine_write_row(struct ovsdb_storage_engine *,
                                     const struct ovsdb_row *);
void ovsdb_storage_engine_delete_row(struct ovsdb_storage_engine *,
                                      const struct uuid *);

/* --- Clustered index access --- */
struct disk_store_index_entry *ovsdb_storage_engine_find_entry(
    struct ovsdb_storage_engine *, const struct uuid *);
size_t ovsdb_storage_engine_count(struct ovsdb_storage_engine *,
                                   const char *table_name);
```

**What it does NOT do:**
- No cache (cache is a separate component)
- No bloom filter checks (that's an index)
- No column lookups (that's an index)
- No query planning (that's the query engine)
- No condition evaluation

---

## Cache (`ovsdb/row-cache.h/c` — existing, unchanged)

The row cache remains its own independent component with its existing
API.  It is NOT part of the storage engine.

The **query engine** decides when to:
- Check cache before reading from storage (point lookups)
- Skip cache entirely (binary streaming to client)
- Populate cache after a storage read (on-demand warming)
- Enter/exit burst mode (full table scans)

This gives the query engine full control over caching policy per query
type, rather than embedding cache logic in the storage layer.

---

## Layer 2: Index Engine (`ovsdb/index-engine.h/c`)

Generic index interface built on top of a **clustered index** model.

### Clustered Index Model

The `disk_store_index_entry` (uuid → offset) is the **clustered index**
— like a primary key B-tree in InnoDB.  It is the single source of truth
for where a row lives on disk.  Every secondary index points to the same
clustered entry via pointer, not a UUID copy.

```
┌─────────────────────────────────────────────────────┐
│  CLUSTERED INDEX (primary key)                      │
│  disk_store_index_entry (one per row)               │
│    uuid → offset, length, table_name, deleted       │
│    (owns the disk location — single allocation)     │
└──────────────────────┬──────────────────────────────┘
                       │
          ┌────────────┼────────────┐
          │            │            │
          ▼            ▼            ▼
   ┌────────────┐ ┌──────────┐ ┌──────────────┐
   │ BLOOM      │ │ HASH idx │ │ HASH idx     │
   │ (uuid)     │ │ "name"   │ │ "logical_port│
   │            │ │          │ │              │
   │ contains?  │ │ key →    │ │ key →        │
   │  → bool    │ │ entry *  │ │  entry *     │
   └────────────┘ └──────────┘ └──────────────┘
                       │            │
                  pointer to same
               disk_store_index_entry
```

### Secondary Index Node (Option B — external, unlimited)

```c
/* Each secondary index owns lightweight nodes pointing to clustered. */
struct ovsdb_index_node {
    struct hmap_node hmap_node;            /* In index's hmap */
    struct disk_store_index_entry *entry;  /* → clustered entry */
    union ovsdb_atom key;                  /* Indexed column value */
    enum ovsdb_atomic_type key_type;       /* For proper destroy */
};
```

Lookup path: `hash(key) → index_node → entry → entry->offset → pread`

Benefits over inline (Option A):
- No clustered entry bloat
- Unlimited secondary indexes per table
- Index engine owns its own memory — clean lifecycle

### Interface

```c
struct disk_store_index_entry;

/* --- Index Types --- */
enum ovsdb_index_type {
    OVSDB_IDX_BLOOM,     /* Probabilistic UUID existence (contains only) */
    OVSDB_IDX_HASH,      /* Column value → clustered entry pointer */
    /* Future: OVSDB_IDX_BTREE for range queries */
};

/* --- Index Specification (declarative) --- */
struct ovsdb_index_spec {
    enum ovsdb_index_type type;
    char *name;                  /* Index name for logging/EXPLAIN */
    size_t n_columns;
    const char **column_names;   /* Which column(s) the index covers */
};

/* --- Index Handle (opaque) --- */
struct ovsdb_index;

/* --- Lifecycle --- */
struct ovsdb_index *ovsdb_index_create(
    const struct ovsdb_index_spec *);
void ovsdb_index_destroy(struct ovsdb_index *);

/* --- Mutation --- */
void ovsdb_index_add(struct ovsdb_index *,
                      const union ovsdb_atom *key,
                      struct disk_store_index_entry *entry);
void ovsdb_index_remove(struct ovsdb_index *,
                         const union ovsdb_atom *key,
                         struct disk_store_index_entry *entry);

/* --- Lookup --- */
bool ovsdb_index_contains(const struct ovsdb_index *,
                           const struct uuid *uuid);
struct disk_store_index_entry *ovsdb_index_lookup(
    const struct ovsdb_index *,
    const union ovsdb_atom *key);

/* --- Metadata --- */
enum ovsdb_index_type ovsdb_index_get_type(const struct ovsdb_index *);
const char *ovsdb_index_get_name(const struct ovsdb_index *);
size_t ovsdb_index_get_count(const struct ovsdb_index *);

/* --- Index Set (per-table collection of indexes) --- */
struct ovsdb_index_set {
    struct ovsdb_index **indexes;
    size_t n_indexes;
};

struct ovsdb_index_set *ovsdb_index_set_from_schema(
    const struct ovsdb_table_schema *,
    struct ovsdb_storage_engine *);
void ovsdb_index_set_destroy(struct ovsdb_index_set *);
const struct ovsdb_index *ovsdb_index_set_find_for_column(
    const struct ovsdb_index_set *, const char *column_name);
const struct ovsdb_index *ovsdb_index_set_find_bloom(
    const struct ovsdb_index_set *);
```

---

## Layer 3: Query Engine (`ovsdb/query-engine.h/c`)

Plans and executes queries.  Orchestrates storage + index + cache.

```c
/* --- Execution Plan --- */
enum ovsdb_plan_type {
    OVSDB_PLAN_POINT_LOOKUP,   /* UUID → bloom + clustered + storage */
    OVSDB_PLAN_INDEX_LOOKUP,   /* Column → secondary idx → clustered → storage */
    OVSDB_PLAN_FULL_SCAN,      /* Cursor iteration with optional filter */
};

struct ovsdb_execution_plan {
    enum ovsdb_plan_type type;
    const char *description;

    /* POINT_LOOKUP: */
    struct uuid target_uuid;

    /* INDEX_LOOKUP: */
    const struct ovsdb_index *index;
    union ovsdb_atom key;
    enum ovsdb_atomic_type key_type;

    /* FULL_SCAN: */
    const struct ovsdb_condition *condition;
    bool use_cache;       /* true = check/populate cache per row */
};

/* --- Planning --- */
struct ovsdb_execution_plan *ovsdb_query_engine_plan(
    const struct ovsdb_condition *,
    const struct ovsdb_column_set *,
    const struct ovsdb_index_set *,
    const struct ovsdb_table *);
void ovsdb_execution_plan_destroy(struct ovsdb_execution_plan *);

/* --- EXPLAIN --- */
const char *ovsdb_execution_plan_describe(
    const struct ovsdb_execution_plan *);

/* --- Execution --- */
struct ovsdb_query_result *ovsdb_query_engine_execute(
    const struct ovsdb_execution_plan *,
    struct ovsdb_storage_engine *,
    struct ovsdb_index_set *,
    struct ovsdb_row_cache *);        /* NULL = skip cache */
const struct ovsdb_row *ovsdb_query_result_next(
    struct ovsdb_query_result *);
void ovsdb_query_result_close(struct ovsdb_query_result *);

/* --- Async Streaming --- */
struct ovsdb_stream_handle *ovsdb_query_engine_stream(
    const struct ovsdb_execution_plan *,
    struct ovsdb_storage_engine *,
    struct ovsdb_index_set *,
    struct ovsdb_row_cache *,         /* NULL = skip cache */
    struct ovsdb_worker_pool *,
    const struct ovsdb_column_set *,
    ovsdb_stream_batch_fn batch_fn,
    void *aux);
void ovsdb_stream_handle_cancel(struct ovsdb_stream_handle *);
```

### Query Planning Logic

```
ovsdb_query_engine_plan(condition, columns, indexes, table):

  if condition is NULL:
      return FULL_SCAN (no filter, use_cache=false for streaming)

  if condition has _uuid == <uuid> clause:
      bloom = index_set_find_bloom(indexes)
      if bloom and !index_contains(bloom, uuid):
          return POINT_LOOKUP (will return empty — bloom negative)
      return POINT_LOOKUP with target_uuid

  for each clause in condition:
      idx = index_set_find_for_column(indexes, clause.column)
      if idx and clause.function == EQ:
          return INDEX_LOOKUP with index + key

  return FULL_SCAN (with condition filter, use_cache=true)
```

### EXPLAIN Output

```
PLAN: POINT_LOOKUP via bloom + clustered index
  target: a93b5600-5ac4-448f-9199-a9127f10378a
  bloom: positive (may exist)
  cost: O(1) — single pread

PLAN: INDEX_LOOKUP via hash-index "idx_name"
  column: name
  key: "neutron-router-abc"
  path: hash("neutron-router-abc") → index_node → entry → pread
  cost: O(1) — index lookup + single pread

PLAN: FULL_SCAN with condition filter
  table: Logical_Flow (1,680,000 rows)
  condition: [["priority",">=",100]]
  cost: O(N) — sequential cursor scan
  cache: false (streaming)
```

---

## Data Flow After Refactoring

### Point lookup: `ovn-sbctl list logical_flow <uuid>`

```
query_engine_plan(condition=[_uuid==uuid])
    → PLAN: POINT_LOOKUP uuid=a93b5600...

query_engine_execute(plan, storage, indexes, cache)
    ├── INDEX:   bloom.contains(uuid) → true (may exist)
    ├── CACHE:   row_cache_lookup(uuid) → miss
    ├── STORAGE: find_entry(uuid) → clustered entry
    │            read_row(entry) → pread at entry->offset
    ├── CACHE:   row_cache_insert(row)
    └── return single-row iterator
    (1 row, microseconds)
```

### Index lookup: `ovn-nbctl list Logical_Router neutron-abc`

```
query_engine_plan(condition=[name=="neutron-abc"])
    → PLAN: INDEX_LOOKUP index=idx_name, key="neutron-abc"

query_engine_execute(plan, storage, indexes, cache)
    ├── INDEX:   hash_idx.lookup("neutron-abc")
    │            → index_node → entry * (clustered)
    ├── CACHE:   row_cache_lookup(entry->uuid) → miss
    ├── STORAGE: read_row(entry) → pread at entry->offset
    ├── CACHE:   row_cache_insert(row)
    └── return single-row iterator
    (1 row, microseconds, zero UUID copy)
```

### Full scan streaming: `ovn-nbctl list Logical_Router`

```
query_engine_plan(condition=NULL)
    → PLAN: FULL_SCAN, use_cache=false

query_engine_stream(plan, storage, indexes, cache=NULL, pool, cb)
    ├── worker: cursor_open → cursor_next → serialize → batch
    ├── main: drain batches → send binary frames
    └── INITIAL_END when done
    (worker thread, no main thread blocking, no cache)
```

### Conditioned scan: `ovn-sbctl list logical_flow priority>=100`

```
query_engine_plan(condition=[priority>=100])
    → PLAN: FULL_SCAN with filter, use_cache=true

query_engine_execute(plan, storage, indexes, cache)
    ├── STORAGE: cursor_open("Logical_Flow")
    ├── for each row from cursor_next:
    │   ├── evaluate condition → match?
    │   ├── if match: cache_insert + yield to iterator
    │   └── if no match: destroy row
    └── cursor_close
```

---

## Phased Implementation (Detailed)

### Phase 1: Storage Engine

**Goal:** Facade over `disk-store.c`. No callers changed.

**New files:**
- `ovsdb/storage-engine.h` — interface (as above)
- `ovsdb/storage-engine.c` — wraps `ovsdb_disk_store_*` functions

**Implementation details:**
```c
struct ovsdb_storage_engine {
    struct ovsdb_disk_store *ds;  /* Underlying disk store */
};

struct ovsdb_row *
ovsdb_storage_engine_read_row(struct ovsdb_storage_engine *se,
                               struct ovsdb_table *table,
                               const struct disk_store_index_entry *entry)
{
    return ovsdb_disk_store_read_row(se->ds, table, &entry->uuid);
}

struct ovsdb_storage_cursor *
ovsdb_storage_engine_cursor_open(struct ovsdb_storage_engine *se,
                                  const char *table_name)
{
    /* Wraps ovsdb_disk_store_cursor_open — returns opaque handle */
    return (struct ovsdb_storage_cursor *)
        ovsdb_disk_store_cursor_open(se->ds, table_name);
}
```

**Tests (`tests/test-storage-engine.c`):**
- T1: `create` + `destroy` lifecycle
- T2: `read_row_by_uuid` on known UUID → returns correct row
- T3: `read_row_by_uuid` on non-existent UUID → returns NULL
- T4: `cursor_open` + `cursor_next` iterates all table rows
- T5: `cursor_open` on empty table → `cursor_next` returns NULL immediately
- T6: `count` returns correct number of rows per table
- T7: `find_entry` returns correct offset for known UUID
- T8: `write_row` + `read_row` round-trip

**Modify:** `ovsdb/automake.mk` — add new source files

---

### Phase 2: Index Engine

**Goal:** Generic index interface. BLOOM wraps existing bloom filter.
HASH replaces `ovsdb_name_index` with generic column→entry support.

**New files:**
- `ovsdb/index-engine.h` — interface (as above)
- `ovsdb/index-engine.c` — BLOOM + HASH implementations

**Implementation details:**

```c
struct ovsdb_index {
    enum ovsdb_index_type type;
    char *name;
    size_t n_columns;
    char **column_names;

    union {
        /* BLOOM: */
        struct ovsdb_bloom_filter *bloom;

        /* HASH: */
        struct {
            struct hmap entries;     /* ovsdb_index_node hmap */
            size_t count;
        } hash;
    };
};

struct disk_store_index_entry *
ovsdb_index_lookup(const struct ovsdb_index *idx,
                    const union ovsdb_atom *key)
{
    if (idx->type != OVSDB_IDX_HASH) {
        return NULL;
    }
    uint32_t h = ovsdb_atom_hash(key, idx->key_type, 0);
    struct ovsdb_index_node *node;
    HMAP_FOR_EACH_WITH_HASH (node, hmap_node, h, &idx->hash.entries) {
        if (ovsdb_atom_equals(&node->key, key, idx->key_type)) {
            return node->entry;
        }
    }
    return NULL;
}
```

**`ovsdb_index_set_from_schema` logic:**
```c
for each table in schema:
    1. Always create BLOOM index (for UUID existence)
    2. For each entry in schema->indexes:
       if single-column && type is string/integer/uuid:
           create HASH index for that column
       if multi-column:
           skip for now (future: compound HASH)
```

**Tests (`tests/test-index-engine.c`):**
- T1: BLOOM create + add UUIDs + `contains` → true
- T2: BLOOM `contains` for non-existent UUID → false
- T3: HASH create for string column + add entries
- T4: HASH `lookup` existing key → correct entry pointer
- T5: HASH `lookup` non-existent key → NULL
- T6: HASH `remove` + `lookup` → NULL (entry removed)
- T7: HASH with integer key type (not just string)
- T8: HASH with UUID key type (ref columns)
- T9: Multiple HASH indexes on same table (no interference)
- T10: `index_set_from_schema` auto-detects correct indexes from OVN NB schema
- T11: `index_set_find_for_column("name")` → finds correct HASH index
- T12: `index_set_find_bloom` → finds BLOOM index
- T13: Index `add` + `lookup` returns same `disk_store_index_entry *` (pointer identity)
- T14: 10K entries stress test — add, lookup, remove cycle

**Modify:**
- `ovsdb/automake.mk` — add new source files
- `ovsdb/ovsdb.c:ovsdb_attach_disk_store` — replace hardcoded bloom +
  name_index creation with `ovsdb_index_set_from_schema`

---

### Phase 3: Query Engine

**Goal:** Plan + execute with EXPLAIN. Orchestrates storage + index + cache.

**New files:**
- `ovsdb/query-engine.h` — interface (as above)
- `ovsdb/query-engine.c` — planner + executor

**Implementation details:**

```c
struct ovsdb_execution_plan *
ovsdb_query_engine_plan(const struct ovsdb_condition *cond,
                         const struct ovsdb_column_set *cols,
                         const struct ovsdb_index_set *idxs,
                         const struct ovsdb_table *table)
{
    struct ovsdb_execution_plan *plan = xzalloc(sizeof *plan);

    if (!cond || ovsdb_condition_is_true(cond)) {
        plan->type = OVSDB_PLAN_FULL_SCAN;
        plan->use_cache = false;
        plan->description = "FULL_SCAN (unconditioned)";
        return plan;
    }

    /* Check for UUID exact match. */
    if (cond->n_clauses > 0
        && cond->clauses[0].column->index == OVSDB_COL_UUID
        && cond->clauses[0].function == OVSDB_F_EQ) {
        plan->type = OVSDB_PLAN_POINT_LOOKUP;
        plan->target_uuid = cond->clauses[0].arg.keys[0].uuid;
        plan->description = "POINT_LOOKUP via bloom + clustered";
        return plan;
    }

    /* Check for indexed column match. */
    for (size_t i = 0; i < cond->n_clauses; i++) {
        if (cond->clauses[i].function != OVSDB_F_EQ) {
            continue;
        }
        const struct ovsdb_index *idx =
            ovsdb_index_set_find_for_column(
                idxs, cond->clauses[i].column->name);
        if (idx) {
            plan->type = OVSDB_PLAN_INDEX_LOOKUP;
            plan->index = idx;
            ovsdb_atom_clone(&plan->key,
                              &cond->clauses[i].arg.keys[0],
                              cond->clauses[i].column->type.key.type);
            plan->key_type = cond->clauses[i].column->type.key.type;
            plan->description = "INDEX_LOOKUP via hash index";
            return plan;
        }
    }

    plan->type = OVSDB_PLAN_FULL_SCAN;
    plan->condition = cond;
    plan->use_cache = true;
    plan->description = "FULL_SCAN with condition filter";
    return plan;
}
```

**Executor for POINT_LOOKUP:**
```c
/* Check bloom → cache → storage → populate cache */
bloom = ovsdb_index_set_find_bloom(indexes);
if (bloom && !ovsdb_index_contains(bloom, &plan->target_uuid)) {
    return empty_result;  /* Bloom negative — doesn't exist */
}
if (cache) {
    row = ovsdb_row_cache_lookup(cache, &plan->target_uuid);
    if (row) return single_row_result(row);
}
entry = ovsdb_storage_engine_find_entry(storage, &plan->target_uuid);
if (!entry) return empty_result;
row = ovsdb_storage_engine_read_row(storage, table, entry);
if (row && cache) {
    ovsdb_row_cache_insert(cache, row, ovsdb_row_count_atoms(row));
}
return single_row_result(row);
```

**Executor for INDEX_LOOKUP:**
```c
entry = ovsdb_index_lookup(plan->index, &plan->key);
if (!entry) return empty_result;
if (cache) {
    row = ovsdb_row_cache_lookup(cache, &entry->uuid);
    if (row) return single_row_result(row);
}
row = ovsdb_storage_engine_read_row(storage, table, entry);
if (row && cache) {
    ovsdb_row_cache_insert(cache, row, ovsdb_row_count_atoms(row));
}
return single_row_result(row);
```

**Executor for FULL_SCAN:**
```c
/* Returns a cursor-based iterator. */
cursor = ovsdb_storage_engine_cursor_open(storage, table_name);
/* Iterator internally: cursor_next → condition check → yield/skip */
```

**Streaming (reuses existing binary_stream infrastructure):**
```c
/* Submits worker jobs via ovsdb_worker_pool_submit.
 * Worker calls cursor_open/next, serializes to binary,
 * flushes batches via mutex+seq. */
```

**Tests (`tests/test-query-engine.c`):**
- T1: Plan `NULL` condition → `FULL_SCAN`
- T2: Plan `_uuid == <uuid>` → `POINT_LOOKUP`
- T3: Plan `name == "value"` with hash index → `INDEX_LOOKUP`
- T4: Plan `name == "value"` without hash index → `FULL_SCAN` with filter
- T5: Plan `priority >= 100` (no EQ) → `FULL_SCAN` with filter
- T6: Execute POINT_LOOKUP on existing UUID → returns row
- T7: Execute POINT_LOOKUP on non-existent UUID, bloom negative → empty
- T8: Execute POINT_LOOKUP, cache hit → returns cached row (no pread)
- T9: Execute INDEX_LOOKUP → returns row via index → entry → pread
- T10: Execute FULL_SCAN → returns all rows
- T11: Execute FULL_SCAN with condition → returns only matching rows
- T12: Execute with `cache=NULL` → no cache interaction
- T13: Execute POINT_LOOKUP populates cache → second call is cache hit
- T14: `describe()` produces human-readable EXPLAIN string
- T15: Stream FULL_SCAN → batches arrive via callback
- T16: Stream cancel → worker aborts early

**Tests (`tests/ovsdb-query-engine.at`):**
- T17: `ovn-sbctl list logical_flow <uuid>` → EXPLAIN shows POINT_LOOKUP
- T18: `ovn-nbctl list Logical_Router <name>` → EXPLAIN shows INDEX_LOOKUP
- T19: `ovn-nbctl list Logical_Router` → EXPLAIN shows FULL_SCAN
- T20: 10K row table: POINT_LOOKUP < 1ms, FULL_SCAN via streaming

**Modify:** `ovsdb/automake.mk`, `tests/automake.mk`, `tests/testsuite.at`

---

### Phase 4: Migrate Callers

**Goal:** Replace direct disk/cache/index calls with engine calls,
one file at a time.

#### 4a. `ovsdb/table.c`

**Current (fragmented):**
```c
/* ovsdb_table_get_row — 3 paths manually coded */
HMAP_FOR_EACH_WITH_HASH(...)      /* table->rows */
ovsdb_row_cache_lookup(...)        /* cache */
ovsdb_bloom_filter_may_contain()   /* bloom */
ovsdb_disk_store_read_row(...)     /* disk */
ovsdb_row_cache_insert(...)        /* cache insert */

/* ovsdb_table_query — 3-path switch */
Path 1: UUID exact match
Path 2: Name index lookup
Path 3: Full cursor scan
```

**After:**
```c
const struct ovsdb_row *
ovsdb_table_get_row(const struct ovsdb_table *table,
                     const struct uuid *uuid)
{
    /* In-memory rows still checked first (txn modifications). */
    struct ovsdb_row *row;
    HMAP_FOR_EACH_WITH_HASH(row, hmap_node, uuid_hash(uuid),
                             &table->rows) {
        if (uuid_equals(ovsdb_row_get_uuid(row), uuid)) {
            return row;
        }
    }

    if (!table->storage_engine) {
        return NULL;
    }

    /* Query engine handles: bloom → cache → disk → cache insert */
    struct ovsdb_condition cond;
    /* Build UUID condition... */
    struct ovsdb_execution_plan *plan = ovsdb_query_engine_plan(
        &cond, NULL, table->index_set, table);
    struct ovsdb_query_result *result = ovsdb_query_engine_execute(
        plan, table->storage_engine, table->index_set, table->cache);
    row = ovsdb_query_result_next(result);
    ovsdb_query_result_close(result);
    ovsdb_execution_plan_destroy(plan);
    return row;
}

void
ovsdb_table_query(struct ovsdb_table *table,
                   const struct ovsdb_condition *condition,
                   ovsdb_table_row_cb cb, void *aux)
{
    /* In-memory rows first... */
    /* Then: */
    struct ovsdb_execution_plan *plan = ovsdb_query_engine_plan(
        condition, NULL, table->index_set, table);
    struct ovsdb_query_result *result = ovsdb_query_engine_execute(
        plan, table->storage_engine, table->index_set, table->cache);
    const struct ovsdb_row *row;
    while ((row = ovsdb_query_result_next(result))) {
        if (!cb(row, aux)) break;
    }
    ovsdb_query_result_close(result);
    ovsdb_execution_plan_destroy(plan);
}
```

**New fields on `struct ovsdb_table`:**
```c
struct ovsdb_storage_engine *storage_engine;  /* Replaces disk_store ptr */
struct ovsdb_index_set *index_set;            /* Replaces bloom + name_index */
/* cache stays as-is */
```

**Tests:**
- All existing `ovsdb-query.at` tests must pass (regression)
- All existing `ovsdb-execution.at` tests must pass
- Add: `ovsdb-table.at` test for get_row via query engine

#### 4b. `ovsdb/ovsdb.c`

**Current:**
```c
ovsdb_attach_disk_store():
    table->disk_store = ds;
    table->cache = ovsdb_row_cache_create(max_atoms);
    table->bloom = ovsdb_bloom_filter_create(n_rows);
    for_each_uuid(ds, add_bloom_cb, bloom);
    table->name_index = ovsdb_name_index_create(...);
    ovsdb_disk_store_build_name_index(ds, name_index);
```

**After:**
```c
ovsdb_attach_disk_store():
    table->storage_engine = ovsdb_storage_engine_create(ds);
    table->cache = ovsdb_row_cache_create(max_atoms);
    table->index_set = ovsdb_index_set_from_schema(
        table->schema, table->storage_engine);
    ovsdb_row_cache_start_sweeper(table->cache);
```

**Tests:** Startup test — verify indexes built correctly for NB + SB schemas

#### 4c. `ovsdb/jsonrpc-server.c`

**Current:**
```c
disk_cursor_stream_worker_fn():
    cursor = ovsdb_disk_store_cursor_open(job->table->disk_store, ...);
    while (row = ovsdb_disk_store_cursor_next(cursor, ...)) { ... }
```

**After:**
```c
stream_worker_fn():
    plan = ovsdb_query_engine_plan(NULL, &job->columns, ...);
    /* plan->type == FULL_SCAN */
    cursor = ovsdb_storage_engine_cursor_open(job->storage, ...);
    while (row = ovsdb_storage_engine_cursor_next(cursor, ...)) { ... }
```

Or use `ovsdb_query_engine_stream()` which encapsulates the worker
submission, batch production, and cancellation.

**Tests:** Existing binary streaming tests must pass

#### 4d. `ovsdb/monitor.c`

**Current:**
```c
ovsdb_monitor_get_initial():
    ovsdb_table_for_each_loaded_row(table, monitor_initial_row_cb, ...);
```

**After:**
```c
ovsdb_monitor_get_initial():
    plan = ovsdb_query_engine_plan(NULL, columns, table->index_set, table);
    result = ovsdb_query_engine_execute(plan, table->storage_engine,
                                         table->index_set, table->cache);
    while (row = ovsdb_query_result_next(result)) {
        monitor_initial_row_cb(row, ...);
    }
```

**Tests:** Monitor initial snapshot tests must match current output

#### 4e. `ovsdb/lazy-load.c`

**Current:**
```c
row_load_worker():
    row = ovsdb_disk_store_read_row(table->disk_store, table, &uuid);
```

**After:**
```c
row_load_worker():
    row = ovsdb_storage_engine_read_row_by_uuid(
        table->storage_engine, table, &uuid);
```

**Tests:** Lazy-load + trigger parking tests must pass

---

### Phase 5: Index Configuration

**Goal:** Auto-detect from schema (default). Optional override.

**`ovsdb_index_set_from_schema` auto-detection rules:**
1. Always: BLOOM index for UUID existence
2. For each `"indexes": [["col"]]` in schema:
   - Single string column with `n_max==1` → HASH index
   - Single integer column → HASH index
   - Single UUID column → HASH index
   - Multi-column → skip (future compound index)
3. Schema examples:
   - `Logical_Router indexes:[["name"]]` → BLOOM + HASH("name")
   - `Chassis indexes:[["name"]]` → BLOOM + HASH("name")
   - `Port_Binding indexes:[["logical_port"]]` → BLOOM + HASH("logical_port")
   - `Logical_Flow` (no indexes in schema) → BLOOM only
   - `Encap indexes:[["type","ip"]]` → BLOOM only (multi-col skipped)

**Optional `--index-config` (future):**
```
# Override or add indexes beyond schema defaults
[Port_Binding]
hash = logical_port
hash = datapath      # Additional index

[Logical_Flow]
hash = logical_datapath   # Add index not in schema
```

**Tests:**
- T1: Auto-detect from OVN NB schema → correct indexes per table
- T2: Auto-detect from OVN SB schema → correct indexes per table
- T3: Table with no schema indexes → BLOOM only
- T4: Table with multi-column index → skipped (BLOOM only)
- T5: Config file override adds extra HASH index

---

## Files Summary

### New Files

| File | Layer | Lines (est.) |
|------|-------|-------------|
| `ovsdb/storage-engine.h` | Storage | ~60 |
| `ovsdb/storage-engine.c` | Storage | ~150 |
| `ovsdb/index-engine.h` | Index | ~80 |
| `ovsdb/index-engine.c` | Index | ~350 |
| `ovsdb/query-engine.h` | Query | ~70 |
| `ovsdb/query-engine.c` | Query | ~400 |
| `tests/test-storage-engine.c` | Test | ~200 |
| `tests/test-index-engine.c` | Test | ~400 |
| `tests/test-query-engine.c` | Test | ~500 |
| `tests/ovsdb-query-engine.at` | Test | ~100 |

### Modified Files

| File | Phase | Changes |
|------|-------|---------|
| `ovsdb/automake.mk` | 1-3 | Add 6 new source files |
| `tests/automake.mk` | 1-3 | Add 3 test programs + 1 .at file |
| `tests/testsuite.at` | 3 | Include `ovsdb-query-engine.at` |
| `ovsdb/table.h` | 4a | Add `storage_engine`, `index_set` to struct |
| `ovsdb/table.c` | 4a | Replace `get_row` + `query` internals |
| `ovsdb/ovsdb.c` | 4b | Replace index creation in `attach_disk_store` |
| `ovsdb/jsonrpc-server.c` | 4c | Use `storage_engine` for streaming |
| `ovsdb/monitor.c` | 4d | Use `query_engine` for initial |
| `ovsdb/lazy-load.c` | 4e | Use `storage_engine` for row read |
| `ovsdb/disk-store.h` | 2 | Make `disk_store_index_entry` public |

### Removed/Deprecated After Migration

| Item | Replaced by |
|------|-------------|
| `table->bloom` | `index_set` BLOOM entry |
| `table->name_index` | `index_set` HASH entry |
| `table->disk_store` (direct) | `table->storage_engine` |
| `ovsdb_name_index_*` functions | `ovsdb_index_*` generic |
| `ovsdb_bloom_filter_*` (direct) | `ovsdb_index_contains` |
| Path 1/2/3 in `ovsdb_table_query` | `ovsdb_query_engine_plan` |

---

## Verification Checklist

### Per-Phase

- [ ] Phase 1: `test-storage-engine` passes (8 tests)
- [ ] Phase 1: `make -j4` zero errors
- [ ] Phase 2: `test-index-engine` passes (14 tests)
- [ ] Phase 2: Startup builds same indexes as before (compare logs)
- [ ] Phase 3: `test-query-engine` passes (16 unit + 4 integration)
- [ ] Phase 3: EXPLAIN output logged for all query types
- [ ] Phase 4: `make check TESTSUITEFLAGS="-j4 -k ovsdb"` — zero regressions
- [ ] Phase 4: Stress tests pass (`tests/stress/run-all.sh`)
- [ ] Phase 4: Binary streaming works for unconditioned monitors
- [ ] Phase 4: UUID/name lookups use index path (check EXPLAIN logs)

### End-to-End

- [ ] `ovn-sbctl list logical_flow <uuid>` → EXPLAIN: POINT_LOOKUP, < 1ms
- [ ] `ovn-nbctl list Logical_Router <name>` → EXPLAIN: INDEX_LOOKUP, < 1ms
- [ ] `ovn-nbctl list Logical_Router` → EXPLAIN: FULL_SCAN, binary streaming
- [ ] northd startup → binary streaming for all tables
- [ ] Concurrent readers + writers → stress test passes
- [ ] Session disconnect during streaming → no crash
