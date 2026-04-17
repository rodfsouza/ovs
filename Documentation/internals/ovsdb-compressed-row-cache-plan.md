# Option D: Compressed Row Cache — Store Raw Binary Bytes Instead of Deserialized Rows

## Context

The OVSDB disk-store row cache (`ovsdb/row-cache.c`) stores fully deserialized
`struct ovsdb_row` objects.  Each cached row consists of:

```
struct ovsdb_row            ~80 bytes (header, table backpointer, hmap nodes)
  + N × struct ovsdb_datum  ~24 bytes each (n, keys, values pointers)
    + keys[]/values[]       ~16 bytes per atom (union + padding)
      + heap allocs         variable (strings: strlen+1, uuids: 16B inline)
```

For a typical OVN `Logical_Switch` row with 10 columns and ~5 atoms per column,
the in-memory cost is roughly **500–2000 bytes**.  For 100K rows that's
**50–200 MB** of resident memory just in the cache.

The on-disk binary representation of the same row (as written by
`disk_store_serialize_row()` in `disk-store.c`) is much more compact:

```
[table_name_len:2B][table_name:NB]
[type_tag:1B][datum_binary...]  × N columns
```

A typical row's binary body is **50–200 bytes** — roughly **3–5x smaller** than
the deserialized form.  The difference comes from:
- No pointer overhead (keys/values arrays are inline)
- No struct padding
- No per-string heap allocation headers
- No hmap_node / list_node bookkeeping

## Design: Two-Tier Cache Entry

### Current cache entry (`row-cache.c:32-41`)

```c
struct ovsdb_row_cache_entry {
    struct hmap_node hmap_node;
    struct ovs_list lru_node;
    struct uuid uuid;
    struct ovsdb_row *row;      /* Fully deserialized. */
    size_t n_atoms;
    bool pinned;
    enum ovsdb_row_state state;
};
```

### Proposed cache entry

```c
struct ovsdb_row_cache_entry {
    struct hmap_node hmap_node;
    struct ovs_list lru_node;
    struct uuid uuid;

    /* Raw binary representation from disk (compact, always present
     * when state == OVSDB_ROW_CACHED).  Owned by this entry. */
    uint8_t *raw_data;          /* Record body (after 24B row header). */
    size_t raw_len;             /* Length of raw_data. */

    /* Materialized row (deserialized on demand, optional).
     * NULL until first access.  Freed on eviction or when memory
     * pressure triggers a dematerialize pass. */
    struct ovsdb_row *row;

    size_t n_atoms;             /* Atom count (for budget tracking). */
    size_t raw_cost;            /* Memory cost of raw_data (= raw_len). */
    bool pinned;
    enum ovsdb_row_state state;
};
```

### State transitions

```
UNLOADED                    Only uuid known. raw_data=NULL, row=NULL.
    │
    ├─ async load (worker)  Worker reads raw bytes from disk.
    │  or sync read         On completion: raw_data set, state→CACHED.
    ▼
CACHED (compressed)         raw_data populated, row=NULL.
    │                       Memory cost: sizeof(entry) + raw_len.
    │
    ├─ access (materialize) Caller needs struct ovsdb_row *.
    │                       disk_store_deserialize_row(raw_data, ...)
    │                       row set, n_atoms computed.
    ▼
CACHED (materialized)       raw_data AND row both populated.
    │                       Memory cost: sizeof(entry) + raw_len + row cost.
    │
    ├─ memory pressure      Dematerialize: destroy row, keep raw_data.
    │  (dematerialize)      Avoids disk I/O on next access.
    ▼
CACHED (compressed)         Back to raw_data only.
```

### Memory budget accounting

Two separate budgets, both soft limits with LRU eviction:

| Budget | Tracks | Default | Purpose |
|--------|--------|---------|---------|
| `max_raw_bytes` | `sum(entry.raw_len)` | 64 MB | Caps total raw-byte memory |
| `max_materialized_atoms` | `sum(entry.n_atoms)` where `row != NULL` | 100K atoms (same as today's `max_atoms`) | Caps deserialized-row memory |

When `max_raw_bytes` is exceeded, evict the LRU CACHED entry entirely (free
both `raw_data` and `row`, transition to UNLOADED → requires disk re-read).

When `max_materialized_atoms` is exceeded, **dematerialize** the LRU
materialized entry (free `row`, keep `raw_data`).  This is cheaper than
eviction because re-materialization doesn't require disk I/O.

### Why two budgets?

- `max_raw_bytes` bounds total memory consumption.  Even compressed, 10M rows
  × 100 bytes = 1 GB.  Must be capped.
- `max_materialized_atoms` bounds the "hot" working set.  Materialized rows are
  3–5x larger than raw.  Keeping them all materialized defeats the purpose.

For most deployments, `max_raw_bytes` is large enough to hold the entire
database (the whole point is avoiding disk re-reads), and
`max_materialized_atoms` keeps only the actively-queried rows deserialized.

## Implementation

### Phase 1: Compressed cache entries (no materialization caching)

Simplest first step.  Cache stores raw bytes only.  Every access deserializes.

**Changes:**

1. **`ovsdb/row-cache.c` + `.h`**: Change `ovsdb_row_cache_insert()` to accept
   `uint8_t *raw_data, size_t raw_len` instead of `struct ovsdb_row *`.  The
   entry stores a copy of the raw bytes (via `xmemdup`).  `entry->row` is
   always NULL.  Budget tracking uses `raw_len` instead of `n_atoms`.

2. **`ovsdb/row-cache.c`**: New accessor `ovsdb_row_cache_get_raw()` that
   returns `{raw_data, raw_len}` for a UUID.  Caller deserializes.

3. **`ovsdb/lazy-load.c`**: `row_load_done()` callback receives raw bytes from
   worker (worker reads via `pread` but does NOT deserialize).  Inserts raw
   into cache.

4. **`ovsdb/table.c`**: `ovsdb_table_get_row()` and
   `ovsdb_table_for_each_row_from_disk()` call
   `disk_store_deserialize_row(raw_data, raw_len, table, uuid)` after cache
   lookup.  The deserialized row is returned to the caller and destroyed after
   use (transient).

5. **`ovsdb/disk-store.c`**: New function `ovsdb_disk_store_read_raw()` that
   returns raw bytes (record body, without the 24B header) without
   deserializing.  Worker uses this instead of `ovsdb_disk_store_read_row()`.

**Memory impact:**

| Scenario | Before (deserialized) | After (raw bytes) | Savings |
|----------|-----------------------|--------------------|---------|
| 10K rows, 10 cols, 5 atoms/col | ~10 MB | ~2 MB | 5x |
| 100K rows | ~100 MB | ~20 MB | 5x |
| 1M rows | ~1 GB | ~200 MB | 5x |

**CPU impact:**

Every row access deserializes from binary.  Cost: ~1–5 μs per row (memory-copy
+ datum parsing).  For a monitor initial snapshot with 10K rows: ~10–50 ms total
deserialization overhead.  Acceptable — JSON serialization for the same snapshot
is 100–500 ms.

### Phase 2: Materialization caching (optional hot-row optimization)

Add `entry->row` as a secondary cache of the deserialized form.  Populated on
first access, freed under `max_materialized_atoms` pressure.

**Changes:**

1. **`ovsdb/row-cache.c`**: `ovsdb_row_cache_materialize()` — deserializes
   `raw_data` into `row`, sets `n_atoms`, tracks against
   `max_materialized_atoms`.  If over budget, dematerializes LRU entries.

2. **`ovsdb/row-cache.c`**: `ovsdb_row_cache_lookup()` returns `entry->row` if
   materialized, otherwise returns NULL (caller must deserialize from raw or
   call `materialize()`).

3. **`ovsdb/table.c`**: `ovsdb_table_get_row()` calls `materialize()` on cache
   hit to get a stable pointer.  The materialized row stays alive until LRU
   eviction or dematerialization.

**When to dematerialize:**

- After a monitor snapshot is sent (rows no longer needed hot)
- Under memory pressure (new materializations push old ones out)
- Via a periodic sweep (e.g., every N main-loop ticks)

### Phase 3: Column-level access (future, requires format change)

See Option A in the serve-paths fix plan.  With a per-column offset table in
the binary format, `ovsdb_row_cache_get_column()` could deserialize a single
column from `raw_data` without parsing the entire row.  Useful for secondary
index lookups that only need the indexed column.

## Files to modify

### Phase 1

| File | Change |
|------|--------|
| `ovsdb/row-cache.h` | Change `ovsdb_row_cache_insert()` signature; add `ovsdb_row_cache_get_raw()`, `ovsdb_row_cache_max_raw_bytes()` |
| `ovsdb/row-cache.c` | Store `raw_data` + `raw_len` instead of `row`; budget on bytes not atoms; add `get_raw()` |
| `ovsdb/disk-store.h` | Declare `ovsdb_disk_store_read_raw()` |
| `ovsdb/disk-store.c` | Implement `read_raw()` (pread + return body bytes without deserialize) |
| `ovsdb/lazy-load.c` | Worker calls `read_raw()` instead of `read_row()`; done callback inserts raw into cache |
| `ovsdb/table.c` | `get_row()` and `for_each_row_from_disk()` deserialize from cache raw on hit |
| `ovsdb/ovsdb-server.c` | Add `--cache-max-bytes` CLI flag alongside `--cache-max-atoms` |
| `ovsdb/ovsdb.c` | Pass `cache_max_bytes` to `ovsdb_row_cache_create()` |

### Phase 2 (additional)

| File | Change |
|------|--------|
| `ovsdb/row-cache.h` | Add `ovsdb_row_cache_materialize()`, `max_materialized_atoms` |
| `ovsdb/row-cache.c` | Implement materialize/dematerialize with LRU tracking |
| `ovsdb/table.c` | `get_row()` calls materialize for stable-pointer callers |

## API changes

### Phase 1 — row-cache.h

```c
/* Insert raw binary data for a row.  Takes ownership of 'raw_data'
 * (caller must not free).  'raw_len' is the body length (excluding
 * the 24-byte disk-store row header).  Sets state to OVSDB_ROW_CACHED. */
void ovsdb_row_cache_insert_raw(struct ovsdb_row_cache *,
                                const struct uuid *uuid,
                                uint8_t *raw_data, size_t raw_len);

/* Retrieve cached raw bytes for 'uuid'.  Returns true and sets
 * '*datap' and '*lenp' on hit (data owned by cache, valid until
 * eviction).  Returns false on miss. */
bool ovsdb_row_cache_get_raw(struct ovsdb_row_cache *,
                             const struct uuid *uuid,
                             const uint8_t **datap, size_t *lenp);

/* Returns the soft byte budget for raw data.  Entries over this
 * budget are eligible for LRU eviction. */
size_t ovsdb_row_cache_max_raw_bytes(const struct ovsdb_row_cache *);
```

### Phase 1 — disk-store.h

```c
/* Reads the raw binary body (everything after the 24-byte row header)
 * for 'uuid' from the disk store.  Returns the data and its length
 * via '*datap' and '*lenp'.  Caller owns the returned buffer.
 * Returns true on success, false on error or if the row is deleted. */
bool ovsdb_disk_store_read_raw(struct ovsdb_disk_store *,
                               const struct uuid *uuid,
                               uint8_t **datap, size_t *lenp);
```

## Tests

### T1: Compressed cache — basic round-trip (`tests/test-row-cache.c`)

- Insert raw bytes for 3 UUIDs via `ovsdb_row_cache_insert_raw()`.
- Retrieve each via `ovsdb_row_cache_get_raw()`.
- Verify bytes match exactly.
- Verify `ovsdb_row_cache_count()` == 3.

### T2: Compressed cache — LRU eviction on byte budget

- Create cache with `max_raw_bytes = 100`.
- Insert 5 rows of 30 bytes each (total 150 > 100).
- Verify count < 5 (eviction happened).
- Verify most recently inserted entries are still present.
- Verify evicted entries return false from `get_raw()`.

### T3: Compressed cache — deserialize on access

- Create a disk store with known rows.
- Insert raw bytes into cache.
- Call `disk_store_deserialize_row(raw_data, raw_len, table, uuid)`.
- Verify deserialized row has correct column values.
- Destroy the deserialized row (transient).

### T4: Autotest — monitor with compressed cache

- Same setup as T4 from the serve-paths fix (insert rows, convert binary,
  restart, monitor).
- With compressed cache enabled, verify monitor initial snapshot is complete.
- This validates the full end-to-end path: disk → raw cache → deserialize →
  monitor JSON.

### T5: Autotest — memory comparison

- Start server on a 1000-row binary DB.
- Query `ovs-appctl memory/show` or read `/proc/PID/status VmRSS`.
- Compare memory with deserialized cache (current) vs compressed cache (new).
- Assert compressed uses measurably less (e.g., <50% of deserialized).

### T6: Phase 2 — materialize/dematerialize cycle

- Insert raw bytes for 10 rows.
- Materialize 5 of them.
- Verify `row != NULL` for materialized entries.
- Trigger dematerialization (exceed `max_materialized_atoms`).
- Verify `row == NULL` for dematerialized entries.
- Verify `raw_data` still present (no disk re-read needed).
- Re-materialize: verify correct data.

### T7: Phase 2 — get_row returns materialized pointer

- Insert raw bytes, materialize.
- Call `ovsdb_table_get_row()` — must return the materialized pointer
  without re-deserializing (stable, cache-owned).
- Verify pointer is the same on repeated calls (no double-materialize).

## Performance expectations

| Operation | Before (deserialized cache) | After Phase 1 (raw cache) | After Phase 2 (materialized) |
|-----------|-----------------------------|---------------------------|------------------------------|
| Cache insert (from worker) | Deserialize + insert ~10 μs | memcpy raw bytes ~1 μs | Same as Phase 1 |
| Cache lookup (hot) | Return pointer ~0.1 μs | Deserialize from raw ~3 μs | Return pointer ~0.1 μs |
| Memory per row | ~500–2000 B | ~50–200 B | ~50–200 B (raw) + ~500–2000 B (materialized subset) |
| Monitor 10K rows | 10K × 0.1 μs = 1 ms | 10K × 3 μs = 30 ms | 1 ms (if materialized) |
| LRU eviction cost | free(row) ~1 μs | free(raw_data) ~0.1 μs | free(row) + keep raw ~1 μs |

Phase 1 adds ~30 ms to a 10K-row monitor snapshot (from deserialization on
access).  This is small compared to the JSON serialization cost (~200 ms for
10K rows).  Phase 2 eliminates this overhead for frequently-accessed rows by
keeping them materialized.

## Risks

- **Phase 1 CPU regression**: Every cache hit now deserializes.  For small DBs
  that fit entirely in the current atom-based cache, this is a pure regression
  (more CPU, same memory).  Mitigated by Phase 2 materialization.

- **Raw bytes become stale**: If the binary format changes (schema evolution),
  cached raw bytes may not deserialize correctly.  Mitigation: invalidate the
  entire cache on schema change (same as today).

- **Two-budget complexity**: Operators now have two knobs (`--cache-max-bytes`
  and `--cache-max-atoms`).  Mitigated by sensible defaults and clear
  documentation.  Phase 1 only needs `--cache-max-bytes`; Phase 2 adds
  `--cache-max-atoms` for the materialized subset.

- **Thread safety of raw_data**: Worker threads write `raw_data` during load;
  main thread reads during access.  Same handoff pattern as today's `row`
  pointer — no new concurrency concern.

## Implementation order

1. `ovsdb_disk_store_read_raw()` — standalone, testable via `test-disk-store`
2. `ovsdb_row_cache_insert_raw()` + `get_raw()` — standalone, testable via `test-row-cache`
3. Worker path change (`lazy-load.c`) — swap `read_row` to `read_raw` + `insert_raw`
4. Consumer path change (`table.c`) — deserialize from raw on access
5. CLI flag `--cache-max-bytes` — alongside existing `--cache-max-atoms`
6. Phase 2 materialization — only after Phase 1 is validated in production
