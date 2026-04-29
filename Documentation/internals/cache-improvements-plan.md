# Plan: Cache Improvements — Usage Tracking, Background Eviction, Concurrent Reads, Async Full Loads

Broken into 5 independent phases, each implementable and testable on its own.

---

## Phase 1: Per-Entry Usage Tracking (LRU → LRU+LFU Hybrid)

### Problem
Current cache has no per-entry access tracking. Only cache-wide `hits`/`misses` counters. Pure LRU evicts least-recently-used, but a row accessed 1,000 times is evicted as readily as one accessed once if they share the same last-access time.

### Changes

**`ovsdb/row-cache.c`** — Extend `ovsdb_row_cache_entry`:
```c
struct ovsdb_row_cache_entry {
    /* ... existing fields ... */
    uint32_t access_count;        /* Number of lookups since insertion. */
    long long int last_access_ms; /* time_msec() of last lookup. */
    long long int insert_time_ms; /* time_msec() when inserted. */
};
```

**`ovsdb/row-cache.c`** — Update `ovsdb_row_cache_touch__()`:
```c
static void
ovsdb_row_cache_touch__(struct ovsdb_row_cache *cache,
                        struct ovsdb_row_cache_entry *entry)
{
    ovs_list_remove(&entry->lru_node);
    ovs_list_push_back(&cache->lru, &entry->lru_node);
    entry->access_count++;
    entry->last_access_ms = time_msec();
}
```

**`ovsdb/row-cache.c`** — Update `ovsdb_row_cache_insert()`:
```c
entry->access_count = 0;
entry->last_access_ms = time_msec();
entry->insert_time_ms = time_msec();
```

**Eviction remains LRU for now** — Phase 2 uses this data for smarter eviction. This phase just adds the tracking.

### Tests
- Insert rows, access some multiple times, verify access_count increments
- Verify last_access_ms updates on lookup
- Regression: all existing tests pass

### Files
- `ovsdb/row-cache.c` — entry struct, touch, insert

---

## Phase 2: Background Cache Maintenance Thread

### Problem
Eviction happens synchronously on the main thread during `ovsdb_row_cache_insert()`. For large caches this blocks request processing. No aging — cold entries never get evicted unless the cache is full.

### Design

A dedicated persistent thread that wakes periodically, scans the cache, marks entries for eviction based on age/usage, and evicts on the next pass.

**Thread lifecycle:**
```
cache_maintenance_thread():
  loop:
    sleep(CACHE_SCAN_INTERVAL_MS)  // e.g., 30 seconds
    
    Pass 1: SCAN — identify cold entries
      for each entry in cache:
        age = now - entry->last_access_ms
        if age > COLD_THRESHOLD_MS && access_count < MIN_ACCESS_COUNT:
          mark entry->evict_candidate = true
    
    Pass 2: EVICT (next wake cycle)
      for each entry where evict_candidate == true:
        if still evict_candidate (not accessed since):
          evict entry
        else:
          clear evict_candidate
```

**Two-pass approach:** Mark on one cycle, evict on the next. If the entry is accessed between passes, the mark is cleared — it earned its place. This prevents evicting entries that are about to be accessed.

**Thread integration:**
```c
static struct ovs_mutex cache_maint_mutex = OVS_MUTEX_INITIALIZER;
static pthread_cond_t cache_maint_cond;
static bool cache_maint_enabled = false;
static struct latch cache_maint_latch;

static void *
cache_maintenance_thread(void *arg)
{
    struct ovsdb_row_cache *cache = arg;
    
    pthread_detach(pthread_self());
    
    for (;;) {
        /* Sleep with timeout (periodic scan). */
        long long int next_scan = time_msec() + CACHE_SCAN_INTERVAL_MS;
        do {
            poll_timer_wait_until(next_scan);
            latch_wait(&cache_maint_latch);  /* On-demand wake */
            poll_block();
        } while (time_msec() < next_scan
                 && !latch_poll(&cache_maint_latch));
        
        /* Scan cache under read lock (see Phase 3). */
        cache_scan_and_mark(cache);
        
        /* Evict marked entries under write lock. */
        cache_evict_marked(cache);
    }
}
```

**New entry field:**
```c
bool evict_candidate;  /* Marked by maintenance thread. */
```

**Clear on access** — in `ovsdb_row_cache_touch__()`:
```c
entry->evict_candidate = false;  /* Accessed — keep it. */
```

### Tests
- Start maintenance thread, verify periodic scanning
- Insert rows, let them age, verify cold entries evicted
- Access a row during mark-evict window, verify NOT evicted
- Verify main thread not blocked during eviction

### Files
- `ovsdb/row-cache.c` — entry struct (evict_candidate), maintenance thread
- `ovsdb/row-cache.h` — expose start/stop maintenance
- `ovsdb/ovsdb.c` — start maintenance thread at startup

---

## Phase 3: Read-Write Lock on Cache for Concurrent Reads

### Problem
All cache operations (lookup, insert, evict) are unprotected — main-thread-only assumption. With the maintenance thread (Phase 2) and future async full loads (Phase 5), we need concurrent read access.

### Design

Add an `ovs_rwlock` to the cache. Read path (lookups) takes rdlock, write path (insert/evict/remove) takes wrlock.

```c
struct ovsdb_row_cache {
    /* ... existing fields ... */
    struct ovs_rwlock rwlock;  /* Protects entries hmap and lru list. */
};
```

**Lock granularity:**

| Operation | Lock | Thread |
|-----------|------|--------|
| `lookup()` | rdlock | main + workers |
| `touch__()` | wrlock | main (promotes from rdlock) |
| `insert()` | wrlock | main |
| `remove()` | wrlock | main |
| `evict__()` | wrlock | main + maintenance |
| maintenance scan | rdlock | maintenance thread |
| maintenance evict | wrlock | maintenance thread |

**Problem with touch:** `lookup()` wants rdlock for the hmap find, but `touch__()` modifies the LRU list (needs wrlock). Options:

**Option A: Split lookup into find + touch**
```c
const struct ovsdb_row *
ovsdb_row_cache_lookup(struct ovsdb_row_cache *cache,
                       const struct uuid *uuid)
{
    ovs_rwlock_rdlock(&cache->rwlock);
    entry = find_entry(cache, uuid);  /* Read-only hmap lookup. */
    struct ovsdb_row *row = entry ? entry->row : NULL;
    ovs_rwlock_unlock(&cache->rwlock);
    
    if (entry) {
        /* Deferred touch: just update counters atomically.
         * LRU reorder deferred to next write operation. */
        atomic_add(&entry->access_count, 1);
        atomic_store(&entry->last_access_ms, time_msec());
        atomic_store(&entry->evict_candidate, false);
    }
    return row;
}
```

This avoids promoting rdlock to wrlock (which POSIX doesn't support). Access counts are updated atomically without lock promotion. LRU list reordering is approximated — the maintenance thread uses `last_access_ms` for eviction decisions, not LRU list position.

**Option B: Keep wrlock on every lookup (simpler, less concurrent)**

Option A is recommended — it allows truly concurrent reads.

### Tests
- Multiple threads reading cache concurrently: no crashes
- Write operations serialize correctly
- Maintenance thread scans without blocking readers
- Regression: all existing tests pass

### Files
- `ovsdb/row-cache.c` — add rwlock, update all operations
- `ovsdb/row-cache.h` — struct change

---

## Phase 4: Dynamic Cache Sizing

### Problem
Cache has a fixed `max_atoms` budget set at startup. For a full-load request (northd), the cache should temporarily grow to accommodate all rows, then shrink as entries age out. Currently the fixed budget causes thrashing.

### Design

**Dynamic budget with high-water and low-water marks:**

```c
struct ovsdb_row_cache {
    /* ... existing fields ... */
    size_t base_max_atoms;    /* Configured base budget. */
    size_t current_max_atoms; /* Dynamic limit (can grow). */
    size_t high_water_atoms;  /* Upper bound (e.g., 10x base). */
};
```

**Growth trigger:** When a full scan is detected (unconditioned `ovsdb_table_query`), temporarily raise `current_max_atoms` to `high_water_atoms`:

```c
/* In ovsdb_table_query, Path 3 (unconditioned scan): */
if (!check_cond && table->cache) {
    ovsdb_row_cache_enter_burst_mode(table->cache);
}
/* ... scan ... */
if (!check_cond && table->cache) {
    ovsdb_row_cache_exit_burst_mode(table->cache);
}
```

**Shrink trigger:** The maintenance thread (Phase 2) gradually reduces `current_max_atoms` toward `base_max_atoms` when the cache is idle (no burst mode active). Entries that age out naturally get evicted, and the budget contracts.

```c
/* In cache_maintenance_thread: */
if (!cache->burst_mode
    && cache->current_max_atoms > cache->base_max_atoms) {
    /* Shrink by 10% per cycle until base is reached. */
    cache->current_max_atoms = MAX(
        cache->base_max_atoms,
        cache->current_max_atoms * 9 / 10);
}
```

**Full scan behavior with dynamic sizing:**
1. Scan starts → burst mode → budget grows
2. Rows deserialized and cached (no thrashing)
3. Scan ends → burst mode off
4. Maintenance thread gradually shrinks budget
5. Cold entries evicted by age/usage (Phase 2)
6. Memory returns to base level

### Tests
- Full scan with dynamic sizing: verify budget grows
- After scan: verify budget shrinks over time
- Verify peak memory is bounded by high_water_atoms
- Verify base budget enforced during normal operations

### Files
- `ovsdb/row-cache.c` — dynamic max_atoms, burst mode
- `ovsdb/row-cache.h` — new API (enter/exit burst mode)
- `ovsdb/table.c` — trigger burst mode in Path 3

---

## Phase 5: Async Full-Load with Deferred Client Response

### Problem
When northd or a CLI tool requests the full database, the main thread reads every row from disk synchronously, blocking all other clients. The JSON response is built entirely in memory before sending.

### Design

**Offload the full-table iteration to a worker thread.** The client connection is parked (similar to trigger parking for lazy-load), and the worker thread reads rows from disk, builds the response, and signals completion.

**Flow:**
```
Client sends monitor_cond [true]
  │
  ▼
Main thread: detect full-load (no conditions, disk-store)
  → Create async_load_request
  → Submit to worker pool
  → Park the client (defer response)
  → Return to event loop (serve other clients)

Worker thread:
  → Open disk cursor
  → For each row: pread → deserialize → build JSON fragment
  → Accumulate response (or stream chunks)
  → Signal completion via seq_change()

Main thread (on wake):
  → Retrieve response from async_load_request
  → Send to parked client
  → Resume event loop
```

**Reuses existing patterns:**
- Trigger parking (from lazy-load): park client, resume on event
- Worker pool: submit/done callback pattern
- seq API: wake main thread on completion

**Key difference from lazy-load:** Lazy-load loads ONE row per job. This loads an ENTIRE table. The worker thread holds the disk cursor open for the duration.

**Cache interaction:** The worker thread does NOT insert into the cache (Phase 4's burst mode handles that on the main thread if needed). The worker just builds the JSON response.

### Tests
- Full-load request: verify client gets correct response
- During full-load: verify other clients are not blocked
- Verify response identical to synchronous path
- Concurrent full-load requests: verify correct isolation

### Files
- `ovsdb/jsonrpc-server.c` — detect full-load, park client, resume on completion
- `ovsdb/monitor.c` — async initial dump path
- New: async load request struct and worker function

---

## Implementation Order

```
Phase 1 (tracking)
    │
    ▼
Phase 2 (maintenance thread)  ←── depends on Phase 1
    │
    ▼
Phase 3 (rwlock)  ←── depends on Phase 2
    │
    ├──→ Phase 4 (dynamic sizing)  ←── depends on Phase 2
    │
    └──→ Phase 5 (async full-load) ←── depends on Phase 3
```

Each phase is independently testable and deployable.
Phase 1-2 can ship together (tracking + thread).
Phase 3 enables Phase 4 and 5 in parallel.

## Verification per Phase

```bash
make -j4
make check TESTSUITEFLAGS="-j4 -k ovsdb"
```

After each phase: `/simplify` and `/review`.
