# OVSDB Clock-Sweep Row Cache

## Overview

The OVSDB row cache (`ovsdb/row-cache.c`) caches deserialized row objects
in front of the disk store, bounded by an atom budget.  It replaces the
original LRU doubly-linked list with a PostgreSQL-style clock-sweep
algorithm, adding scan-aware eviction, dynamic sizing, background
maintenance, and concurrent access support.

## Architecture

```
                           +------------------+
                           |   ovsdb_row_cache |
                           |                  |
    lookup(uuid) --------->| hmap (UUID->entry)|-------> entry
    insert(row)            | clock_buf[0..N]  |         (CACHED)
    remove(uuid)           | scan_ring[0..255]|
    pin/unpin              | sweeper thread   |
                           +------------------+
                                   |
                           +-------+-------+
                           |               |
                      clock_buf       scan_ring
                    (main cache)    (bulk-read temp)
                    budget-bounded   fixed 256 slots
                    usage_count 0-5  usage_count 0-1
```

## Entry Lifecycle

```
  disk store has UUID
         |
         v
  +------------+     add_unloaded()     +-----------+
  |   ON DISK  | ---------------------> | UNLOADED  |
  |            |                        | hmap only |
  +------------+                        | no clock  |
                                        | no row    |
                                        +-----------+
                                             |
                          lazy_load_request() |
                                             v
                                        +-----------+
                                        |  LOADING  |
                                        | hmap only |
                                        | worker    |
                                        | reads disk|
                                        +-----------+
                                          /        \
                         worker success  /          \ worker failure
                                        v            v
                                   +---------+  +-----------+
                                   |  CACHED |  |   ERROR   |
                                   | hmap +  |  | hmap only |
                                   | clock   |  | no retry  |
                                   | buf     |  +-----------+
                                   | row data|       ^
                                   +---------+       |
                                     |    |    max retries
                                     |    |    exceeded
                              lookup |    | evict (budget    +----------+
                              touch  |    | pressure or      | UNLOADED |
                              uc++   |    | decay to 0)      | (retry)  |
                                     |    |                  +----------+
                                     v    v                       ^
                               usage_count                        |
                               decremented          load failure  |
                               by sweeper           (retries left)|
                               every 30s            backoff 100-500ms
```

## Clock-Sweep Eviction

The clock-sweep algorithm replaces LRU with approximate-LRU that avoids
global mutation on the read path.

### How it works

Each CACHED entry has a `usage_count` (0 to MAX_USAGE=5).  A circular
clock hand sweeps through the buffer:

```
  clock_buf:  [A:3] [B:0] [C:1] [D:5] [E:0] [F:2] [NULL] [G:1]
                              ^
                          clock_hand

  Sweep step:
    C has uc=1 -> decrement to 0, skip
    D has uc=5 -> decrement to 4, skip
    E has uc=0 -> EVICT
    (budget satisfied, stop)
```

- **On lookup**: `usage_count++` (capped at 5).  Atomic, no list mutation.
- **On eviction**: hand sweeps, decrements non-zero counts, evicts zeros.
- **Advantage over LRU**: reads don't mutate any shared structure.
  Concurrent lookups under rdlock are truly parallel.

### Clock buffer management

- Starts at 64 slots, doubles on demand.
- Compacts (halves) when utilization drops below 25 percent.
- UNLOADED entries are NOT in the clock buffer (they have nothing to
  evict).  Only CACHED entries occupy clock slots.

## Scan-Aware Behavior

### Problem

Sequential scans (e.g., full-table monitor responses) read every row
from disk.  Without protection, these scanned rows would evict the hot
working set from the cache.

### Solution: Two modes

```
  +-------------------+     +---------------------+
  | Conditioned scan  |     | Unconditioned scan  |
  | (filtered query)  |     | (full table dump)   |
  +-------------------+     +---------------------+
           |                          |
     bulk_read mode             burst mode
           |                          |
     +----------+              +----------+
     | Scan Ring|              | Main     |
     | 256 slots|              | Clock Buf|
     | uc cap=1 |              | budget   |
     | discarded|              | raised   |
     | on end   |              | to 10x   |
     +----------+              +----------+
```

**Bulk-read (scan ring)**: Rows go into a fixed 256-slot circular buffer
with `usage_count` capped at 1.  No promotion to main cache.  Evicted
when `bulk_read_end()` is called.  Prevents scan pollution.

**Burst mode**: Budget temporarily raised to `high_water_atoms` (10x
base, capped at 100M).  Rows go into the main clock buffer and persist
for subsequent queries.  The sweeper gradually shrinks the budget back
to base (10 percent per 30s cycle, ~5 minutes to converge).

## Dynamic Cache Sizing

```
  max_atoms timeline during burst:

  10M |  ___________
      | /           \
      |/             \___________
  1M  |                          \_____________ (base)
      +----+----+----+----+----+----+----+----+->
      0s   30s  60s  90s  120s 150s 180s 210s   time

      enter_burst    exit_burst    sweeper shrinks
                                   10%/cycle
```

- `base_max_atoms`: configured budget (default 1M, via --cache-max-atoms)
- `high_water_atoms`: burst ceiling (10x base, capped at 100M)
- `burst_refcount`: reference-counted for concurrent scans
- `set_max_atoms()`: runtime adjustment via ovs-appctl

## Sweeper Thread

### Responsibilities

```
  Every 30 seconds (or on latch signal):

  +-------------------------------------------+
  |            SWEEPER WAKE CYCLE              |
  +-------------------------------------------+
  |                                            |
  |  1. DECAY (rdlock)                         |
  |     Walk clock_buf                         |
  |     Decrement every usage_count by 1       |
  |     Concurrent lookups NOT blocked         |
  |                                            |
  |  2. EVICT (wrlock, only if over budget)    |
  |     Clock-sweep until total <= max_atoms   |
  |     All readers/writers blocked            |
  |                                            |
  |  3. SHRINK (wrlock, only if burst ended)   |
  |     max_atoms *= 0.9                       |
  |     Floor at base_max_atoms                |
  |                                            |
  +-------------------------------------------+
```

### Deferred start

The sweeper is NOT started at `cache_create()` time.  During startup,
only the main thread touches the cache (no concurrency needed).  The
sweeper is started via `ovsdb_row_cache_start_sweeper()` after warmup
completes, eliminating lock contention during the `add_unloaded` x N
startup path.

### Startup sequence

```
  cache_create(max_atoms)           no sweeper, no lock contention
       |
  add_unloaded x 1.68M             hmap-only, no clock_buf slots
       |
  bloom filter + name index         no cache involvement
       |
  enter_burst()                     raise budget to 10x
       |
  bulk_request_until_full()         submit warmup jobs to worker pool
       |
  start_sweeper()                   NOW start background thread
       |
  exit_burst()                      sweeper shrinks back to base
```

## Concurrency Model

### Lock hierarchy

```
  ovs_rwlock (cache->rwlock)
  |
  +-- rdlock (shared, concurrent readers)
  |   - lookup()
  |   - get_state(), is_retry_ready(), has_unloaded()
  |   - n_atoms(), count(), max_atoms(), base_max_atoms()
  |   - scan_reuse(), usage_histogram()
  |   - sweeper decay walk
  |
  +-- wrlock (exclusive, one writer)
      - insert(), remove()
      - pin(), unpin()
      - set_state(), record_load_failure(), add_unloaded()
      - for_each_loaded(), for_each_unloaded()
      - bulk_read_start/end, enter/exit_burst
      - sweeper eviction + shrink
      - start_sweeper()

  No lock needed:
  - hits(), misses(), evictions()    (atomic_uint64_t)
  - max_atoms (immutable base)       (single-word read)
```

### Contention points

```
  Thread            Lock      Duration        Frequency     Impact
  ----------------  --------  --------------  -----------   ------
  Main (lookup)     rdlock    ~microseconds   High          None (shared)
  Main (insert)     wrlock    ~microseconds   Medium        Blocks reads briefly
  Sweeper (decay)   rdlock    ~ms (100K ops)  Every 30s     None (shared)
  Sweeper (evict)   wrlock    ~ms             On demand     Blocks all briefly
  Worker done cb    wrlock    ~microseconds   Per row load  Blocks reads briefly
  for_each          wrlock    ~ms (full scan) Occasional    Blocks all
```

**Worst case**: `for_each_loaded` holds wrlock for the entire hmap
iteration.  During this time, the sweeper blocks (waits for wrlock),
and all lookups block (rdlock waits for wrlock waiters to drain).

**Mitigation**: `for_each` runs infrequently (monitor initial snapshots).
The sweeper's decay runs under rdlock (no reader blocking).  The sweeper
only takes wrlock when eviction or shrink is actually needed.

### Re-entrancy safety

```
  for_each_loaded (wrlock held, iterating > 0)
       |
       +-- callback calls insert()
       |   |
       |   +-- wrlock_if_needed__() checks atomic iterating
       |   |   iterating > 0 -> skip lock (already held)
       |   +-- insert_locked__() runs directly
       |
       +-- callback calls set_state()
           |
           +-- same pattern: skip lock, modify directly
```

If an entry is evicted during iteration, it is placed on the
`deferred_free` list (not removed from hmap) so the iterator's
pre-fetched next pointer stays valid.  Deferred entries are swept
after the outermost iteration completes.

## Metrics

| Metric           | Type           | Lock   | Description                  |
|------------------|----------------|--------|------------------------------|
| hits             | atomic_uint64_t| none   | Successful lookups           |
| misses           | atomic_uint64_t| none   | Failed lookups               |
| evictions        | atomic_uint64_t| none   | Entries evicted              |
| n_atoms          | size_t         | rdlock | Current atom usage           |
| max_atoms        | size_t         | rdlock | Current dynamic budget       |
| base_max_atoms   | size_t         | rdlock | Configured base budget       |
| count            | size_t         | rdlock | Number of entries            |
| scan_reuse       | size_t         | rdlock | Scan ring slot reuses        |
| usage_histogram  | size_t[6]      | rdlock | Entries at each usage_count  |

## Configuration

| Parameter              | Default   | Source                        |
|------------------------|-----------|-------------------------------|
| base_max_atoms         | 1,000,000 | --cache-max-atoms CLI flag    |
|                        |           | OVS_OVSDB_CACHE_MAX_ATOMS env|
| high_water_atoms       | 10x base  | Computed (capped at 100M)     |
| MAX_USAGE              | 5         | OVSDB_ROW_CACHE_MAX_USAGE     |
| SCAN_RING_SIZE         | 256       | OVSDB_ROW_CACHE_SCAN_RING_SIZE|
| SWEEP_INTERVAL_MS      | 30,000    | Compile-time constant         |
| CLOCK_BUF_INIT_CAP     | 64        | Compile-time constant         |

## Commit History

```
ed243e408 Replace LRU with clock-sweep + scan ring + compaction
92693155f Add concurrency (rwlock + atomics) + scan-ring wiring + stress tests
373896afd Add background sweeper thread for eviction and usage decay
53638d1ce Fix data races (atomic usage_count, atomic iterating)
a3d3a1367 Add dynamic sizing, low-contention sweeper, deferred startup
8aab04a3e Fix concurrent burst (refcount), lock-free getter races
b03dabc18 Fix deadlock in set_state from for_each callback
46ea14920 Exclude UNLOADED entries from clock buffer for faster startup
```

## Design Decisions

1. **Clock-sweep over LRU**: Eliminates global list mutation on read
   path.  Lookup is O(1) with no shared-structure write.

2. **Scan ring vs burst**: Conditioned scans (filtered) use the
   disposable scan ring.  Unconditioned scans (full table) use burst
   mode so rows persist for subsequent queries.

3. **Decay under rdlock**: The sweeper's most frequent operation
   (decrementing usage_count) uses rdlock, not wrlock.  Concurrent
   lookups are never blocked by the sweeper.

4. **Deferred sweeper start**: No sweeper thread during startup.
   Eliminates 100K+ wrlock/unlock cycles on the add_unloaded path.

5. **UNLOADED entries excluded from clock_buf**: Placeholders with
   no data (n_atoms=0) don't need eviction slots.  Saves 15 clock_buf
   doublings and 2M wasted sweep iterations for a 1.68M-entry database.

6. **Re-entrant locking via atomic iterating**: `for_each` callbacks
   can call insert/remove/set_state without deadlock.  The atomic
   `iterating` counter lets public functions detect that wrlock is
   already held on the current thread.

7. **Burst refcount**: Multiple concurrent unconditioned scans can
   enter burst simultaneously.  Budget stays raised until all exit.

8. **Mini-evict inline + full evict in sweeper**: `insert()` evicts
   at most one entry inline (2 clock revolutions), then signals the
   sweeper for the remaining cleanup.  Keeps insert latency bounded.
