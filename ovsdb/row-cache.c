/* Copyright (c) 2026 Magalu Cloud.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

#include <config.h>

#include "row-cache.h"

#include <string.h>

#include "latch.h"
#include "openvswitch/hmap.h"
#include "openvswitch/list.h"
#include "openvswitch/vlog.h"
#include "ovs-atomic.h"
#include "ovs-thread.h"
#include "openvswitch/poll-loop.h"
#include "row.h"
#include "timeval.h"
#include "uuid.h"
#include "util.h"

VLOG_DEFINE_THIS_MODULE(ovsdb_row_cache);

/* Initial capacity of the clock buffer. */
#define CLOCK_BUF_INIT_CAP 64
BUILD_ASSERT_DECL(IS_POW2(CLOCK_BUF_INIT_CAP));

/* Sweeper thread interval.  Decay and shrink both run once per
 * wake cycle.  Budget-exceeded events wake the sweeper instantly
 * via latch, so this interval only governs background maintenance. */
#define SWEEP_INTERVAL_MS  30000

/* Hard ceiling on dynamic high-water budget (atoms). */
#define HIGH_WATER_CEILING 100000000

/* A single cached row, stored inside the cache's hmap and clock buffer.
 *
 * Clock-sweep replacement: each entry has a 'usage_count' (0 to
 * MAX_USAGE_COUNT).  On lookup the count is incremented; on eviction
 * sweep the clock hand decrements non-zero counts and evicts entries
 * whose count reaches zero. */
struct ovsdb_row_cache_entry {
    struct hmap_node hmap_node;   /* In cache->entries, hashed by UUID. */
    struct ovs_list deferred_node; /* In cache->deferred_free when
                                    * iterating > 0. */
    struct uuid uuid;             /* Row UUID (copy for fast compare). */
    struct ovsdb_row *row;        /* Owned by the cache; NULL if not yet
                                   * loaded (UNLOADED or LOADING state). */
    size_t n_atoms;               /* Atom cost of this row. */
    bool pinned;                  /* If true, entry cannot be evicted. */
    enum ovsdb_row_state state;   /* Loading state (Phase 2). */
    uint8_t load_failures;        /* Consecutive load failure count. */
    long long int retry_after;    /* time_msec() before which no retry
                                   * should be attempted (backoff). */

    /* Clock-sweep fields. */
    atomic_int usage_count;       /* Access frequency, 0..MAX_USAGE.
                                   * Atomic because lookup() bumps it
                                   * under rdlock while evict/decay
                                   * access it under wrlock. */
    uint32_t clock_slot;          /* Index in cache->clock_buf. */
    bool in_scan_ring;            /* True if entry lives in scan ring. */
};

/* Scan ring: fixed-size circular buffer for bulk-read mode.
 *
 * When bulk_read is active, inserts go into the ring instead of the
 * main clock buffer.  Ring entries have usage_count capped at 1 and
 * are never promoted to the main cache, preventing sequential scans
 * from evicting hot working-set entries. */
struct ovsdb_row_cache_scan_ring {
    struct ovsdb_row_cache_entry *slots[OVSDB_ROW_CACHE_SCAN_RING_SIZE];
    uint32_t head;                /* Next slot to write into. */
    size_t count;                 /* Number of occupied slots. */
    size_t reuse_count;           /* Metric: times a slot was reused. */
};

/* A clock-sweep cache of ovsdb_row objects, bounded by a maximum
 * atom count.
 *
 * Rows may be "pinned" to prevent eviction.  When the total atom
 * count exceeds 'max_atoms', the clock hand sweeps the buffer:
 * entries with usage_count > 0 get decremented; entries at zero
 * are evicted.  If every remaining entry is pinned, eviction stops
 * and the cache is allowed to exceed the limit temporarily.
 *
 * Re-entrancy safety:  The for_each_loaded() and for_each_unloaded()
 * functions iterate the hmap with HMAP_FOR_EACH_SAFE.  If a
 * callback triggers insert or remove, evict_entry__() defers both
 * hmap_remove and free to a 'deferred_free' list while the
 * 'iterating' depth counter is positive.  Deferred entries are
 * marked dead (state=ERROR, row=NULL) but kept in the hmap chain
 * so the iterator's pre-fetched next pointer stays valid.  The
 * deferred entries are swept after the outermost iteration
 * completes. */
struct ovsdb_row_cache {
    struct ovs_rwlock rwlock;     /* Protects structural mutations.
                                   * Readers (lookup, stats) take rdlock;
                                   * writers (insert, remove, etc.) take
                                   * wrlock. */
    struct hmap entries;          /* Contains ovsdb_row_cache_entry. */

    /* Clock-sweep buffer (replaces LRU list). */
    struct ovsdb_row_cache_entry **clock_buf; /* Flat slot array. */
    size_t clock_cap;             /* Allocated capacity. */
    size_t clock_len;             /* Number of occupied slots. */
    uint32_t clock_hand;          /* Next slot to examine. */

    /* Scan ring for bulk-read mode. */
    struct ovsdb_row_cache_scan_ring scan_ring;
    bool bulk_read;               /* True when in BULK_READ mode. */

    size_t max_atoms;             /* Current dynamic atom budget. */
    size_t base_max_atoms;        /* Configured base budget (immutable). */
    size_t high_water_atoms;      /* Upper bound for burst mode. */
    bool burst_mode;              /* True during unconditioned scan. */
    size_t total_atoms;           /* Sum of n_atoms for all entries. */
    size_t n_entries;             /* Number of entries in the cache. */

    /* Atomic counters for lock-free stats access. */
    atomic_uint64_t hits;         /* Number of successful lookups. */
    atomic_uint64_t misses;       /* Number of failed lookups. */
    atomic_uint64_t evictions;    /* Number of evicted entries. */

    /* Re-entrancy guard for safe iteration.  Atomic so that
     * insert/remove can check it without holding the lock to
     * detect re-entrant calls from for_each callbacks. */
    atomic_int iterating;         /* Nesting depth of for_each_* calls.
                                   * When > 0, rwlock is already held by
                                   * the current thread (wrlock), so
                                   * public insert/remove must not
                                   * re-acquire. */
    struct ovs_list deferred_free; /* Entries to free after iteration. */

    /* Background sweeper thread. */
    pthread_t sweeper;            /* Runs eviction + decay. */
    struct latch sweep_latch;     /* Signaled when budget exceeded. */
    struct latch exit_latch;      /* Signaled on destroy. */
    bool sweeper_started;         /* False until start_sweeper(). */
};

/* ------------------------------------------------------------------
 * Internal helpers.
 * ------------------------------------------------------------------ */

/* Looks up the cache entry for 'uuid'.  Returns NULL if not found. */
static struct ovsdb_row_cache_entry *
ovsdb_row_cache_find__(const struct ovsdb_row_cache *cache,
                       const struct uuid *uuid)
{
    struct ovsdb_row_cache_entry *entry;
    size_t hash = uuid_hash(uuid);

    HMAP_FOR_EACH_WITH_HASH (entry, hmap_node, hash, &cache->entries) {
        if (uuid_equals(&entry->uuid, uuid)) {
            return entry;
        }
    }
    return NULL;
}

/* Increments 'entry->usage_count', capped at MAX_USAGE_COUNT.
 * For scan-ring entries, the cap is 1 to prevent promotion.
 * Safe to call under rdlock (uses atomic operations). */
static void
ovsdb_row_cache_touch__(struct ovsdb_row_cache_entry *entry)
{
    int cap = entry->in_scan_ring
              ? 1
              : OVSDB_ROW_CACHE_MAX_USAGE;
    int uc;
    atomic_read_relaxed(&entry->usage_count, &uc);
    if (uc < cap) {
        atomic_store_relaxed(&entry->usage_count, uc + 1);
    }
}

/* Sweeps entries on the deferred_free list, completing their removal
 * from the hmap and freeing them.
 * Must only be called when iterating == 0. */
static void
ovsdb_row_cache_sweep_deferred__(struct ovsdb_row_cache *cache)
{
    struct ovsdb_row_cache_entry *entry;

    while (!ovs_list_is_empty(&cache->deferred_free)) {
        entry = CONTAINER_OF(ovs_list_pop_front(&cache->deferred_free),
                             struct ovsdb_row_cache_entry,
                             deferred_node);
        hmap_remove(&cache->entries, &entry->hmap_node);
        /* Row was already destroyed in evict_entry__. */
        free(entry);
    }
}

/* Clears the scan ring slot for 'entry'.  Uses entry->clock_slot
 * which stores the ring index when in_scan_ring is true. */
static void
ovsdb_row_cache_clear_scan_ring_slot__(struct ovsdb_row_cache *cache,
                                       struct ovsdb_row_cache_entry *entry)
{
    uint32_t slot = entry->clock_slot;
    if (slot < OVSDB_ROW_CACHE_SCAN_RING_SIZE
        && cache->scan_ring.slots[slot] == entry) {
        cache->scan_ring.slots[slot] = NULL;
        cache->scan_ring.count--;
    }
}

/* Removes and frees 'entry', destroying the contained row (if any).
 * The caller is responsible for any additional bookkeeping.
 * Entries in UNLOADED/LOADING state may have row==NULL.
 *
 * If the cache is currently being iterated (iterating > 0), both
 * the hmap_remove and free are deferred.  The entry is marked as
 * dead (state = OVSDB_ROW_ERROR, row = NULL) so iterators skip it,
 * but its hmap_node stays in the chain to avoid corrupting a
 * pre-fetched HMAP_FOR_EACH_SAFE next pointer.  The actual
 * hmap_remove + free happens in sweep_deferred__() after the
 * outermost iteration completes. */
static void
ovsdb_row_cache_evict_entry__(struct ovsdb_row_cache *cache,
                              struct ovsdb_row_cache_entry *entry)
{
    cache->total_atoms -= entry->n_atoms;
    cache->n_entries--;

    /* Remove from clock buffer or scan ring. */
    if (entry->in_scan_ring) {
        ovsdb_row_cache_clear_scan_ring_slot__(cache, entry);
    } else if (entry->clock_slot < cache->clock_cap) {
        cache->clock_buf[entry->clock_slot] = NULL;
        cache->clock_len--;
    }

    { int iter_depth;
      atomic_read_relaxed(&cache->iterating, &iter_depth);

      if (iter_depth > 0) {
          /* Defer both hmap_remove and free.  Destroy the row now
           * to release memory, but keep the entry in the hmap so
           * the iterator's bucket chain stays valid. */
          if (entry->row) {
              ovsdb_row_destroy(entry->row);
              entry->row = NULL;
          }
          entry->n_atoms = 0;
          entry->state = OVSDB_ROW_ERROR; /* Mark dead for iterators. */
          ovs_list_push_back(&cache->deferred_free,
                             &entry->deferred_node);
      } else {
          hmap_remove(&cache->entries, &entry->hmap_node);
          if (entry->row) {
              ovsdb_row_destroy(entry->row);
          }
          free(entry);
      }
    }
}

/* Finds a free slot in clock_buf, growing the buffer if necessary.
 * Returns the slot index. */
static uint32_t
ovsdb_row_cache_alloc_slot__(struct ovsdb_row_cache *cache)
{
    uint32_t i;

    /* Grow if needed.  Shrinking is handled by compact__(). */
    if (cache->clock_len >= cache->clock_cap) {
        size_t new_cap = cache->clock_cap * 2;
        struct ovsdb_row_cache_entry **new_buf;
        new_buf = xcalloc(new_cap, sizeof *new_buf);
        memcpy(new_buf, cache->clock_buf,
               cache->clock_cap * sizeof *new_buf);
        free(cache->clock_buf);
        cache->clock_buf = new_buf;
        /* Return first slot in newly extended region. */
        i = cache->clock_cap;
        cache->clock_cap = new_cap;
        return i;
    }

    /* Scan for a NULL slot starting at clock_hand. */
    for (i = 0; i < cache->clock_cap; i++) {
        uint32_t slot = (cache->clock_hand + i) % cache->clock_cap;
        if (!cache->clock_buf[slot]) {
            return slot;
        }
    }

    /* Should not be reached (we grow above). */
    OVS_NOT_REACHED();
}

/* Shrinks clock_buf when utilization drops below one quarter to
 * reduce memory waste and sweep overhead.  Packs entries densely
 * at the front of the new buffer and updates their clock_slot.
 * Must not be called during iteration (deferred entries hold
 * stale slot indices). */
static void
ovsdb_row_cache_compact__(struct ovsdb_row_cache *cache)
{
    size_t new_cap;
    struct ovsdb_row_cache_entry **new_buf;
    uint32_t dst = 0;
    uint32_t i;

    { int iter_depth;
      atomic_read_relaxed(&cache->iterating, &iter_depth);
      if (iter_depth > 0) {
          return;
      }
    }
    if (cache->clock_cap <= CLOCK_BUF_INIT_CAP
        || cache->clock_len >= cache->clock_cap / 4) {
        return;
    }

    new_cap = cache->clock_cap / 2;
    while (new_cap > CLOCK_BUF_INIT_CAP
           && cache->clock_len < new_cap / 4) {
        new_cap /= 2;
    }

    new_buf = xcalloc(new_cap, sizeof *new_buf);
    for (i = 0; i < cache->clock_cap; i++) {
        if (cache->clock_buf[i]) {
            cache->clock_buf[i]->clock_slot = dst;
            new_buf[dst++] = cache->clock_buf[i];
        }
    }

    free(cache->clock_buf);
    cache->clock_buf = new_buf;
    cache->clock_cap = new_cap;
    cache->clock_hand = dst % new_cap;
}

/* Clock-sweep eviction: sweeps the clock buffer, decrementing
 * usage_count for non-zero entries and evicting zero-count entries
 * until total_atoms <= max_atoms or all remaining entries are
 * pinned.
 *
 * The sweep allows up to (MAX_USAGE + 2) full revolutions so that
 * even entries at maximum usage_count can be decremented to zero
 * and evicted. */
static void
ovsdb_row_cache_evict__(struct ovsdb_row_cache *cache)
{
    size_t max_steps;
    size_t steps = 0;

    /* Hard limit: enough revolutions to drain MAX_USAGE, plus margin. */
    max_steps = (OVSDB_ROW_CACHE_MAX_USAGE + 2)
                * (cache->clock_cap ? cache->clock_cap : 1);

    while (cache->total_atoms > cache->max_atoms
           && cache->clock_len > 0
           && steps < max_steps) {
        struct ovsdb_row_cache_entry *e;
        uint32_t slot = cache->clock_hand;

        cache->clock_hand = (slot + 1) % cache->clock_cap;
        steps++;

        e = cache->clock_buf[slot];
        if (!e) {
            continue; /* Empty slot. */
        }

        if (e->pinned) {
            continue;
        }

        { int uc;
          atomic_read_relaxed(&e->usage_count, &uc);
          if (uc > 0) {
              atomic_store_relaxed(&e->usage_count, uc - 1);
              continue;
          }
        }

        /* usage_count == 0, not pinned -> evict. */
        VLOG_DBG("evicting row "UUID_FMT" (%"PRIuSIZE" atoms)",
                 UUID_ARGS(&e->uuid), e->n_atoms);
        { uint64_t orig;
          atomic_add_relaxed(&cache->evictions, 1, &orig); }
        ovsdb_row_cache_evict_entry__(cache, e);
    }

    /* Shrink buffer if utilization dropped. */
    ovsdb_row_cache_compact__(cache);
}

/* ------------------------------------------------------------------
 * Sweeper helpers.
 * ------------------------------------------------------------------ */

/* Decrements usage_count for all entries by 1, flooring at 0.
 * Uses rdlock so concurrent lookups are not blocked.  Only
 * writers (insert/remove) are briefly blocked. */
static void
ovsdb_row_cache_decay_usage__(struct ovsdb_row_cache *cache)
{
    uint32_t i;

    ovs_rwlock_rdlock(&cache->rwlock);
    for (i = 0; i < cache->clock_cap; i++) {
        struct ovsdb_row_cache_entry *e = cache->clock_buf[i];
        if (e) {
            int uc;
            atomic_read_relaxed(&e->usage_count, &uc);
            if (uc > 0) {
                atomic_store_relaxed(&e->usage_count, uc - 1);
            }
        }
    }
    ovs_rwlock_unlock(&cache->rwlock);
}

/* Evicts at most one entry via two clock-hand revolutions.
 * Two revolutions are needed because the first pass may only
 * decrement usage_count from 1 to 0, and the second pass
 * evicts the now-zero entry.
 *
 * This is best-effort: entries at usage_count >= 2 will only
 * be decremented, not evicted.  The full eviction sweep is
 * deferred to the sweeper thread.  During sustained insert
 * bursts, the cache may temporarily exceed its atom budget
 * until the sweeper runs (up to SWEEP_INTERVAL_MS later).
 *
 * Used as inline fallback when the sweeper cannot keep up.
 * Caller must hold wrlock. */
static void
ovsdb_row_cache_evict_one__(struct ovsdb_row_cache *cache)
{
    size_t steps = 0;
    size_t max_steps = 2 * cache->clock_cap;

    while (cache->total_atoms > cache->max_atoms
           && steps < max_steps) {
        uint32_t slot = cache->clock_hand;
        struct ovsdb_row_cache_entry *e;

        cache->clock_hand = (slot + 1) % cache->clock_cap;
        steps++;

        e = cache->clock_buf[slot];
        if (!e || e->pinned) {
            continue;
        }
        { int uc;
          atomic_read_relaxed(&e->usage_count, &uc);
          if (uc > 0) {
              atomic_store_relaxed(&e->usage_count, uc - 1);
              continue;
          }
        }
        { uint64_t orig;
          atomic_add_relaxed(&cache->evictions, 1, &orig); }
        ovsdb_row_cache_evict_entry__(cache, e);
        return; /* Evicted one -- enough for inline. */
    }
}

/* Background sweeper thread.  Responsibilities:
 * 1. Decay usage_count under rdlock (doesn't block readers).
 * 2. Evict over-budget entries under wrlock (only when needed).
 * 3. Gradually shrink burst budget toward base.
 *
 * The sweeper blocks on wrlock while iteration (for_each) is
 * in progress; it can never observe iterating > 0. */
static void *
ovsdb_row_cache_sweeper_main__(void *arg)
{
    struct ovsdb_row_cache *cache = arg;

    while (!latch_is_set(&cache->exit_latch)) {
        bool need_sweep = latch_poll(&cache->sweep_latch);

        /* Decay under rdlock -- concurrent lookups not blocked. */
        ovsdb_row_cache_decay_usage__(cache);

        /* Eviction + shrink under wrlock, only if needed. */
        if (!cache->bulk_read
            && (need_sweep
                || cache->total_atoms > cache->max_atoms
                || (!cache->burst_mode
                    && cache->max_atoms > cache->base_max_atoms))) {
            ovs_rwlock_wrlock(&cache->rwlock);

            if (cache->total_atoms > cache->max_atoms) {
                ovsdb_row_cache_evict__(cache);
            }

            /* Gradually shrink burst budget toward base. */
            if (!cache->burst_mode
                && cache->max_atoms > cache->base_max_atoms) {
                size_t target = cache->max_atoms * 9 / 10;
                if (target < cache->base_max_atoms) {
                    target = cache->base_max_atoms;
                }
                cache->max_atoms = target;
            }

            ovs_rwlock_unlock(&cache->rwlock);
        }

        /* Wait for signal or timer. */
        poll_timer_wait(SWEEP_INTERVAL_MS);
        latch_wait(&cache->sweep_latch);
        latch_wait(&cache->exit_latch);
        poll_block();
    }
    return NULL;
}

/* ------------------------------------------------------------------
 * Scan ring helpers.
 * ------------------------------------------------------------------ */

/* Evicts a scan ring entry, removing it from the ring slot and
 * the main hmap. */
static void
ovsdb_row_cache_scan_ring_evict__(struct ovsdb_row_cache *cache,
                                  uint32_t ring_slot)
{
    struct ovsdb_row_cache_entry *e;

    e = cache->scan_ring.slots[ring_slot];
    if (!e) {
        return;
    }

    cache->scan_ring.slots[ring_slot] = NULL;
    cache->scan_ring.count--;
    { uint64_t orig;
      atomic_add_relaxed(&cache->evictions, 1, &orig); }
    ovsdb_row_cache_evict_entry__(cache, e);
}

/* Inserts 'entry' into the scan ring instead of the main clock
 * buffer.  If the target ring slot is occupied, the old entry is
 * evicted first. */
static void
ovsdb_row_cache_scan_ring_insert__(struct ovsdb_row_cache *cache,
                                   struct ovsdb_row_cache_entry *entry)
{
    uint32_t slot;

    slot = cache->scan_ring.head % OVSDB_ROW_CACHE_SCAN_RING_SIZE;

    if (cache->scan_ring.slots[slot]) {
        ovsdb_row_cache_scan_ring_evict__(cache, slot);
        cache->scan_ring.reuse_count++;
    }

    entry->in_scan_ring = true;
    atomic_init(&entry->usage_count, 0);
    entry->clock_slot = slot; /* Ring index (not clock_buf index). */
    cache->scan_ring.slots[slot] = entry;
    cache->scan_ring.count++;
    cache->scan_ring.head++;
}

/* ------------------------------------------------------------------
 * Lifecycle.
 * ------------------------------------------------------------------ */

/* Creates a new row cache with a soft limit of 'max_atoms' atoms.
 * The cache starts empty; 'max_atoms' may be 0, in which case every
 * unpinned insert is immediately evicted. */
struct ovsdb_row_cache *
ovsdb_row_cache_create(size_t max_atoms)
{
    struct ovsdb_row_cache *cache;

    cache = xmalloc(sizeof *cache);
    ovs_rwlock_init(&cache->rwlock);
    hmap_init(&cache->entries);

    cache->clock_buf = xcalloc(CLOCK_BUF_INIT_CAP,
                               sizeof *cache->clock_buf);
    cache->clock_cap = CLOCK_BUF_INIT_CAP;
    cache->clock_len = 0;
    cache->clock_hand = 0;

    memset(&cache->scan_ring, 0, sizeof cache->scan_ring);
    cache->bulk_read = false;

    cache->max_atoms = max_atoms;
    cache->base_max_atoms = max_atoms;
    cache->high_water_atoms = MIN(max_atoms * 10, HIGH_WATER_CEILING);
    cache->burst_mode = false;
    cache->total_atoms = 0;
    cache->n_entries = 0;
    atomic_init(&cache->hits, 0);
    atomic_init(&cache->misses, 0);
    atomic_init(&cache->evictions, 0);
    atomic_init(&cache->iterating, 0);
    ovs_list_init(&cache->deferred_free);

    /* Sweeper thread is started later via start_sweeper(). */
    latch_init(&cache->sweep_latch);
    latch_init(&cache->exit_latch);
    cache->sweeper_started = false;

    VLOG_DBG("created row cache (max_atoms=%"PRIuSIZE")", max_atoms);
    return cache;
}

/* Destroys 'cache', freeing all contained rows and entries.
 * 'cache' may be NULL, in which case this is a no-op. */
void
ovsdb_row_cache_destroy(struct ovsdb_row_cache *cache)
{
    struct ovsdb_row_cache_entry *entry;

    if (!cache) {
        return;
    }

    /* Stop sweeper thread if it was started. */
    if (cache->sweeper_started) {
        latch_set(&cache->exit_latch);
        xpthread_join(cache->sweeper, NULL);
    }
    latch_destroy(&cache->sweep_latch);
    latch_destroy(&cache->exit_latch);

    /* Sweep any deferred entries first. */
    ovsdb_row_cache_sweep_deferred__(cache);

    HMAP_FOR_EACH_SAFE (entry, hmap_node, &cache->entries) {
        hmap_remove(&cache->entries, &entry->hmap_node);
        if (entry->row) {
            ovsdb_row_destroy(entry->row);
        }
        free(entry);
    }

    hmap_destroy(&cache->entries);
    free(cache->clock_buf);
    ovs_rwlock_destroy(&cache->rwlock);
    free(cache);
}

/* ------------------------------------------------------------------
 * Lookup.
 * ------------------------------------------------------------------ */

/* Looks up the cached row for 'uuid'.  Returns the row on a cache hit
 * (and increments its usage_count) or NULL on a miss.  The returned
 * row is still owned by the cache; the caller must not free it. */
struct ovsdb_row *
ovsdb_row_cache_lookup(struct ovsdb_row_cache *cache,
                       const struct uuid *uuid)
{
    struct ovsdb_row_cache_entry *entry;
    uint64_t orig;

    ovs_rwlock_rdlock(&cache->rwlock);
    entry = ovsdb_row_cache_find__(cache, uuid);
    if (entry) {
        ovsdb_row_cache_touch__(entry);
        atomic_add_relaxed(&cache->hits, 1, &orig);
        ovs_rwlock_unlock(&cache->rwlock);
        return entry->row;
    }

    atomic_add_relaxed(&cache->misses, 1, &orig);
    ovs_rwlock_unlock(&cache->rwlock);
    return NULL;
}

/* ------------------------------------------------------------------
 * Insert / remove.
 * ------------------------------------------------------------------ */

/* Internal insert, caller must hold wrlock. */
static void
ovsdb_row_cache_insert_locked__(struct ovsdb_row_cache *cache,
                                struct ovsdb_row *row, size_t n_atoms)
{
    struct ovsdb_row_cache_entry *old;
    struct ovsdb_row_cache_entry *entry;
    const struct uuid *uuid;

    uuid = ovsdb_row_get_uuid(row);

    /* Remove any existing entry for this UUID. */
    old = ovsdb_row_cache_find__(cache, uuid);
    if (old) {
        ovsdb_row_cache_evict_entry__(cache, old);
    }

    /* Create the new entry. */
    entry = xmalloc(sizeof *entry);
    entry->uuid = *uuid;
    entry->row = row;
    entry->n_atoms = n_atoms;
    entry->pinned = false;
    entry->state = OVSDB_ROW_CACHED;
    entry->load_failures = 0;
    entry->retry_after = 0;

    if (cache->bulk_read) {
        /* Route to scan ring. */
        ovsdb_row_cache_scan_ring_insert__(cache, entry);
    } else {
        /* Place in clock buffer. */
        uint32_t slot = ovsdb_row_cache_alloc_slot__(cache);
        atomic_init(&entry->usage_count, 1);
        entry->in_scan_ring = false;
        entry->clock_slot = slot;
        cache->clock_buf[slot] = entry;
        cache->clock_len++;
    }

    hmap_insert(&cache->entries, &entry->hmap_node,
                uuid_hash(uuid));
    cache->total_atoms += n_atoms;
    cache->n_entries++;

    VLOG_DBG("inserted row "UUID_FMT" (%"PRIuSIZE" atoms, "
             "total=%"PRIuSIZE")",
             UUID_ARGS(uuid), n_atoms, cache->total_atoms);

    /* If over budget, signal the sweeper for background eviction
     * and do a mini-evict (one entry) inline as fallback. */
    if (!cache->bulk_read
        && cache->total_atoms > cache->max_atoms) {
        latch_set(&cache->sweep_latch);
        ovsdb_row_cache_evict_one__(cache);
    }
}

/* Inserts 'row' into 'cache' with an atom cost of 'n_atoms'.
 *
 * If a row with the same UUID is already present, the old entry is
 * removed first (its row is destroyed).  The cache takes ownership of
 * 'row'.
 *
 * After insertion, entries are evicted via clock-sweep until
 * total_atoms <= max_atoms.
 *
 * In bulk_read mode, the entry is placed in the scan ring instead
 * of the main clock buffer. */
void
ovsdb_row_cache_insert(struct ovsdb_row_cache *cache,
                       struct ovsdb_row *row, size_t n_atoms)
{
    int depth;
    atomic_read_relaxed(&cache->iterating, &depth);
    if (depth > 0) {
        /* Called from a for_each callback on the current thread,
         * which already holds wrlock. */
        ovsdb_row_cache_insert_locked__(cache, row, n_atoms);
    } else {
        ovs_rwlock_wrlock(&cache->rwlock);
        ovsdb_row_cache_insert_locked__(cache, row, n_atoms);
        ovs_rwlock_unlock(&cache->rwlock);
    }
}

/* Internal remove, caller must hold wrlock. */
static void
ovsdb_row_cache_remove_locked__(struct ovsdb_row_cache *cache,
                                const struct uuid *uuid)
{
    struct ovsdb_row_cache_entry *entry;

    entry = ovsdb_row_cache_find__(cache, uuid);
    if (entry) {
        VLOG_DBG("removing row "UUID_FMT" (%"PRIuSIZE" atoms)",
                 UUID_ARGS(uuid), entry->n_atoms);
        ovsdb_row_cache_evict_entry__(cache, entry);
    }
}

/* Removes the cached entry for 'uuid', destroying its row.
 * Does nothing if no such entry exists. */
void
ovsdb_row_cache_remove(struct ovsdb_row_cache *cache,
                       const struct uuid *uuid)
{
    int depth;
    atomic_read_relaxed(&cache->iterating, &depth);
    if (depth > 0) {
        ovsdb_row_cache_remove_locked__(cache, uuid);
    } else {
        ovs_rwlock_wrlock(&cache->rwlock);
        ovsdb_row_cache_remove_locked__(cache, uuid);
        ovs_rwlock_unlock(&cache->rwlock);
    }
}

/* ------------------------------------------------------------------
 * Pinning.
 * ------------------------------------------------------------------ */

/* Pins the entry for 'uuid', preventing it from being evicted.
 * Does nothing if the entry does not exist or is already pinned. */
void
ovsdb_row_cache_pin(struct ovsdb_row_cache *cache,
                    const struct uuid *uuid)
{
    struct ovsdb_row_cache_entry *entry;

    ovs_rwlock_wrlock(&cache->rwlock);
    entry = ovsdb_row_cache_find__(cache, uuid);
    if (entry) {
        if (!entry->pinned) {
            entry->pinned = true;
            VLOG_DBG("pinned row "UUID_FMT, UUID_ARGS(uuid));
        }
    } else {
        VLOG_DBG("pin request for absent row "UUID_FMT,
                 UUID_ARGS(uuid));
    }
    ovs_rwlock_unlock(&cache->rwlock);
}

/* Unpins the entry for 'uuid', allowing it to be evicted again.
 * Does nothing if the entry does not exist or is not pinned.
 *
 * After unpinning, eviction is triggered if the cache is over
 * budget, since this entry may now be eligible for eviction. */
void
ovsdb_row_cache_unpin(struct ovsdb_row_cache *cache,
                      const struct uuid *uuid)
{
    struct ovsdb_row_cache_entry *entry;

    ovs_rwlock_wrlock(&cache->rwlock);
    entry = ovsdb_row_cache_find__(cache, uuid);
    if (entry) {
        if (entry->pinned) {
            entry->pinned = false;
            VLOG_DBG("unpinned row "UUID_FMT, UUID_ARGS(uuid));
            if (!entry->in_scan_ring) {
                ovsdb_row_cache_evict__(cache);
            }
        }
    } else {
        VLOG_DBG("unpin request for absent row "UUID_FMT,
                 UUID_ARGS(uuid));
    }
    ovs_rwlock_unlock(&cache->rwlock);
}

/* ------------------------------------------------------------------
 * Bulk-read mode (scan ring).
 * ------------------------------------------------------------------ */

/* Enters bulk-read mode.  Subsequent inserts go into the scan ring
 * instead of the main clock buffer, preventing scan workloads from
 * evicting hot entries. */
void
ovsdb_row_cache_bulk_read_start(struct ovsdb_row_cache *cache)
{
    ovs_rwlock_wrlock(&cache->rwlock);
    cache->bulk_read = true;
    ovs_rwlock_unlock(&cache->rwlock);
}

/* Exits bulk-read mode and evicts all entries remaining in the
 * scan ring.  Subsequent inserts go into the main clock buffer. */
void
ovsdb_row_cache_bulk_read_end(struct ovsdb_row_cache *cache)
{
    uint32_t i;

    ovs_rwlock_wrlock(&cache->rwlock);
    cache->bulk_read = false;

    for (i = 0; i < OVSDB_ROW_CACHE_SCAN_RING_SIZE; i++) {
        if (cache->scan_ring.slots[i]) {
            ovsdb_row_cache_scan_ring_evict__(cache, i);
        }
    }
    ovs_rwlock_unlock(&cache->rwlock);
}

/* ------------------------------------------------------------------
 * Burst mode (dynamic sizing).
 * ------------------------------------------------------------------ */

/* Enters burst mode: raises max_atoms to high_water_atoms so that
 * a full unconditioned scan can cache all rows without eviction.
 * The sweeper gradually shrinks back to base_max_atoms after
 * exit_burst(). */
void
ovsdb_row_cache_enter_burst(struct ovsdb_row_cache *cache)
{
    ovs_rwlock_wrlock(&cache->rwlock);
    cache->burst_mode = true;
    cache->max_atoms = cache->high_water_atoms;
    ovs_rwlock_unlock(&cache->rwlock);
}

/* Exits burst mode.  Does not immediately shrink max_atoms;
 * the sweeper thread contracts it gradually (10 percent per
 * cycle) so entries age out naturally via decay. */
void
ovsdb_row_cache_exit_burst(struct ovsdb_row_cache *cache)
{
    ovs_rwlock_wrlock(&cache->rwlock);
    cache->burst_mode = false;
    ovs_rwlock_unlock(&cache->rwlock);
}

/* Adjusts the base and high-water atom budgets at runtime.
 * Takes effect immediately; the sweeper will enforce the new
 * limits on its next cycle. */
void
ovsdb_row_cache_set_max_atoms(struct ovsdb_row_cache *cache,
                              size_t base, size_t high_water)
{
    ovs_rwlock_wrlock(&cache->rwlock);
    cache->base_max_atoms = base;
    cache->high_water_atoms = MIN(high_water, HIGH_WATER_CEILING);
    if (!cache->burst_mode) {
        cache->max_atoms = base;
    }
    ovs_rwlock_unlock(&cache->rwlock);
}

size_t
ovsdb_row_cache_base_max_atoms(const struct ovsdb_row_cache *cache)
{
    return cache->base_max_atoms;
}

/* ------------------------------------------------------------------
 * Sweeper lifecycle.
 * ------------------------------------------------------------------ */

/* Starts the background sweeper thread.  Must be called after
 * startup warmup completes.  Safe to call multiple times. */
void
ovsdb_row_cache_start_sweeper(struct ovsdb_row_cache *cache)
{
    if (!cache->sweeper_started) {
        cache->sweeper = ovs_thread_create(
            "cache-sweep",
            ovsdb_row_cache_sweeper_main__, cache);
        cache->sweeper_started = true;
    }
}

/* ------------------------------------------------------------------
 * Stats.
 * ------------------------------------------------------------------ */

/* Returns the total atom cost of all entries in 'cache'. */
size_t
ovsdb_row_cache_n_atoms(const struct ovsdb_row_cache *cache)
{
    size_t val;
    ovs_rwlock_rdlock(CONST_CAST(struct ovs_rwlock *, &cache->rwlock));
    val = cache->total_atoms;
    ovs_rwlock_unlock(CONST_CAST(struct ovs_rwlock *, &cache->rwlock));
    return val;
}

/* Returns the current dynamic atom budget of 'cache'.  This may
 * be higher than base during burst mode, or shrinking toward
 * base after burst ends. */
size_t
ovsdb_row_cache_max_atoms(const struct ovsdb_row_cache *cache)
{
    return cache->max_atoms;
}

/* Returns the number of entries in 'cache'. */
size_t
ovsdb_row_cache_count(const struct ovsdb_row_cache *cache)
{
    size_t val;
    ovs_rwlock_rdlock(CONST_CAST(struct ovs_rwlock *, &cache->rwlock));
    val = cache->n_entries;
    ovs_rwlock_unlock(CONST_CAST(struct ovs_rwlock *, &cache->rwlock));
    return val;
}

/* Returns the number of cache hits (successful lookups). */
size_t
ovsdb_row_cache_hits(const struct ovsdb_row_cache *cache)
{
    uint64_t val;
    atomic_read_relaxed(&cache->hits, &val);
    return (size_t) val;
}

/* Returns the number of cache misses (failed lookups). */
size_t
ovsdb_row_cache_misses(const struct ovsdb_row_cache *cache)
{
    uint64_t val;
    atomic_read_relaxed(&cache->misses, &val);
    return (size_t) val;
}

/* Returns the number of entries evicted from 'cache'. */
size_t
ovsdb_row_cache_evictions(const struct ovsdb_row_cache *cache)
{
    uint64_t val;
    atomic_read_relaxed(&cache->evictions, &val);
    return (size_t) val;
}

/* Returns the number of scan ring slot reuses. */
size_t
ovsdb_row_cache_scan_reuse(const struct ovsdb_row_cache *cache)
{
    size_t val;
    ovs_rwlock_rdlock(CONST_CAST(struct ovs_rwlock *, &cache->rwlock));
    val = cache->scan_ring.reuse_count;
    ovs_rwlock_unlock(CONST_CAST(struct ovs_rwlock *, &cache->rwlock));
    return val;
}

/* Fills 'histogram' with the count of entries at each usage_count
 * level (0 through MAX_USAGE_COUNT).  'histogram' must point to
 * an array of at least MAX_USAGE_COUNT+1 elements. */
void
ovsdb_row_cache_usage_histogram(const struct ovsdb_row_cache *cache,
                                size_t histogram[])
{
    struct ovsdb_row_cache_entry *entry;
    int i;

    /* Cast away const for rwlock — rdlock does not modify
     * logical state. */
    ovs_rwlock_rdlock(CONST_CAST(struct ovs_rwlock *, &cache->rwlock));

    for (i = 0; i <= OVSDB_ROW_CACHE_MAX_USAGE; i++) {
        histogram[i] = 0;
    }

    HMAP_FOR_EACH (entry, hmap_node, &cache->entries) {
        int uc;
        atomic_read_relaxed(&entry->usage_count, &uc);
        if (uc > OVSDB_ROW_CACHE_MAX_USAGE) {
            uc = OVSDB_ROW_CACHE_MAX_USAGE;
        }
        histogram[uc]++;
    }

    ovs_rwlock_unlock(CONST_CAST(struct ovs_rwlock *, &cache->rwlock));
}

/* ------------------------------------------------------------------
 * Lazy-load state (Phase 2).
 * ------------------------------------------------------------------ */

/* Returns the loading state of the row identified by 'uuid'.
 * Returns OVSDB_ROW_UNLOADED if the UUID is unknown. */
enum ovsdb_row_state
ovsdb_row_cache_get_state(struct ovsdb_row_cache *cache,
                          const struct uuid *uuid)
{
    struct ovsdb_row_cache_entry *entry;
    enum ovsdb_row_state result;

    ovs_rwlock_rdlock(&cache->rwlock);
    entry = ovsdb_row_cache_find__(cache, uuid);
    result = entry ? entry->state : OVSDB_ROW_UNLOADED;
    ovs_rwlock_unlock(&cache->rwlock);
    return result;
}

/* Sets the loading state of an existing cache entry for 'uuid'.
 * Does nothing if the UUID is not in the cache. */
void
ovsdb_row_cache_set_state(struct ovsdb_row_cache *cache,
                          const struct uuid *uuid,
                          enum ovsdb_row_state state)
{
    struct ovsdb_row_cache_entry *entry;

    ovs_rwlock_wrlock(&cache->rwlock);
    entry = ovsdb_row_cache_find__(cache, uuid);
    if (entry) {
        entry->state = state;
    }
    ovs_rwlock_unlock(&cache->rwlock);
}

/* Records a load failure for 'uuid'.  Returns the new state. */
enum ovsdb_row_state
ovsdb_row_cache_record_load_failure(struct ovsdb_row_cache *cache,
                                    const struct uuid *uuid)
{
    struct ovsdb_row_cache_entry *entry;
    enum ovsdb_row_state result;

    ovs_rwlock_wrlock(&cache->rwlock);
    entry = ovsdb_row_cache_find__(cache, uuid);
    if (!entry) {
        ovs_rwlock_unlock(&cache->rwlock);
        return OVSDB_ROW_UNLOADED;
    }
    if (entry->state == OVSDB_ROW_ERROR) {
        ovs_rwlock_unlock(&cache->rwlock);
        return OVSDB_ROW_ERROR;
    }

    entry->load_failures++;
    if (entry->load_failures >= OVSDB_MAX_LOAD_RETRIES) {
        static struct vlog_rate_limit rl = VLOG_RATE_LIMIT_INIT(5, 20);
        entry->state = OVSDB_ROW_ERROR;
        entry->retry_after = LLONG_MAX; /* Never retry. */
        VLOG_WARN_RL(&rl, "row "UUID_FMT" permanently failed "
                     "after %d load attempts",
                     UUID_ARGS(uuid), entry->load_failures);
    } else {
        /* Escalating backoff: 100ms, 500ms. */
        long long int backoff_ms = entry->load_failures == 1
                                   ? 100 : 500;
        entry->state = OVSDB_ROW_UNLOADED;
        entry->retry_after = time_msec() + backoff_ms;
    }
    result = entry->state;
    ovs_rwlock_unlock(&cache->rwlock);
    return result;
}

/* Returns true if 'uuid' is in UNLOADED state and the backoff
 * period has elapsed (ready for another load attempt). */
bool
ovsdb_row_cache_is_retry_ready(struct ovsdb_row_cache *cache,
                               const struct uuid *uuid)
{
    struct ovsdb_row_cache_entry *entry;
    bool result;

    ovs_rwlock_rdlock(&cache->rwlock);
    entry = ovsdb_row_cache_find__(cache, uuid);
    if (!entry || entry->state != OVSDB_ROW_UNLOADED) {
        result = false;
    } else {
        result = time_msec() >= entry->retry_after;
    }
    ovs_rwlock_unlock(&cache->rwlock);
    return result;
}

/* Returns true if the cache has any entries in UNLOADED or LOADING
 * state. */
bool
ovsdb_row_cache_has_unloaded(const struct ovsdb_row_cache *cache)
{
    struct ovsdb_row_cache_entry *entry;
    bool result = false;

    ovs_rwlock_rdlock(CONST_CAST(struct ovs_rwlock *, &cache->rwlock));
    HMAP_FOR_EACH (entry, hmap_node, &cache->entries) {
        if (entry->state == OVSDB_ROW_UNLOADED
            || entry->state == OVSDB_ROW_LOADING) {
            result = true;
            break;
        }
    }
    ovs_rwlock_unlock(CONST_CAST(struct ovs_rwlock *, &cache->rwlock));
    return result;
}

/* Calls 'cb' for each cache entry with state OVSDB_ROW_UNLOADED.
 * Does not modify entry state -- caller is responsible for
 * transitions.  Stops early if 'cb' returns false. */
void
ovsdb_row_cache_for_each_unloaded(
    struct ovsdb_row_cache *cache,
    bool (*cb)(const struct uuid *uuid, void *aux),
    void *aux)
{
    struct ovsdb_row_cache_entry *entry;

    ovs_rwlock_wrlock(&cache->rwlock);
    { int old_depth;
      atomic_add_relaxed(&cache->iterating, 1, &old_depth);
    }
    HMAP_FOR_EACH_SAFE (entry, hmap_node, &cache->entries) {
        if (entry->state == OVSDB_ROW_UNLOADED
            && time_msec() >= entry->retry_after) {
            if (!cb(&entry->uuid, aux)) {
                break;
            }
        }
    }
    { int old_depth;
      atomic_sub_relaxed(&cache->iterating, 1, &old_depth);
      if (old_depth == 1) {
          ovsdb_row_cache_sweep_deferred__(cache);
      }
    }
    ovs_rwlock_unlock(&cache->rwlock);
}

/* Calls 'cb' for each cache entry with state OVSDB_ROW_CACHED.
 * The row pointer passed to 'cb' is owned by the cache and remains
 * valid for the duration of the callback (and longer, as long as
 * the cache is not destroyed).
 *
 * Re-entrancy safe: callbacks may call insert/remove/evict without
 * corrupting the iteration -- evicted entries are deferred until
 * after the outermost iteration completes. */
void
ovsdb_row_cache_for_each_loaded(
    struct ovsdb_row_cache *cache,
    bool (*cb)(const struct ovsdb_row *row, void *aux),
    void *aux)
{
    struct ovsdb_row_cache_entry *entry;

    ovs_rwlock_wrlock(&cache->rwlock);
    { int old_depth;
      atomic_add_relaxed(&cache->iterating, 1, &old_depth);
    }
    HMAP_FOR_EACH_SAFE (entry, hmap_node, &cache->entries) {
        if (entry->state == OVSDB_ROW_CACHED && entry->row) {
            if (!cb(entry->row, aux)) {
                break;
            }
        }
    }
    { int old_depth;
      atomic_sub_relaxed(&cache->iterating, 1, &old_depth);
      if (old_depth == 1) {
          ovsdb_row_cache_sweep_deferred__(cache);
      }
    }
    ovs_rwlock_unlock(&cache->rwlock);
}

/* Registers a UUID in the cache as UNLOADED (no row data yet).
 * This is used at startup to populate the index from disk
 * without loading actual row data. */
void
ovsdb_row_cache_add_unloaded(struct ovsdb_row_cache *cache,
                             const struct uuid *uuid)
{
    struct ovsdb_row_cache_entry *entry;
    uint32_t slot;

    ovs_rwlock_wrlock(&cache->rwlock);
    entry = ovsdb_row_cache_find__(cache, uuid);
    if (entry) {
        ovs_rwlock_unlock(&cache->rwlock);
        return;
    }

    entry = xmalloc(sizeof *entry);
    entry->uuid = *uuid;
    entry->row = NULL;
    entry->n_atoms = 0;
    entry->pinned = false;
    entry->state = OVSDB_ROW_UNLOADED;
    entry->load_failures = 0;
    entry->retry_after = 0;
    atomic_init(&entry->usage_count, 0);
    entry->in_scan_ring = false;

    slot = ovsdb_row_cache_alloc_slot__(cache);
    entry->clock_slot = slot;
    cache->clock_buf[slot] = entry;
    cache->clock_len++;

    hmap_insert(&cache->entries, &entry->hmap_node,
                uuid_hash(uuid));
    cache->n_entries++;
    ovs_rwlock_unlock(&cache->rwlock);
}
