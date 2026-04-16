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

#include "openvswitch/hmap.h"
#include "openvswitch/list.h"
#include "openvswitch/vlog.h"
#include "row.h"
#include "uuid.h"
#include "util.h"

VLOG_DEFINE_THIS_MODULE(ovsdb_row_cache);

/* A single cached row, stored inside the cache's hmap and LRU list. */
struct ovsdb_row_cache_entry {
    struct hmap_node hmap_node;   /* In cache->entries, hashed by UUID. */
    struct ovs_list lru_node;     /* In cache->lru list. */
    struct uuid uuid;             /* Row UUID (copy for fast compare). */
    struct ovsdb_row *row;        /* Owned by the cache; NULL if not yet
                                   * loaded (UNLOADED or LOADING state). */
    size_t n_atoms;               /* Atom cost of this row. */
    bool pinned;                  /* If true, entry cannot be evicted. */
    enum ovsdb_row_state state;   /* Loading state (Phase 2). */
};

/* An LRU cache of ovsdb_row objects, bounded by a maximum atom count.
 *
 * Rows may be "pinned" to prevent eviction.  When the total atom count
 * exceeds 'max_atoms', the least-recently-used unpinned entry is
 * evicted.  If every remaining entry is pinned, eviction stops and the
 * cache is allowed to exceed the limit temporarily. */
struct ovsdb_row_cache {
    struct hmap entries;          /* Contains ovsdb_row_cache_entry. */
    struct ovs_list lru;          /* LRU list, least-recent at front. */
    size_t max_atoms;             /* Soft atom budget. */
    size_t total_atoms;           /* Sum of n_atoms for all entries. */
    size_t n_entries;             /* Number of entries in the cache. */
    size_t hits;                  /* Number of successful lookups. */
    size_t misses;                /* Number of failed lookups. */
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

/* Promotes 'entry' to the most-recently-used position (back of LRU). */
static void
ovsdb_row_cache_touch__(struct ovsdb_row_cache *cache,
                        struct ovsdb_row_cache_entry *entry)
{
    ovs_list_remove(&entry->lru_node);
    ovs_list_push_back(&cache->lru, &entry->lru_node);
}

/* Removes and frees 'entry', destroying the contained row (if any).
 * The caller is responsible for any additional bookkeeping.
 * Entries in UNLOADED/LOADING state may have row==NULL. */
static void
ovsdb_row_cache_evict_entry__(struct ovsdb_row_cache *cache,
                              struct ovsdb_row_cache_entry *entry)
{
    ovs_list_remove(&entry->lru_node);
    hmap_remove(&cache->entries, &entry->hmap_node);

    cache->total_atoms -= entry->n_atoms;
    cache->n_entries--;

    if (entry->row) {
        ovsdb_row_destroy(entry->row);
    }
    free(entry);
}

/* Evicts unpinned entries starting from the least-recently-used end
 * until total_atoms <= max_atoms or only pinned entries remain. */
static void
ovsdb_row_cache_evict__(struct ovsdb_row_cache *cache)
{
    struct ovsdb_row_cache_entry *entry;

    LIST_FOR_EACH_SAFE (entry, lru_node, &cache->lru) {
        if (cache->total_atoms <= cache->max_atoms) {
            break;
        }
        if (entry->pinned) {
            continue;
        }

        VLOG_DBG("evicting row "UUID_FMT" (%"PRIuSIZE" atoms)",
                 UUID_ARGS(&entry->uuid), entry->n_atoms);
        ovsdb_row_cache_evict_entry__(cache, entry);
    }
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
    hmap_init(&cache->entries);
    ovs_list_init(&cache->lru);
    cache->max_atoms = max_atoms;
    cache->total_atoms = 0;
    cache->n_entries = 0;
    cache->hits = 0;
    cache->misses = 0;

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

    HMAP_FOR_EACH_SAFE (entry, hmap_node, &cache->entries) {
        hmap_remove(&cache->entries, &entry->hmap_node);
        ovsdb_row_destroy(entry->row);
        free(entry);
    }

    hmap_destroy(&cache->entries);
    free(cache);
}

/* ------------------------------------------------------------------
 * Lookup.
 * ------------------------------------------------------------------ */

/* Looks up the cached row for 'uuid'.  Returns the row on a cache hit
 * (and promotes it to the most-recently-used position) or NULL on a
 * miss.  The returned row is still owned by the cache; the caller must
 * not free it. */
struct ovsdb_row *
ovsdb_row_cache_lookup(struct ovsdb_row_cache *cache,
                       const struct uuid *uuid)
{
    struct ovsdb_row_cache_entry *entry;

    entry = ovsdb_row_cache_find__(cache, uuid);
    if (entry) {
        ovsdb_row_cache_touch__(cache, entry);
        cache->hits++;
        return entry->row;
    }

    cache->misses++;
    return NULL;
}

/* ------------------------------------------------------------------
 * Insert / remove.
 * ------------------------------------------------------------------ */

/* Inserts 'row' into 'cache' with an atom cost of 'n_atoms'.
 *
 * If a row with the same UUID is already present, the old entry is
 * removed first (its row is destroyed).  The cache takes ownership of
 * 'row'.
 *
 * After insertion, unpinned entries are evicted from the
 * least-recently-used end until total_atoms <= max_atoms. */
void
ovsdb_row_cache_insert(struct ovsdb_row_cache *cache,
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

    hmap_insert(&cache->entries, &entry->hmap_node,
                uuid_hash(uuid));
    ovs_list_push_back(&cache->lru, &entry->lru_node);
    cache->total_atoms += n_atoms;
    cache->n_entries++;

    VLOG_DBG("inserted row "UUID_FMT" (%"PRIuSIZE" atoms, "
             "total=%"PRIuSIZE")",
             UUID_ARGS(uuid), n_atoms, cache->total_atoms);

    /* Evict if necessary. */
    ovsdb_row_cache_evict__(cache);
}

/* Removes the cached entry for 'uuid', destroying its row.
 * Does nothing if no such entry exists. */
void
ovsdb_row_cache_remove(struct ovsdb_row_cache *cache,
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

    entry = ovsdb_row_cache_find__(cache, uuid);
    if (entry) {
        if (entry->pinned) {
            entry->pinned = false;
            VLOG_DBG("unpinned row "UUID_FMT, UUID_ARGS(uuid));
            ovsdb_row_cache_evict__(cache);
        }
    } else {
        VLOG_DBG("unpin request for absent row "UUID_FMT,
                 UUID_ARGS(uuid));
    }
}

/* ------------------------------------------------------------------
 * Stats.
 * ------------------------------------------------------------------ */

/* Returns the total atom cost of all entries in 'cache'. */
size_t
ovsdb_row_cache_n_atoms(const struct ovsdb_row_cache *cache)
{
    return cache->total_atoms;
}

/* Returns the number of entries in 'cache'. */
size_t
ovsdb_row_cache_count(const struct ovsdb_row_cache *cache)
{
    return cache->n_entries;
}

/* Returns the number of cache hits (successful lookups). */
size_t
ovsdb_row_cache_hits(const struct ovsdb_row_cache *cache)
{
    return cache->hits;
}

/* Returns the number of cache misses (failed lookups). */
size_t
ovsdb_row_cache_misses(const struct ovsdb_row_cache *cache)
{
    return cache->misses;
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

    entry = ovsdb_row_cache_find__(cache, uuid);
    return entry ? entry->state : OVSDB_ROW_UNLOADED;
}

/* Sets the loading state of an existing cache entry for 'uuid'.
 * Does nothing if the UUID is not in the cache. */
void
ovsdb_row_cache_set_state(struct ovsdb_row_cache *cache,
                          const struct uuid *uuid,
                          enum ovsdb_row_state state)
{
    struct ovsdb_row_cache_entry *entry;

    entry = ovsdb_row_cache_find__(cache, uuid);
    if (entry) {
        entry->state = state;
    }
}

/* Returns true if the cache has any entries in UNLOADED or LOADING state. */
bool
ovsdb_row_cache_has_unloaded(const struct ovsdb_row_cache *cache)
{
    struct ovsdb_row_cache_entry *entry;

    HMAP_FOR_EACH (entry, hmap_node, &cache->entries) {
        if (entry->state == OVSDB_ROW_UNLOADED
            || entry->state == OVSDB_ROW_LOADING) {
            return true;
        }
    }
    return false;
}

/* Calls 'cb' for each cache entry with state OVSDB_ROW_UNLOADED.
 * Does not modify entry state — caller is responsible for transitions. */
void
ovsdb_row_cache_for_each_unloaded(
    struct ovsdb_row_cache *cache,
    void (*cb)(const struct uuid *uuid, void *aux),
    void *aux)
{
    struct ovsdb_row_cache_entry *entry;

    HMAP_FOR_EACH (entry, hmap_node, &cache->entries) {
        if (entry->state == OVSDB_ROW_UNLOADED) {
            cb(&entry->uuid, aux);
        }
    }
}

/* Calls 'cb' for each cache entry with state OVSDB_ROW_CACHED.
 * The row pointer passed to 'cb' is owned by the cache and remains
 * valid for the duration of the callback (and longer, as long as
 * no eviction or removal occurs).  Stops if 'cb' returns false. */
void
ovsdb_row_cache_for_each_loaded(
    struct ovsdb_row_cache *cache,
    bool (*cb)(const struct ovsdb_row *row, void *aux),
    void *aux)
{
    struct ovsdb_row_cache_entry *entry;

    HMAP_FOR_EACH (entry, hmap_node, &cache->entries) {
        if (entry->state == OVSDB_ROW_CACHED && entry->row) {
            if (!cb(entry->row, aux)) {
                break;
            }
        }
    }
}

/* Registers a UUID in the cache as UNLOADED (no row data yet).
 * This is used at startup to populate the index from disk
 * without loading actual row data. */
void
ovsdb_row_cache_add_unloaded(struct ovsdb_row_cache *cache,
                             const struct uuid *uuid)
{
    struct ovsdb_row_cache_entry *entry;

    entry = ovsdb_row_cache_find__(cache, uuid);
    if (entry) {
        return;
    }

    entry = xmalloc(sizeof *entry);
    entry->uuid = *uuid;
    entry->row = NULL;
    entry->n_atoms = 0;
    entry->pinned = false;
    entry->state = OVSDB_ROW_UNLOADED;

    hmap_insert(&cache->entries, &entry->hmap_node,
                uuid_hash(uuid));
    ovs_list_push_back(&cache->lru, &entry->lru_node);
    cache->n_entries++;
}
