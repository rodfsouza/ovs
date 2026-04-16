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

#ifndef OVSDB_ROW_CACHE_H
#define OVSDB_ROW_CACHE_H 1

#include <stddef.h>
#include "compiler.h"
#include "openvswitch/uuid.h"

struct ovsdb_row;
struct ovsdb_row_cache;

/* Row loading state for lazy-load (Phase 2). */
enum ovsdb_row_state {
    OVSDB_ROW_UNLOADED,   /* UUID known, data on disk only. */
    OVSDB_ROW_LOADING,    /* Load job submitted to worker pool. */
    OVSDB_ROW_CACHED,     /* Data in memory. */
};

/* Lifecycle. */
struct ovsdb_row_cache *ovsdb_row_cache_create(size_t max_atoms);
void ovsdb_row_cache_destroy(struct ovsdb_row_cache *);

/* Lookup (returns NULL on miss). */
struct ovsdb_row *ovsdb_row_cache_lookup(struct ovsdb_row_cache *,
                                         const struct uuid *);

/* Insert/update (may trigger eviction). */
void ovsdb_row_cache_insert(struct ovsdb_row_cache *,
                            struct ovsdb_row *, size_t n_atoms);
void ovsdb_row_cache_remove(struct ovsdb_row_cache *,
                            const struct uuid *);

/* Pinning (prevents eviction). */
void ovsdb_row_cache_pin(struct ovsdb_row_cache *,
                         const struct uuid *);
void ovsdb_row_cache_unpin(struct ovsdb_row_cache *,
                           const struct uuid *);

/* Lazy-load state (Phase 2). */
enum ovsdb_row_state ovsdb_row_cache_get_state(
    struct ovsdb_row_cache *, const struct uuid *);
void ovsdb_row_cache_set_state(struct ovsdb_row_cache *,
                               const struct uuid *,
                               enum ovsdb_row_state);
void ovsdb_row_cache_add_unloaded(struct ovsdb_row_cache *,
                                  const struct uuid *);

/* Returns true if the cache has any entries in UNLOADED or LOADING state. */
bool ovsdb_row_cache_has_unloaded(const struct ovsdb_row_cache *);

/* Calls 'cb' for each cache entry with state OVSDB_ROW_UNLOADED.
 * Does not modify entry state — caller is responsible for transitions. */
void ovsdb_row_cache_for_each_unloaded(
    struct ovsdb_row_cache *,
    void (*cb)(const struct uuid *uuid, void *aux),
    void *aux);

/* Stats. */
size_t ovsdb_row_cache_n_atoms(const struct ovsdb_row_cache *);
size_t ovsdb_row_cache_count(const struct ovsdb_row_cache *);
size_t ovsdb_row_cache_hits(const struct ovsdb_row_cache *);
size_t ovsdb_row_cache_misses(const struct ovsdb_row_cache *);

#endif /* ovsdb/row-cache.h */
