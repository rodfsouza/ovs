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

#ifndef OVSDB_LAZY_LOAD_H
#define OVSDB_LAZY_LOAD_H 1

#include <stdbool.h>
#include "openvswitch/uuid.h"

struct ovsdb;
struct ovsdb_table;
struct ovsdb_worker_pool;

/* Initialize the lazy-load subsystem with the given worker pool.
 * Must be called once at startup before any lazy loads occur. */
void ovsdb_lazy_load_init(struct ovsdb_worker_pool *);

/* Destroy the lazy-load subsystem.  Called at shutdown. */
void ovsdb_lazy_load_destroy(void);

/* Submits an async row load for 'uuid' in 'table'.
 * When the load completes, the row is inserted into the table's
 * cache and db->run_triggers is set to wake parked triggers.
 *
 * Returns true if a load job was submitted.
 * Returns false if the pool is unavailable (caller should
 * fall back to synchronous loading). */
bool ovsdb_lazy_load_request(struct ovsdb *db,
                             struct ovsdb_table *table,
                             const struct uuid *uuid);

/* Submits async load requests for ALL UNLOADED rows in 'table'.
 * Returns the number of load jobs submitted (0 if all already
 * loaded or no worker pool available). */
size_t ovsdb_lazy_load_bulk_request(struct ovsdb *db,
                                    struct ovsdb_table *table);

/* Submits async load requests for UNLOADED rows in 'table' until
 * doing so would fill the cache's atom budget
 * (ovsdb_row_cache_max_atoms()).  Intended for startup cache
 * warm-up on disk-store databases: for DBs that fit in the cache
 * this loads every row; for DBs that exceed the budget it stops
 * before load-and-evict thrashing begins.  Remaining UNLOADED rows
 * are read on demand by ovsdb_table_for_each_row_from_disk() or
 * ovsdb_table_get_row().
 *
 * Returns the number of load jobs submitted (0 if no worker pool,
 * cache full, or nothing UNLOADED). */
size_t ovsdb_lazy_load_bulk_request_until_full(struct ovsdb *db,
                                               struct ovsdb_table *table);

/* Returns true if the worker pool has pending or in-flight load
 * jobs.  Used by the trigger subsystem to decide whether a failed
 * transaction should be parked (rows may still be loading) or
 * completed as a real error (no loads in flight). */
bool ovsdb_lazy_load_has_pending(void);

/* Returns true if the lazy-load worker pool is available (i.e.
 * ovsdb_lazy_load_init() was called with a non-NULL pool).  Callers
 * use this to choose between the async deferred path and a
 * synchronous fallback. */
bool ovsdb_lazy_load_pool_available(void);
struct ovsdb_worker_pool *ovsdb_lazy_load_get_pool(void);

#endif /* ovsdb/lazy-load.h */
