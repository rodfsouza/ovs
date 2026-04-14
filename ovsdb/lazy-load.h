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

/* Returns true if the worker pool has pending or in-flight load
 * jobs.  Used by the trigger subsystem to decide whether a failed
 * transaction should be parked (rows may still be loading) or
 * completed as a real error (no loads in flight). */
bool ovsdb_lazy_load_has_pending(void);

#endif /* ovsdb/lazy-load.h */
