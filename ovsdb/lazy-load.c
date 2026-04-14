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

#include "lazy-load.h"

#include "disk-store.h"
#include "openvswitch/vlog.h"
#include "ovsdb.h"
#include "row.h"
#include "row-cache.h"
#include "table.h"
#include "worker-pool.h"
#include "util.h"

VLOG_DEFINE_THIS_MODULE(ovsdb_lazy_load);

/* Global worker pool reference, set by ovsdb_lazy_load_init(). */
static struct ovsdb_worker_pool *lazy_pool;

/* Request structure passed from the main thread to a worker
 * and back to the done callback. */
struct row_load_request {
    struct ovsdb *db;
    struct ovsdb_table *table;
    struct uuid uuid;
};

/* Worker function: runs in a background thread.
 * Reads a single row from the disk store.  The disk store's
 * pread() is thread-safe (each call uses its own fd offset
 * via pread, not lseek+read). */
static void *
row_load_worker(void *arg)
{
    struct row_load_request *req = arg;
    struct ovsdb_row *row;

    row = ovsdb_disk_store_read_row(req->table->disk_store,
                                    req->table,
                                    &req->uuid);
    return row;
}

/* Done callback: runs on the main thread during
 * ovsdb_worker_pool_run().
 *
 * Inserts the loaded row into the table's cache and signals
 * the trigger subsystem to retry parked triggers. */
static void
row_load_done(void *result, void *aux)
{
    struct ovsdb_row *row = result;
    struct row_load_request *req = aux;

    if (row) {
        size_t n_atoms = ovsdb_row_count_atoms(row);

        ovsdb_row_cache_insert(req->table->cache, row,
                               n_atoms);
        ovsdb_row_cache_set_state(req->table->cache,
                                  &req->uuid,
                                  OVSDB_ROW_CACHED);

        /* Wake the trigger subsystem so parked triggers
         * that were waiting for this row get retried. */
        req->db->run_triggers = true;
        req->db->run_triggers_now = true;

        VLOG_DBG("lazy-load: loaded row "UUID_FMT
                 " (%"PRIuSIZE" atoms)",
                 UUID_ARGS(&req->uuid), n_atoms);
    } else {
        VLOG_WARN("lazy-load: failed to load row "UUID_FMT,
                  UUID_ARGS(&req->uuid));
        /* Mark as UNLOADED so it can be retried. */
        ovsdb_row_cache_set_state(req->table->cache,
                                  &req->uuid,
                                  OVSDB_ROW_UNLOADED);
    }

    free(req);
}

void
ovsdb_lazy_load_init(struct ovsdb_worker_pool *pool)
{
    lazy_pool = pool;
}

void
ovsdb_lazy_load_destroy(void)
{
    lazy_pool = NULL;
}

bool
ovsdb_lazy_load_request(struct ovsdb *db,
                        struct ovsdb_table *table,
                        const struct uuid *uuid)
{
    struct row_load_request *req;

    if (!lazy_pool) {
        return false;
    }

    req = xmalloc(sizeof *req);
    req->db = db;
    req->table = table;
    req->uuid = *uuid;

    ovsdb_worker_pool_submit(lazy_pool,
                             row_load_worker, req,
                             row_load_done, req);
    return true;
}
