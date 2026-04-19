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
#include "openvswitch/hmap.h"
#include "openvswitch/uuid.h"
#include "openvswitch/vlog.h"
#include "ovsdb.h"
#include "row.h"
#include "row-cache.h"
#include "table.h"
#include "uuid.h"
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
 *
 * Reads a single row from the disk store.  Thread safety:
 *   - pread() is used for disk I/O (thread-safe, no shared offset).
 *   - table->disk_store and table->schema are read-only after
 *     startup and are never modified while the server is running.
 *     Schema changes (ovsdb_replace) disconnect all clients first,
 *     draining all in-flight triggers and loads.
 *   - ovsdb_row_create() accesses table->schema->columns which is
 *     immutable after ovsdb_create(). */
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

/* Returns true if 'uuid' is present in the table->rows hmap.
 * Caller must hold no locks; runs on the main thread. */
static bool
row_in_table_rows(const struct ovsdb_table *table,
                  const struct uuid *uuid)
{
    const struct ovsdb_row *r;

    HMAP_FOR_EACH_WITH_HASH (r, hmap_node, uuid_hash(uuid),
                             &table->rows) {
        if (uuid_equals(ovsdb_row_get_uuid(r), uuid)) {
            return true;
        }
    }
    return false;
}

/* Done callback: runs on the main thread during
 * ovsdb_worker_pool_run().
 *
 * Inserts the loaded row into the table's cache and signals
 * the trigger subsystem to retry parked triggers.
 *
 * Concurrency note: while this load was in flight, a transaction
 * may have committed a newer version of the same UUID into
 * table->rows.  In that case the loaded copy is stale and is
 * discarded; table->rows takes precedence (matching the get_row
 * lookup order). */
static void
row_load_done(void *result, void *aux)
{
    struct ovsdb_row *row = result;
    struct row_load_request *req = aux;

    if (row) {
        if (row_in_table_rows(req->table, &req->uuid)) {
            /* Newer version exists in table->rows — discard load. */
            ovsdb_row_destroy(row);
            ovsdb_row_cache_remove(req->table->cache, &req->uuid);
            VLOG_DBG("lazy-load: discarded stale load of "UUID_FMT
                     " (newer version in table->rows)",
                     UUID_ARGS(&req->uuid));
        } else {
            size_t n_atoms = ovsdb_row_count_atoms(row);

            /* Insert sets state to OVSDB_ROW_CACHED automatically. */
            ovsdb_row_cache_insert(req->table->cache, row,
                                   n_atoms);

            VLOG_DBG("lazy-load: loaded row "UUID_FMT
                     " (%"PRIuSIZE" atoms)",
                     UUID_ARGS(&req->uuid), n_atoms);
        }

        /* Wake the trigger subsystem so parked triggers that were
         * waiting for this row get retried. */
        req->db->run_triggers = true;
        req->db->run_triggers_now = true;
    } else {
        static struct vlog_rate_limit rl = VLOG_RATE_LIMIT_INIT(5, 20);
        enum ovsdb_row_state new_state;

        VLOG_WARN_RL(&rl, "lazy-load: failed to load row "UUID_FMT,
                     UUID_ARGS(&req->uuid));
        /* Record failure; transitions to ERROR after max retries. */
        new_state = ovsdb_row_cache_record_load_failure(
            req->table->cache, &req->uuid);
        (void) new_state;

        /* Always wake triggers — either for retry (UNLOADED) or
         * so the parked trigger sees the row as absent (ERROR)
         * and can complete its response to the client. */
        req->db->run_triggers = true;
        req->db->run_triggers_now = true;
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

/* Submits an async row load for 'uuid' in 'table'.  On success,
 * transitions the cache entry's state from UNLOADED to LOADING so
 * callers do not need to do so themselves (single source of truth).
 * Returns false if the worker pool is unavailable. */
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

    /* Mark the cache entry as LOADING so concurrent get_row() and
     * bulk_request() calls don't resubmit the same UUID. */
    if (table->cache) {
        ovsdb_row_cache_set_state(table->cache, uuid, OVSDB_ROW_LOADING);
    }

    return true;
}

/* Callback context for bulk load. */
struct bulk_load_aux {
    struct ovsdb *db;
    struct ovsdb_table *table;
    size_t n_submitted;
};

static bool
bulk_load_cb(const struct uuid *uuid, void *aux_)
{
    struct bulk_load_aux *aux = aux_;

    /* ovsdb_lazy_load_request() transitions state to LOADING on
     * success, so we don't need to do it here. */
    if (ovsdb_lazy_load_request(aux->db, aux->table, uuid)) {
        aux->n_submitted++;
    }
    return true;  /* Continue iterating. */
}

size_t
ovsdb_lazy_load_bulk_request(struct ovsdb *db,
                             struct ovsdb_table *table)
{
    if (!lazy_pool || !table->cache) {
        return 0;
    }

    struct bulk_load_aux aux = {
        .db = db,
        .table = table,
        .n_submitted = 0,
    };
    ovsdb_row_cache_for_each_unloaded(table->cache, bulk_load_cb, &aux);

    if (aux.n_submitted) {
        VLOG_DBG("lazy-load: submitted %"PRIuSIZE" bulk load jobs for "
                 "table %s", aux.n_submitted, table->schema->name);
    }
    return aux.n_submitted;
}

/* Rough estimate of how many atoms a single row occupies.  Used by
 * the bounded warm-up to decide when to stop submitting loads.  Set
 * conservatively low so we tend to fill rather than under-fill; LRU
 * eviction on actual insert handles the real cap. */
#define OVSDB_WARMUP_AVG_ATOMS_PER_ROW 16

/* Callback context for bounded bulk load. */
struct bounded_bulk_load_aux {
    struct ovsdb *db;
    struct ovsdb_table *table;
    size_t n_submitted;
    size_t max_submissions;  /* Cap, computed from cache budget. */
};

static bool
bounded_bulk_load_cb(const struct uuid *uuid, void *aux_)
{
    struct bounded_bulk_load_aux *aux = aux_;

    if (aux->n_submitted >= aux->max_submissions) {
        return false;  /* Cache would overflow; stop iterating. */
    }
    if (ovsdb_lazy_load_request(aux->db, aux->table, uuid)) {
        aux->n_submitted++;
    }
    return true;
}

size_t
ovsdb_lazy_load_bulk_request_until_full(struct ovsdb *db,
                                        struct ovsdb_table *table)
{
    if (!lazy_pool || !table->cache) {
        return 0;
    }

    size_t max_atoms = ovsdb_row_cache_max_atoms(table->cache);
    size_t total_atoms = ovsdb_row_cache_n_atoms(table->cache);

    if (total_atoms >= max_atoms) {
        return 0;  /* Cache already at budget. */
    }

    size_t remaining_atoms = max_atoms - total_atoms;
    size_t max_submissions = remaining_atoms / OVSDB_WARMUP_AVG_ATOMS_PER_ROW;

    if (max_submissions == 0) {
        /* Budget too tight even for a single estimated row; submit
         * one job anyway so tiny caches aren't completely cold. */
        max_submissions = 1;
    }

    struct bounded_bulk_load_aux aux = {
        .db = db,
        .table = table,
        .n_submitted = 0,
        .max_submissions = max_submissions,
    };
    ovsdb_row_cache_for_each_unloaded(table->cache, bounded_bulk_load_cb,
                                      &aux);

    if (aux.n_submitted) {
        VLOG_DBG("lazy-load: warm-up submitted %"PRIuSIZE" bulk load jobs "
                 "for table %s (cap=%"PRIuSIZE", budget=%"PRIuSIZE"/%"
                 PRIuSIZE" atoms)",
                 aux.n_submitted, table->schema->name, max_submissions,
                 total_atoms, max_atoms);
    }
    return aux.n_submitted;
}

bool
ovsdb_lazy_load_has_pending(void)
{
    return lazy_pool && ovsdb_worker_pool_has_pending(lazy_pool);
}

bool
ovsdb_lazy_load_pool_available(void)
{
    return lazy_pool != NULL;
}
