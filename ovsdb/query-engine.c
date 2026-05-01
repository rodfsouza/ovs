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

#include "query-engine.h"

#include "column.h"
#include "condition.h"
#include "disk-store.h"
#include "index-engine.h"
#include "row.h"
#include "row-cache.h"
#include "storage-engine.h"
#include "table.h"
#include "openvswitch/dynamic-string.h"
#include "openvswitch/vlog.h"
#include "util.h"

VLOG_DEFINE_THIS_MODULE(ovsdb_query_engine);

/* ------------------------------------------------------------------ */
/* Result iterator internals.                                          */
/* ------------------------------------------------------------------ */

struct ovsdb_query_result {
    enum ovsdb_plan_type type;

    /* For POINT/INDEX_LOOKUP: single row (owned). */
    struct ovsdb_row *single_row;
    bool single_consumed;

    /* For FULL_SCAN: cursor + filter. */
    struct ovsdb_storage_cursor *cursor;
    struct ovsdb_storage_engine *storage;
    struct ovsdb_table *table;
    const struct ovsdb_condition *condition;
    struct ovsdb_row_cache *cache;

    /* Current row for FULL_SCAN (owned by result until next call). */
    struct ovsdb_row *current_row;
};

/* ------------------------------------------------------------------ */
/* Planning.                                                           */
/* ------------------------------------------------------------------ */

struct ovsdb_execution_plan *
ovsdb_query_engine_plan(const struct ovsdb_condition *cond,
                         const struct ovsdb_index_set *indexes,
                         const struct ovsdb_table *table OVS_UNUSED)
{
    struct ovsdb_execution_plan *plan = xzalloc(sizeof *plan);

    /* No condition or trivially true → full scan. */
    if (!cond || ovsdb_condition_is_true(cond)) {
        plan->type = OVSDB_PLAN_FULL_SCAN;
        plan->condition = NULL;
        plan->use_cache = false;
        return plan;
    }

    /* UUID exact match → point lookup. */
    if (cond->n_clauses > 0
        && cond->clauses[0].column->index == OVSDB_COL_UUID
        && cond->clauses[0].function == OVSDB_F_EQ) {
        plan->type = OVSDB_PLAN_POINT_LOOKUP;
        plan->target_uuid = cond->clauses[0].arg.keys[0].uuid;
        return plan;
    }

    /* Check for indexed column EQ match. */
    if (indexes) {
        size_t i;

        for (i = 0; i < cond->n_clauses; i++) {
            const struct ovsdb_index *idx;

            if (cond->clauses[i].function != OVSDB_F_EQ) {
                continue;
            }
            idx = ovsdb_index_set_find_for_column(
                indexes, cond->clauses[i].column->name);
            if (idx) {
                plan->type = OVSDB_PLAN_INDEX_LOOKUP;
                plan->index = idx;
                ovsdb_atom_clone(&plan->key,
                                  &cond->clauses[i].arg.keys[0],
                                  cond->clauses[i].column->type.key.type);
                plan->key_type = cond->clauses[i].column->type.key.type;
                plan->key_valid = true;
                return plan;
            }
        }
    }

    /* No index available → full scan with condition filter. */
    plan->type = OVSDB_PLAN_FULL_SCAN;
    plan->condition = cond;
    plan->use_cache = true;
    return plan;
}

void
ovsdb_execution_plan_destroy(struct ovsdb_execution_plan *plan)
{
    if (plan) {
        if (plan->key_valid) {
            ovsdb_atom_destroy(&plan->key, plan->key_type);
        }
        free(plan);
    }
}

/* ------------------------------------------------------------------ */
/* EXPLAIN.                                                            */
/* ------------------------------------------------------------------ */

char *
ovsdb_execution_plan_describe(const struct ovsdb_execution_plan *plan)
{
    struct ds s = DS_EMPTY_INITIALIZER;

    switch (plan->type) {
    case OVSDB_PLAN_POINT_LOOKUP:
        ds_put_format(&s, "POINT_LOOKUP uuid=" UUID_FMT,
                      UUID_ARGS(&plan->target_uuid));
        break;

    case OVSDB_PLAN_INDEX_LOOKUP:
        ds_put_format(&s, "INDEX_LOOKUP via \"%s\"",
                      ovsdb_index_get_name(plan->index));
        break;

    case OVSDB_PLAN_FULL_SCAN:
        if (plan->condition) {
            ds_put_cstr(&s, "FULL_SCAN with condition filter");
        } else {
            ds_put_cstr(&s, "FULL_SCAN (unconditioned)");
        }
        ds_put_format(&s, ", cache=%s",
                      plan->use_cache ? "yes" : "no");
        break;
    }

    return ds_steal_cstr(&s);
}

/* ------------------------------------------------------------------ */
/* Execution.                                                          */
/* ------------------------------------------------------------------ */

static struct ovsdb_query_result *
make_single_result(struct ovsdb_row *row)
{
    struct ovsdb_query_result *result = xzalloc(sizeof *result);
    result->type = OVSDB_PLAN_POINT_LOOKUP;
    result->single_row = row;
    result->single_consumed = false;
    return result;
}

static struct ovsdb_query_result *
make_empty_result(void)
{
    struct ovsdb_query_result *result = xzalloc(sizeof *result);
    result->type = OVSDB_PLAN_POINT_LOOKUP;
    result->single_row = NULL;
    result->single_consumed = true;
    return result;
}

static struct ovsdb_query_result *
execute_point_lookup(const struct ovsdb_execution_plan *plan,
                     struct ovsdb_storage_engine *storage,
                     struct ovsdb_index_set *indexes,
                     struct ovsdb_row_cache *cache,
                     struct ovsdb_table *table)
{
    const struct ovsdb_index *bloom;
    const struct ovsdb_row *cached;
    struct ovsdb_row *row;

    /* Bloom filter fast negative. */
    if (indexes) {
        bloom = ovsdb_index_set_find_bloom(indexes);
        if (bloom && !ovsdb_index_contains(bloom, &plan->target_uuid)) {
            VLOG_DBG("EXPLAIN: POINT_LOOKUP bloom negative for " UUID_FMT,
                     UUID_ARGS(&plan->target_uuid));
            return make_empty_result();
        }
    }

    /* Cache hit? */
    if (cache) {
        cached = ovsdb_row_cache_lookup(cache, &plan->target_uuid);
        if (cached) {
            VLOG_DBG("EXPLAIN: POINT_LOOKUP cache hit for " UUID_FMT,
                     UUID_ARGS(&plan->target_uuid));
            /* Return the cached row — we don't own it, but the
             * result iterator contract says pointer valid until
             * next/close, and cached rows are stable. */
            struct ovsdb_query_result *result = xzalloc(sizeof *result);
            result->type = OVSDB_PLAN_POINT_LOOKUP;
            result->single_row = NULL; /* Not owned. */
            result->single_consumed = false;
            /* Store as non-owned pointer via a cast — the caller
             * must not destroy it. */
            result->current_row = CONST_CAST(struct ovsdb_row *, cached);
            return result;
        }
    }

    /* Disk read. */
    row = ovsdb_storage_engine_read_row(storage, table, &plan->target_uuid);
    if (!row) {
        VLOG_DBG("EXPLAIN: POINT_LOOKUP not found on disk for " UUID_FMT,
                 UUID_ARGS(&plan->target_uuid));
        return make_empty_result();
    }

    VLOG_DBG("EXPLAIN: POINT_LOOKUP disk read for " UUID_FMT,
             UUID_ARGS(&plan->target_uuid));

    /* Populate cache. */
    if (cache) {
        size_t n_atoms = ovsdb_row_count_atoms(row);
        ovsdb_row_cache_insert(cache, row, n_atoms);
        /* Cache owns the row now.  Return pointer to it. */
        cached = ovsdb_row_cache_lookup(cache, &plan->target_uuid);
        if (cached) {
            struct ovsdb_query_result *result = xzalloc(sizeof *result);
            result->type = OVSDB_PLAN_POINT_LOOKUP;
            result->single_row = NULL;
            result->single_consumed = false;
            result->current_row = CONST_CAST(struct ovsdb_row *, cached);
            return result;
        }
    }

    return make_single_result(row);
}

static struct ovsdb_query_result *
execute_index_lookup(const struct ovsdb_execution_plan *plan,
                     struct ovsdb_storage_engine *storage,
                     struct ovsdb_row_cache *cache,
                     struct ovsdb_table *table)
{
    struct disk_store_index_entry *entry;
    const struct ovsdb_row *cached;
    struct ovsdb_row *row;

    entry = ovsdb_index_lookup(plan->index, &plan->key);
    if (!entry || entry->deleted) {
        VLOG_DBG("EXPLAIN: INDEX_LOOKUP not found via \"%s\"",
                 ovsdb_index_get_name(plan->index));
        return make_empty_result();
    }

    /* Cache hit? */
    if (cache) {
        cached = ovsdb_row_cache_lookup(cache, &entry->uuid);
        if (cached) {
            VLOG_DBG("EXPLAIN: INDEX_LOOKUP cache hit via \"%s\"",
                     ovsdb_index_get_name(plan->index));
            struct ovsdb_query_result *result = xzalloc(sizeof *result);
            result->type = OVSDB_PLAN_INDEX_LOOKUP;
            result->single_row = NULL;
            result->single_consumed = false;
            result->current_row = CONST_CAST(struct ovsdb_row *, cached);
            return result;
        }
    }

    /* Disk read via clustered entry. */
    row = ovsdb_storage_engine_read_row(storage, table, &entry->uuid);
    if (!row) {
        return make_empty_result();
    }

    VLOG_DBG("EXPLAIN: INDEX_LOOKUP disk read via \"%s\" → " UUID_FMT,
             ovsdb_index_get_name(plan->index), UUID_ARGS(&entry->uuid));

    /* Populate cache. */
    if (cache) {
        size_t n_atoms = ovsdb_row_count_atoms(row);
        ovsdb_row_cache_insert(cache, row, n_atoms);
        cached = ovsdb_row_cache_lookup(cache, &entry->uuid);
        if (cached) {
            struct ovsdb_query_result *result = xzalloc(sizeof *result);
            result->type = OVSDB_PLAN_INDEX_LOOKUP;
            result->single_row = NULL;
            result->single_consumed = false;
            result->current_row = CONST_CAST(struct ovsdb_row *, cached);
            return result;
        }
    }

    return make_single_result(row);
}

static struct ovsdb_query_result *
execute_full_scan(const struct ovsdb_execution_plan *plan,
                  struct ovsdb_storage_engine *storage,
                  struct ovsdb_row_cache *cache,
                  struct ovsdb_table *table)
{
    struct ovsdb_query_result *result = xzalloc(sizeof *result);

    result->type = OVSDB_PLAN_FULL_SCAN;
    result->cursor = ovsdb_storage_engine_cursor_open(
        storage, table->schema->name);
    result->storage = storage;
    result->table = table;
    result->condition = plan->condition;
    result->cache = plan->use_cache ? cache : NULL;
    result->current_row = NULL;

    VLOG_DBG("EXPLAIN: FULL_SCAN on table %s, cache=%s",
             table->schema->name, plan->use_cache ? "yes" : "no");

    return result;
}

struct ovsdb_query_result *
ovsdb_query_engine_execute(const struct ovsdb_execution_plan *plan,
                            struct ovsdb_storage_engine *storage,
                            struct ovsdb_index_set *indexes,
                            struct ovsdb_row_cache *cache,
                            struct ovsdb_table *table)
{
    char *desc = ovsdb_execution_plan_describe(plan);
    VLOG_DBG("query-engine: executing %s", desc);
    free(desc);

    switch (plan->type) {
    case OVSDB_PLAN_POINT_LOOKUP:
        return execute_point_lookup(plan, storage, indexes, cache, table);

    case OVSDB_PLAN_INDEX_LOOKUP:
        return execute_index_lookup(plan, storage, cache, table);

    case OVSDB_PLAN_FULL_SCAN:
        return execute_full_scan(plan, storage, cache, table);
    }

    OVS_NOT_REACHED();
}

/* ------------------------------------------------------------------ */
/* Result iteration.                                                   */
/* ------------------------------------------------------------------ */

const struct ovsdb_row *
ovsdb_query_result_next(struct ovsdb_query_result *result)
{
    switch (result->type) {
    case OVSDB_PLAN_POINT_LOOKUP:
    case OVSDB_PLAN_INDEX_LOOKUP:
        if (result->single_consumed) {
            return NULL;
        }
        result->single_consumed = true;
        if (result->current_row) {
            return result->current_row;  /* Cached (not owned). */
        }
        return result->single_row;       /* Owned by result. */

    case OVSDB_PLAN_FULL_SCAN:
        /* Destroy previous row if owned. */
        if (result->current_row) {
            ovsdb_row_destroy(result->current_row);
            result->current_row = NULL;
        }

        if (!result->cursor) {
            return NULL;
        }

        while (true) {
            struct ovsdb_row *row;

            row = ovsdb_storage_engine_cursor_next(result->cursor,
                                                    result->table);
            if (!row) {
                return NULL;  /* End of table. */
            }

            /* Apply condition filter. */
            if (result->condition
                && !ovsdb_condition_match_every_clause(
                       row, result->condition)) {
                ovsdb_row_destroy(row);
                continue;
            }

            /* Optionally cache. */
            if (result->cache) {
                size_t n_atoms = ovsdb_row_count_atoms(row);
                ovsdb_row_cache_insert(result->cache, row, n_atoms);
                /* Cache owns the row.  Lookup to get stable pointer. */
                const struct ovsdb_row *cached =
                    ovsdb_row_cache_lookup(
                        result->cache, ovsdb_row_get_uuid(row));
                if (cached) {
                    result->current_row = NULL;
                    return cached;
                }
                /* Cache evicted it immediately (over budget).
                 * Fall through and return the row we have. */
            }

            result->current_row = row;
            return row;
        }
    }

    OVS_NOT_REACHED();
}

struct ovsdb_row *
ovsdb_query_result_steal_row(struct ovsdb_query_result *result)
{
    struct ovsdb_row *row;

    switch (result->type) {
    case OVSDB_PLAN_POINT_LOOKUP:
    case OVSDB_PLAN_INDEX_LOOKUP:
        if (result->single_row) {
            row = result->single_row;
            result->single_row = NULL;  /* Transfer ownership. */
            return row;
        }
        if (result->current_row) {
            /* Cached row — not owned by result.  Clone it so the
             * caller has a stable owned copy. */
            return ovsdb_row_clone(result->current_row);
        }
        return NULL;

    case OVSDB_PLAN_FULL_SCAN:
        if (result->current_row) {
            row = result->current_row;
            result->current_row = NULL;
            return row;
        }
        return NULL;
    }

    OVS_NOT_REACHED();
}

void
ovsdb_query_result_close(struct ovsdb_query_result *result)
{
    if (!result) {
        return;
    }

    switch (result->type) {
    case OVSDB_PLAN_POINT_LOOKUP:
    case OVSDB_PLAN_INDEX_LOOKUP:
        if (result->single_row) {
            ovsdb_row_destroy(result->single_row);
        }
        /* current_row for cached lookups is NOT owned. */
        break;

    case OVSDB_PLAN_FULL_SCAN:
        if (result->current_row) {
            ovsdb_row_destroy(result->current_row);
        }
        if (result->cursor) {
            ovsdb_storage_engine_cursor_close(result->cursor);
        }
        break;
    }

    free(result);
}

/* ------------------------------------------------------------------ */
/* Convenience: direct UUID lookup (no plan/condition overhead).       */
/* ------------------------------------------------------------------ */

const struct ovsdb_row *
ovsdb_query_engine_lookup_uuid(struct ovsdb_storage_engine *storage,
                                struct ovsdb_index_set *indexes,
                                struct ovsdb_row_cache *cache,
                                struct ovsdb_table *table,
                                const struct uuid *uuid)
{
    const struct ovsdb_index *bloom;
    const struct ovsdb_row *cached;
    struct ovsdb_row *row;

    /* Bloom filter fast negative. */
    bloom = ovsdb_index_set_find_bloom(indexes);
    if (bloom && !ovsdb_index_contains(bloom, uuid)) {
        return NULL;
    }

    /* Cache hit? */
    if (cache) {
        cached = ovsdb_row_cache_lookup(cache, uuid);
        if (cached) {
            return cached;
        }
    }

    /* Disk read. */
    row = ovsdb_storage_engine_read_row(storage, table, uuid);
    if (!row) {
        return NULL;
    }

    /* Populate cache.  Cache takes ownership of row. */
    if (cache) {
        size_t n_atoms = ovsdb_row_count_atoms(row);
        ovsdb_row_cache_insert(cache, row, n_atoms);
        /* Return the cached pointer (stable). */
        cached = ovsdb_row_cache_lookup(cache, uuid);
        if (cached) {
            return cached;
        }
        /* Cache evicted immediately — row was destroyed by insert.
         * This shouldn't happen for a single row, but handle it. */
        return NULL;
    }

    /* No cache — the caller gets the row but has no way to own it.
     * This path shouldn't be reached in disk-store mode (cache
     * is always present).  Leak the row rather than crash. */
    return row;
}
