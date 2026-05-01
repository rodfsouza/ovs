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

#ifndef OVSDB_QUERY_ENGINE_H
#define OVSDB_QUERY_ENGINE_H 1

#include <stdbool.h>
#include <stddef.h>
#include "openvswitch/uuid.h"
#include "ovsdb-types.h"
#include "ovsdb-data.h"

struct ovsdb_condition;
struct ovsdb_column_set;
struct ovsdb_index_set;
struct ovsdb_row_cache;
struct ovsdb_storage_engine;
struct ovsdb_table;

/* Query engine — plans and executes queries.
 *
 * Orchestrates storage engine (disk I/O), index engine (lookups),
 * and cache (row cache) to serve queries optimally.
 *
 * Usage:
 *   1. plan(condition, indexes, table) → execution_plan
 *   2. describe(plan) → human-readable EXPLAIN string
 *   3. execute(plan, storage, indexes, cache) → result iterator
 *   4. iterate: next(result) until NULL
 *   5. close(result) */

/* --- Plan Types --- */
enum ovsdb_plan_type {
    OVSDB_PLAN_POINT_LOOKUP,   /* UUID exact match → bloom + storage */
    OVSDB_PLAN_INDEX_LOOKUP,   /* Column EQ → index → entry → storage */
    OVSDB_PLAN_FULL_SCAN,      /* Cursor iteration, optional filter */
};

/* --- Execution Plan (inspectable) --- */
struct ovsdb_execution_plan {
    enum ovsdb_plan_type type;

    /* POINT_LOOKUP: */
    struct uuid target_uuid;

    /* INDEX_LOOKUP: */
    const struct ovsdb_index *index;
    union ovsdb_atom key;
    enum ovsdb_atomic_type key_type;
    bool key_valid;

    /* FULL_SCAN: */
    const struct ovsdb_condition *condition; /* NULL = no filter */
    bool use_cache;       /* true = check/populate cache per row */
};

/* --- Planning --- */

/* Inspects condition + available indexes to choose the optimal path.
 * Returns a plan that can be inspected (EXPLAIN) or executed. */
struct ovsdb_execution_plan *ovsdb_query_engine_plan(
    const struct ovsdb_condition *,       /* NULL = full scan */
    const struct ovsdb_index_set *,       /* Available indexes */
    const struct ovsdb_table *);
void ovsdb_execution_plan_destroy(struct ovsdb_execution_plan *);

/* --- EXPLAIN --- */

/* Returns a human-readable description of the plan.
 * Caller must free the returned string. */
char *ovsdb_execution_plan_describe(const struct ovsdb_execution_plan *);

/* --- Execution --- */

/* Result iterator returned by execute. */
struct ovsdb_query_result;

/* Executes the plan and returns a result iterator.
 * 'cache' may be NULL to skip cache interaction (e.g., streaming). */
struct ovsdb_query_result *ovsdb_query_engine_execute(
    const struct ovsdb_execution_plan *,
    struct ovsdb_storage_engine *,
    struct ovsdb_index_set *,
    struct ovsdb_row_cache *,             /* May be NULL */
    struct ovsdb_table *);

/* Returns the next row, or NULL when done.
 * For POINT/INDEX_LOOKUP: returns at most 1 row.
 * For FULL_SCAN: returns rows sequentially.
 * Caller does NOT take ownership — pointer valid until next() or close(). */
const struct ovsdb_row *ovsdb_query_result_next(
    struct ovsdb_query_result *);

/* Closes the result and frees resources. */
void ovsdb_query_result_close(struct ovsdb_query_result *);

#endif /* ovsdb/query-engine.h */
