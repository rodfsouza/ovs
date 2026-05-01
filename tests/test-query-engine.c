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

#include <stdio.h>
#include <string.h>

#include "ovsdb/query-engine.h"
#include "ovsdb/index-engine.h"
#include "ovsdb/condition.h"
#include "ovsdb/column.h"
#include "ovsdb/table.h"
#include "openvswitch/json.h"
#include "openvswitch/uuid.h"
#include "ovsdb-data.h"
#include "ovsdb-types.h"
#include "util.h"

/* ------------------------------------------------------------------ */
/* Test 1: Plan NULL condition → FULL_SCAN.                           */
/* ------------------------------------------------------------------ */

static void
test_plan_null_condition(void)
{
    struct ovsdb_execution_plan *plan;
    char *desc;

    plan = ovsdb_query_engine_plan(NULL, NULL, NULL);
    ovs_assert(plan->type == OVSDB_PLAN_FULL_SCAN);
    ovs_assert(plan->condition == NULL);
    ovs_assert(plan->use_cache == false);

    desc = ovsdb_execution_plan_describe(plan);
    ovs_assert(strstr(desc, "FULL_SCAN"));
    ovs_assert(strstr(desc, "unconditioned"));
    free(desc);

    ovsdb_execution_plan_destroy(plan);
    printf("plan_null_condition: PASSED\n");
}

/* ------------------------------------------------------------------ */
/* Test 2: Plan with UUID condition → POINT_LOOKUP.                   */
/* ------------------------------------------------------------------ */

static void
test_plan_uuid_condition(void)
{
    /* Build a minimal UUID condition.  We need a fake column
     * definition for _uuid at index OVSDB_COL_UUID. */
    struct ovsdb_type uuid_type;
    struct ovsdb_column uuid_col;
    struct ovsdb_condition cond;
    struct ovsdb_execution_plan *plan;
    struct uuid target;
    char *desc;

    ovsdb_base_type_init(&uuid_type.key, OVSDB_TYPE_UUID);
    ovsdb_base_type_init(&uuid_type.value, OVSDB_TYPE_VOID);
    uuid_type.n_min = 1;
    uuid_type.n_max = 1;

    memset(&uuid_col, 0, sizeof uuid_col);
    uuid_col.index = OVSDB_COL_UUID;
    uuid_col.name = "uuid";
    uuid_col.type = uuid_type;

    uuid_from_string(&target, "a93b5600-5ac4-448f-9199-a9127f10378a");

    memset(&cond, 0, sizeof cond);
    cond.n_clauses = 1;
    cond.clauses = xmalloc(sizeof *cond.clauses);
    cond.clauses[0].function = OVSDB_F_EQ;
    cond.clauses[0].column = &uuid_col;
    cond.clauses[0].index = OVSDB_COL_UUID;
    ovsdb_datum_init_empty(&cond.clauses[0].arg);
    cond.clauses[0].arg.n = 1;
    cond.clauses[0].arg.keys = xmalloc(sizeof *cond.clauses[0].arg.keys);
    cond.clauses[0].arg.keys[0].uuid = target;

    plan = ovsdb_query_engine_plan(&cond, NULL, NULL);
    ovs_assert(plan->type == OVSDB_PLAN_POINT_LOOKUP);
    ovs_assert(uuid_equals(&plan->target_uuid, &target));

    desc = ovsdb_execution_plan_describe(plan);
    ovs_assert(strstr(desc, "POINT_LOOKUP"));
    ovs_assert(strstr(desc, "a93b5600"));
    free(desc);

    ovsdb_execution_plan_destroy(plan);
    free(cond.clauses[0].arg.keys);
    free(cond.clauses);
    printf("plan_uuid_condition: PASSED\n");
}

/* ------------------------------------------------------------------ */
/* Test 3: Plan with name condition + hash index → INDEX_LOOKUP.      */
/* ------------------------------------------------------------------ */

static void
test_plan_name_with_index(void)
{
    struct ovsdb_type str_type;
    struct ovsdb_column name_col;
    struct ovsdb_condition cond;
    struct ovsdb_index_spec idx_spec;
    struct ovsdb_index *idx;
    struct ovsdb_index_set set;
    struct ovsdb_execution_plan *plan;
    char *desc;

    ovsdb_base_type_init(&str_type.key, OVSDB_TYPE_STRING);
    ovsdb_base_type_init(&str_type.value, OVSDB_TYPE_VOID);
    str_type.n_min = 1;
    str_type.n_max = 1;

    memset(&name_col, 0, sizeof name_col);
    name_col.index = 2;  /* Some column index > OVSDB_N_STD_COLUMNS. */
    name_col.name = "name";
    name_col.type = str_type;

    memset(&cond, 0, sizeof cond);
    cond.n_clauses = 1;
    cond.clauses = xmalloc(sizeof *cond.clauses);
    cond.clauses[0].function = OVSDB_F_EQ;
    cond.clauses[0].column = &name_col;
    cond.clauses[0].index = 2;
    ovsdb_datum_init_empty(&cond.clauses[0].arg);
    cond.clauses[0].arg.n = 1;
    cond.clauses[0].arg.keys = xmalloc(sizeof *cond.clauses[0].arg.keys);
    cond.clauses[0].arg.keys[0].s = json_string_create("my-router");

    idx_spec.type = OVSDB_IDX_HASH;
    idx_spec.name = "idx_name";
    idx_spec.column_name = "name";
    idx_spec.key_type = OVSDB_TYPE_STRING;
    idx = ovsdb_index_create(&idx_spec);

    ovsdb_index_set_init(&set);
    ovsdb_index_set_add(&set, idx);

    plan = ovsdb_query_engine_plan(&cond, &set, NULL);
    ovs_assert(plan->type == OVSDB_PLAN_INDEX_LOOKUP);
    ovs_assert(plan->index == idx);

    desc = ovsdb_execution_plan_describe(plan);
    ovs_assert(strstr(desc, "INDEX_LOOKUP"));
    ovs_assert(strstr(desc, "idx_name"));
    free(desc);

    ovsdb_execution_plan_destroy(plan);
    json_destroy(cond.clauses[0].arg.keys[0].s);
    free(cond.clauses[0].arg.keys);
    free(cond.clauses);
    ovsdb_index_set_destroy(&set);
    printf("plan_name_with_index: PASSED\n");
}

/* ------------------------------------------------------------------ */
/* Test 4: Plan with name condition but NO index → FULL_SCAN.         */
/* ------------------------------------------------------------------ */

static void
test_plan_name_without_index(void)
{
    struct ovsdb_type str_type;
    struct ovsdb_column name_col;
    struct ovsdb_condition cond;
    struct ovsdb_index_set set;
    struct ovsdb_execution_plan *plan;

    ovsdb_base_type_init(&str_type.key, OVSDB_TYPE_STRING);
    ovsdb_base_type_init(&str_type.value, OVSDB_TYPE_VOID);
    str_type.n_min = 1;
    str_type.n_max = 1;

    memset(&name_col, 0, sizeof name_col);
    name_col.index = 2;
    name_col.name = "name";
    name_col.type = str_type;

    memset(&cond, 0, sizeof cond);
    cond.n_clauses = 1;
    cond.clauses = xmalloc(sizeof *cond.clauses);
    cond.clauses[0].function = OVSDB_F_EQ;
    cond.clauses[0].column = &name_col;
    cond.clauses[0].index = 2;
    ovsdb_datum_init_empty(&cond.clauses[0].arg);
    cond.clauses[0].arg.n = 1;
    cond.clauses[0].arg.keys = xmalloc(sizeof *cond.clauses[0].arg.keys);
    cond.clauses[0].arg.keys[0].s = json_string_create("my-router");

    /* Empty index set — no hash index for "name". */
    ovsdb_index_set_init(&set);

    plan = ovsdb_query_engine_plan(&cond, &set, NULL);
    ovs_assert(plan->type == OVSDB_PLAN_FULL_SCAN);
    ovs_assert(plan->condition == &cond);
    ovs_assert(plan->use_cache == true);

    ovsdb_execution_plan_destroy(plan);
    json_destroy(cond.clauses[0].arg.keys[0].s);
    free(cond.clauses[0].arg.keys);
    free(cond.clauses);
    ovsdb_index_set_destroy(&set);
    printf("plan_name_without_index: PASSED\n");
}

/* ------------------------------------------------------------------ */
/* Test 5: EXPLAIN output format.                                     */
/* ------------------------------------------------------------------ */

static void
test_explain_format(void)
{
    struct ovsdb_execution_plan *plan;
    char *desc;

    /* Full scan. */
    plan = ovsdb_query_engine_plan(NULL, NULL, NULL);
    desc = ovsdb_execution_plan_describe(plan);
    ovs_assert(strstr(desc, "FULL_SCAN"));
    ovs_assert(strstr(desc, "cache=no"));
    free(desc);
    ovsdb_execution_plan_destroy(plan);

    printf("explain_format: PASSED\n");
}

/* ------------------------------------------------------------------ */
/* Test 6: Plan destroy with cloned key (no leak).                    */
/* ------------------------------------------------------------------ */

static void
test_plan_destroy_with_key(void)
{
    struct ovsdb_type str_type;
    struct ovsdb_column name_col;
    struct ovsdb_condition cond;
    struct ovsdb_index_spec idx_spec;
    struct ovsdb_index *idx;
    struct ovsdb_index_set set;
    struct ovsdb_execution_plan *plan;

    ovsdb_base_type_init(&str_type.key, OVSDB_TYPE_STRING);
    ovsdb_base_type_init(&str_type.value, OVSDB_TYPE_VOID);
    str_type.n_min = 1;
    str_type.n_max = 1;

    memset(&name_col, 0, sizeof name_col);
    name_col.index = 2;
    name_col.name = "name";
    name_col.type = str_type;

    memset(&cond, 0, sizeof cond);
    cond.n_clauses = 1;
    cond.clauses = xmalloc(sizeof *cond.clauses);
    cond.clauses[0].function = OVSDB_F_EQ;
    cond.clauses[0].column = &name_col;
    cond.clauses[0].index = 2;
    ovsdb_datum_init_empty(&cond.clauses[0].arg);
    cond.clauses[0].arg.n = 1;
    cond.clauses[0].arg.keys = xmalloc(sizeof *cond.clauses[0].arg.keys);
    cond.clauses[0].arg.keys[0].s = json_string_create("test-destroy");

    idx_spec.type = OVSDB_IDX_HASH;
    idx_spec.name = "idx";
    idx_spec.column_name = "name";
    idx_spec.key_type = OVSDB_TYPE_STRING;
    idx = ovsdb_index_create(&idx_spec);

    ovsdb_index_set_init(&set);
    ovsdb_index_set_add(&set, idx);

    /* Plan clones the key atom. Destroy should free it. */
    plan = ovsdb_query_engine_plan(&cond, &set, NULL);
    ovs_assert(plan->type == OVSDB_PLAN_INDEX_LOOKUP);
    ovs_assert(plan->key_valid);
    ovsdb_execution_plan_destroy(plan);

    json_destroy(cond.clauses[0].arg.keys[0].s);
    free(cond.clauses[0].arg.keys);
    free(cond.clauses);
    ovsdb_index_set_destroy(&set);
    printf("plan_destroy_with_key: PASSED\n");
}

/* ------------------------------------------------------------------ */
/* Main.                                                              */
/* ------------------------------------------------------------------ */

static struct {
    const char *name;
    void (*fn)(void);
} tests[] = {
    { "plan_null_condition", test_plan_null_condition },
    { "plan_uuid_condition", test_plan_uuid_condition },
    { "plan_name_with_index", test_plan_name_with_index },
    { "plan_name_without_index", test_plan_name_without_index },
    { "explain_format", test_explain_format },
    { "plan_destroy_with_key", test_plan_destroy_with_key },
};

int
main(int argc, char *argv[])
{
    size_t i;

    if (argc < 2) {
        for (i = 0; i < sizeof tests / sizeof tests[0]; i++) {
            tests[i].fn();
        }
        return 0;
    }

    for (i = 0; i < sizeof tests / sizeof tests[0]; i++) {
        if (!strcmp(argv[1], tests[i].name)) {
            tests[i].fn();
            return 0;
        }
    }

    fprintf(stderr, "unknown test: %s\n", argv[1]);
    return 1;
}
