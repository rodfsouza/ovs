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

/* Unit tests for ovsdb/lazy-load. */

#include <config.h>
#undef NDEBUG
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "ovstest.h"
#include "ovsdb/lazy-load.h"
#include "ovsdb/worker-pool.h"
#include "ovsdb/row-cache.h"
#include "ovsdb/disk-store.h"
#include "ovsdb/table.h"
#include "ovsdb/row.h"
#include "ovsdb/ovsdb.h"
#include "ovsdb/column.h"
#include "ovsdb/storage.h"
#include "ovsdb/trigger.h"
#include "openvswitch/uuid.h"
#include "util.h"
#include "timeval.h"

/* Create a minimal table for testing (standalone, no db). */
static struct ovsdb_table *
create_test_table(void)
{
    struct ovsdb_table_schema *ts;
    ts = ovsdb_table_schema_create("test", true, UINT_MAX, true);
    return ovsdb_table_create(ts);
}

/* Create a row with a specific UUID in the given table. */
static struct ovsdb_row *
create_test_row(struct ovsdb_table *table,
                const struct uuid *uuid)
{
    struct ovsdb_row *row = ovsdb_row_create(table);
    *ovsdb_row_get_uuid_rw(row) = *uuid;
    return row;
}

/* Create a schema containing a single "test" table. */
static struct ovsdb_schema *
create_test_schema(void)
{
    struct ovsdb_schema *schema;
    struct ovsdb_table_schema *ts;

    schema = ovsdb_schema_create("testdb", "1.0.0", "");
    ts = ovsdb_table_schema_create("test", true, UINT_MAX, true);
    shash_add(&schema->tables, "test", ts);
    return schema;
}

/* 1. Init and destroy without crashing. */
static void
test_lazy_load_init_destroy(void)
{
    struct ovsdb_worker_pool *pool;

    pool = ovsdb_worker_pool_create(2, "test-ll");
    ovsdb_lazy_load_init(pool);
    ovsdb_lazy_load_destroy();
    ovsdb_worker_pool_destroy(pool);
}

/* 2. Request with no pool returns false. */
static void
test_lazy_load_request_no_pool(void)
{
    struct ovsdb_table *table;
    struct uuid uuid;
    bool submitted;

    /* Ensure the lazy-load subsystem has no pool. */
    ovsdb_lazy_load_destroy();

    table = create_test_table();
    uuid_generate(&uuid);

    submitted = ovsdb_lazy_load_request(NULL, table, &uuid);
    ovs_assert(!submitted);

    ovsdb_table_destroy(table);
}

/* 3. Request submits a job and eventually loads the row. */
static void
test_lazy_load_request_submits(void)
{
    static const char *filename = "test-lazy-load-request.db";
    struct ovsdb_worker_pool *pool;
    struct ovsdb_schema *schema;
    struct ovsdb *db;
    struct ovsdb_table *table;
    struct ovsdb_row_cache *cache;
    struct ovsdb_disk_store *ds;
    struct ovsdb_row *row;
    struct ovsdb_error *err;
    struct uuid uuid;
    bool submitted;
    int i;

    /* Create worker pool and init lazy-load. */
    pool = ovsdb_worker_pool_create(2, "test-ll-req");
    ovsdb_lazy_load_init(pool);

    /* Build an ovsdb with a "test" table. */
    schema = create_test_schema();
    db = ovsdb_create(ovsdb_schema_clone(schema),
                      ovsdb_storage_create_unbacked(NULL));
    table = ovsdb_get_table(db, "test");
    ovs_assert(table != NULL);

    /* Open disk store and write a row. */
    ds = ovsdb_disk_store_open(filename, schema);
    ovs_assert(ds != NULL);

    uuid_generate(&uuid);
    row = create_test_row(table, &uuid);
    err = ovsdb_disk_store_write_row(ds, row);
    ovs_assert(!err);
    ovsdb_row_destroy(row);

    /* Attach cache and disk store to the table. */
    cache = ovsdb_row_cache_create(100000);
    table->cache = cache;
    table->disk_store = ds;

    /* Mark the UUID as unloaded in the cache. */
    ovsdb_row_cache_add_unloaded(cache, &uuid);
    ovs_assert(ovsdb_row_cache_get_state(cache, &uuid)
               == OVSDB_ROW_UNLOADED);

    /* Submit the lazy-load request. */
    submitted = ovsdb_lazy_load_request(db, table, &uuid);
    ovs_assert(submitted);

    /* Spin until the worker delivers the row. */
    for (i = 0; i < 5000; i++) {
        ovsdb_worker_pool_run(pool);
        if (ovsdb_row_cache_get_state(cache, &uuid)
            == OVSDB_ROW_CACHED) {
            break;
        }
        xnanosleep(1000000);  /* 1 ms */
    }

    ovs_assert(ovsdb_row_cache_get_state(cache, &uuid)
               == OVSDB_ROW_CACHED);
    ovs_assert(ovsdb_row_cache_lookup(cache, &uuid) != NULL);

    /* Cleanup. */
    table->cache = NULL;
    table->disk_store = NULL;
    ovsdb_row_cache_destroy(cache);
    ovsdb_disk_store_close(ds);
    ovsdb_lazy_load_destroy();
    ovsdb_destroy(db);
    ovsdb_worker_pool_destroy(pool);
    ovsdb_schema_destroy(schema);
    unlink(filename);
}

/* 4. Cache state transitions: UNLOADED -> LOADING -> CACHED. */
static void
test_cache_state_transitions(void)
{
    struct ovsdb_row_cache *cache;
    struct uuid uuid;

    cache = ovsdb_row_cache_create(100000);
    uuid_generate(&uuid);

    /* Unknown UUID defaults to UNLOADED. */
    ovs_assert(ovsdb_row_cache_get_state(cache, &uuid)
               == OVSDB_ROW_UNLOADED);

    /* Register as unloaded. */
    ovsdb_row_cache_add_unloaded(cache, &uuid);
    ovs_assert(ovsdb_row_cache_get_state(cache, &uuid)
               == OVSDB_ROW_UNLOADED);

    /* Transition to LOADING. */
    ovsdb_row_cache_set_state(cache, &uuid, OVSDB_ROW_LOADING);
    ovs_assert(ovsdb_row_cache_get_state(cache, &uuid)
               == OVSDB_ROW_LOADING);

    /* Transition to CACHED. */
    ovsdb_row_cache_set_state(cache, &uuid, OVSDB_ROW_CACHED);
    ovs_assert(ovsdb_row_cache_get_state(cache, &uuid)
               == OVSDB_ROW_CACHED);

    ovsdb_row_cache_destroy(cache);
}

/* 5. Trigger waiting_for_data field exists and works. */
static void
test_trigger_waiting_for_data_flag(void)
{
    struct ovsdb_trigger trigger;

    memset(&trigger, 0, sizeof trigger);

    ovs_assert(!trigger.waiting_for_data);

    trigger.waiting_for_data = true;
    ovs_assert(trigger.waiting_for_data);

    trigger.waiting_for_data = false;
    ovs_assert(!trigger.waiting_for_data);
}

static void
test_lazy_load_main(int argc OVS_UNUSED, char *argv[] OVS_UNUSED)
{
    printf("test_lazy_load_init_destroy\n");
    test_lazy_load_init_destroy();

    printf("test_lazy_load_request_no_pool\n");
    test_lazy_load_request_no_pool();

    printf("test_lazy_load_request_submits\n");
    test_lazy_load_request_submits();

    printf("test_cache_state_transitions\n");
    test_cache_state_transitions();

    printf("test_trigger_waiting_for_data_flag\n");
    test_trigger_waiting_for_data_flag();

    printf("test-lazy-load: ok\n");
}

OVSTEST_REGISTER("test-lazy-load", test_lazy_load_main);
