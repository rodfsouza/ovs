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

/* Unit tests for ovsdb/row-cache. */

#include <config.h>
#undef NDEBUG
#include <stdio.h>
#include <string.h>

#include "ovstest.h"
#include "util.h"
#include "openvswitch/uuid.h"
#include "ovsdb/row.h"
#include "ovsdb/row-cache.h"
#include "ovsdb/table.h"
#include "ovsdb/column.h"

/* Create a minimal table schema for testing. */
static struct ovsdb_table *
create_test_table(void)
{
    struct ovsdb_table_schema *ts;
    ts = ovsdb_table_schema_create("test", true, UINT_MAX, true);
    return ovsdb_table_create(ts);
}

/* Create a row with a specific UUID in the given table. */
static struct ovsdb_row *
create_test_row(struct ovsdb_table *table, const struct uuid *uuid)
{
    struct ovsdb_row *row = ovsdb_row_create(table);
    *ovsdb_row_get_uuid_rw(row) = *uuid;
    return row;
}

/* Generate a deterministic UUID from an integer index. */
static struct uuid
make_uuid(int i)
{
    struct uuid uuid;
    memset(&uuid, 0, sizeof uuid);
    uuid.parts[0] = i;
    return uuid;
}

/* 1. Basic insert and lookup. */
static void
test_basic_insert_lookup(void)
{
    struct ovsdb_table *table = create_test_table();
    struct ovsdb_row_cache *cache = ovsdb_row_cache_create(10000);
    struct uuid uuid = make_uuid(1);
    struct ovsdb_row *row = create_test_row(table, &uuid);
    struct ovsdb_row *found;

    ovsdb_row_cache_insert(cache, row, 10);

    found = ovsdb_row_cache_lookup(cache, &uuid);
    ovs_assert(found == row);
    ovs_assert(ovsdb_row_cache_count(cache) == 1);
    ovs_assert(ovsdb_row_cache_n_atoms(cache) == 10);

    ovsdb_row_cache_destroy(cache);
    ovsdb_table_destroy(table);
}

/* 2. Eviction when atom budget is exceeded. */
static void
test_eviction_by_atoms(void)
{
    struct ovsdb_table *table = create_test_table();
    struct ovsdb_row_cache *cache = ovsdb_row_cache_create(100);
    struct uuid uuids[15];
    int i;

    /* Insert 15 rows x 10 atoms = 150, exceeding budget of 100. */
    for (i = 0; i < 15; i++) {
        struct ovsdb_row *row;
        uuids[i] = make_uuid(i);
        row = create_test_row(table, &uuids[i]);
        ovsdb_row_cache_insert(cache, row, 10);
    }

    /* At most 10 rows can fit (10 * 10 = 100). */
    ovs_assert(ovsdb_row_cache_count(cache) <= 10);

    /* Oldest rows should have been evicted, newest should remain. */
    for (i = 0; i < 5; i++) {
        ovs_assert(ovsdb_row_cache_lookup(cache, &uuids[i]) == NULL);
    }
    for (i = 10; i < 15; i++) {
        ovs_assert(ovsdb_row_cache_lookup(cache, &uuids[i]) != NULL);
    }

    ovsdb_row_cache_destroy(cache);
    ovsdb_table_destroy(table);
}

/* 3. Pinned rows are not evicted. */
static void
test_pin_prevents_eviction(void)
{
    struct ovsdb_table *table = create_test_table();
    struct ovsdb_row_cache *cache = ovsdb_row_cache_create(50);
    struct uuid pinned_uuids[5];
    struct uuid unpinned_uuids[5];
    int i;

    /* Insert and pin 5 rows (5 * 10 = 50 atoms). */
    for (i = 0; i < 5; i++) {
        struct ovsdb_row *row;
        pinned_uuids[i] = make_uuid(i);
        row = create_test_row(table, &pinned_uuids[i]);
        ovsdb_row_cache_insert(cache, row, 10);
        ovsdb_row_cache_pin(cache, &pinned_uuids[i]);
    }

    /* Insert 5 more unpinned rows (would exceed budget). */
    for (i = 0; i < 5; i++) {
        struct ovsdb_row *row;
        unpinned_uuids[i] = make_uuid(100 + i);
        row = create_test_row(table, &unpinned_uuids[i]);
        ovsdb_row_cache_insert(cache, row, 10);
    }

    /* All pinned rows must survive. */
    for (i = 0; i < 5; i++) {
        ovs_assert(ovsdb_row_cache_lookup(cache, &pinned_uuids[i])
                   != NULL);
    }

    /* Unpinned rows should be evicted to stay within budget. */
    for (i = 0; i < 5; i++) {
        ovs_assert(ovsdb_row_cache_lookup(cache, &unpinned_uuids[i])
                   == NULL);
    }

    ovsdb_row_cache_destroy(cache);
    ovsdb_table_destroy(table);
}

/* 4. Unpinning allows eviction. */
static void
test_unpin_allows_eviction(void)
{
    struct ovsdb_table *table = create_test_table();
    struct ovsdb_row_cache *cache = ovsdb_row_cache_create(30);
    struct uuid u0 = make_uuid(0);
    struct uuid u1 = make_uuid(1);
    struct uuid u2 = make_uuid(2);
    struct uuid u3 = make_uuid(3);
    struct ovsdb_row *row;

    /* Insert and pin row 0. */
    row = create_test_row(table, &u0);
    ovsdb_row_cache_insert(cache, row, 10);
    ovsdb_row_cache_pin(cache, &u0);

    /* Insert rows 1 and 2 (total 30 atoms, at budget). */
    row = create_test_row(table, &u1);
    ovsdb_row_cache_insert(cache, row, 10);
    row = create_test_row(table, &u2);
    ovsdb_row_cache_insert(cache, row, 10);

    /* Row 0 is pinned, so it survives. */
    ovs_assert(ovsdb_row_cache_lookup(cache, &u0) != NULL);

    /* Unpin row 0. */
    ovsdb_row_cache_unpin(cache, &u0);

    /* Insert row 3 to push past budget and trigger eviction. */
    row = create_test_row(table, &u3);
    ovsdb_row_cache_insert(cache, row, 10);

    /* Row 0 is now eligible for eviction (oldest unpinned). */
    ovs_assert(ovsdb_row_cache_lookup(cache, &u0) == NULL);

    ovsdb_row_cache_destroy(cache);
    ovsdb_table_destroy(table);
}

/* 5. Remove by UUID. */
static void
test_remove(void)
{
    struct ovsdb_table *table = create_test_table();
    struct ovsdb_row_cache *cache = ovsdb_row_cache_create(10000);
    struct uuid uuid = make_uuid(42);
    struct ovsdb_row *row;

    row = create_test_row(table, &uuid);
    ovsdb_row_cache_insert(cache, row, 10);
    ovs_assert(ovsdb_row_cache_count(cache) == 1);

    ovsdb_row_cache_remove(cache, &uuid);
    ovs_assert(ovsdb_row_cache_lookup(cache, &uuid) == NULL);
    ovs_assert(ovsdb_row_cache_count(cache) == 0);

    ovsdb_row_cache_destroy(cache);
    ovsdb_table_destroy(table);
}

/* 6. Hit/miss statistics. */
static void
test_stats(void)
{
    struct ovsdb_table *table = create_test_table();
    struct ovsdb_row_cache *cache = ovsdb_row_cache_create(10000);
    struct uuid u0 = make_uuid(0);
    struct uuid u1 = make_uuid(1);
    struct uuid missing = make_uuid(999);
    struct ovsdb_row *row;

    ovs_assert(ovsdb_row_cache_hits(cache) == 0);
    ovs_assert(ovsdb_row_cache_misses(cache) == 0);

    row = create_test_row(table, &u0);
    ovsdb_row_cache_insert(cache, row, 5);
    row = create_test_row(table, &u1);
    ovsdb_row_cache_insert(cache, row, 5);

    /* Two hits. */
    ovsdb_row_cache_lookup(cache, &u0);
    ovsdb_row_cache_lookup(cache, &u1);
    ovs_assert(ovsdb_row_cache_hits(cache) == 2);

    /* Two misses. */
    ovsdb_row_cache_lookup(cache, &missing);
    ovsdb_row_cache_lookup(cache, &missing);
    ovs_assert(ovsdb_row_cache_misses(cache) == 2);

    ovsdb_row_cache_destroy(cache);
    ovsdb_table_destroy(table);
}

/* 7. Zero budget: rows are immediately evicted. */
static void
test_zero_budget(void)
{
    struct ovsdb_table *table = create_test_table();
    struct ovsdb_row_cache *cache = ovsdb_row_cache_create(0);
    struct uuid uuid = make_uuid(1);
    struct ovsdb_row *row;

    row = create_test_row(table, &uuid);
    ovsdb_row_cache_insert(cache, row, 10);

    /* With zero budget the row cannot be retained. */
    ovs_assert(ovsdb_row_cache_lookup(cache, &uuid) == NULL);
    ovs_assert(ovsdb_row_cache_count(cache) == 0);

    ovsdb_row_cache_destroy(cache);
    ovsdb_table_destroy(table);
}

/* 8. Unlimited budget retains all rows. */
static void
test_unlimited(void)
{
    struct ovsdb_table *table = create_test_table();
    struct ovsdb_row_cache *cache = ovsdb_row_cache_create(SIZE_MAX);
    int i;

    for (i = 0; i < 1000; i++) {
        struct ovsdb_row *row;
        struct uuid uuid = make_uuid(i);
        row = create_test_row(table, &uuid);
        ovsdb_row_cache_insert(cache, row, 10);
    }

    ovs_assert(ovsdb_row_cache_count(cache) == 1000);

    /* Verify every row is still accessible. */
    for (i = 0; i < 1000; i++) {
        struct uuid uuid = make_uuid(i);
        ovs_assert(ovsdb_row_cache_lookup(cache, &uuid) != NULL);
    }

    ovsdb_row_cache_destroy(cache);
    ovsdb_table_destroy(table);
}

/* 9. Destroy with rows still in cache (ASAN catches leaks). */
static void
test_destroy_with_rows(void)
{
    struct ovsdb_table *table = create_test_table();
    struct ovsdb_row_cache *cache = ovsdb_row_cache_create(10000);
    int i;

    for (i = 0; i < 50; i++) {
        struct ovsdb_row *row;
        struct uuid uuid = make_uuid(i);
        row = create_test_row(table, &uuid);
        ovsdb_row_cache_insert(cache, row, 5);
    }

    ovs_assert(ovsdb_row_cache_count(cache) == 50);

    /* Destroying the cache must free all owned rows. */
    ovsdb_row_cache_destroy(cache);
    ovsdb_table_destroy(table);
}

/* Verify the lazy-load state tracker (Phase 2).
 * add_unloaded() creates entries with state=UNLOADED and row=NULL.
 * set_state() transitions them, and eviction handles NULL rows. */
static void
test_state_tracker(void)
{
    struct ovsdb_row_cache *cache;
    struct uuid u1;

    cache = ovsdb_row_cache_create(1000);
    uuid_generate(&u1);

    /* Initially unknown UUID. */
    ovs_assert(ovsdb_row_cache_get_state(cache, &u1)
               == OVSDB_ROW_UNLOADED);

    /* Register as unloaded. */
    ovsdb_row_cache_add_unloaded(cache, &u1);
    ovs_assert(ovsdb_row_cache_get_state(cache, &u1)
               == OVSDB_ROW_UNLOADED);
    ovs_assert(ovsdb_row_cache_count(cache) == 1);

    /* Transition to LOADING. */
    ovsdb_row_cache_set_state(cache, &u1, OVSDB_ROW_LOADING);
    ovs_assert(ovsdb_row_cache_get_state(cache, &u1)
               == OVSDB_ROW_LOADING);

    /* Duplicate add_unloaded is a no-op. */
    ovsdb_row_cache_add_unloaded(cache, &u1);
    ovs_assert(ovsdb_row_cache_count(cache) == 1);

    /* Remove an entry with NULL row (UNLOADED/LOADING). */
    ovsdb_row_cache_remove(cache, &u1);
    ovs_assert(ovsdb_row_cache_count(cache) == 0);

    ovsdb_row_cache_destroy(cache);
}

/* Verify pin/unpin lifecycle: pin during modify, unpin after commit. */
static void
test_pin_unpin_lifecycle(void)
{
    struct ovsdb_row_cache *cache;
    struct ovsdb_table *table;
    struct ovsdb_row *row;
    struct uuid u1;

    cache = ovsdb_row_cache_create(100);
    table = create_test_table();
    uuid_generate(&u1);

    row = create_test_row(table, &u1);
    ovsdb_row_cache_insert(cache, row, 10);

    /* Pin the row (simulates ovsdb_txn_row_modify). */
    ovsdb_row_cache_pin(cache, &u1);

    /* Fill cache to force eviction. */
    {
        size_t i;
        for (i = 0; i < 20; i++) {
            struct uuid u;
            uuid_generate(&u);
            ovsdb_row_cache_insert(cache,
                                   create_test_row(table, &u), 10);
        }
    }
    /* Pinned row should survive eviction. */
    ovs_assert(ovsdb_row_cache_lookup(cache, &u1) != NULL);

    /* Unpin (simulates ovsdb_txn_row_commit). */
    ovsdb_row_cache_unpin(cache, &u1);

    /* Force more eviction — now it can be evicted. */
    {
        size_t i;
        for (i = 0; i < 20; i++) {
            struct uuid u;
            uuid_generate(&u);
            ovsdb_row_cache_insert(cache,
                                   create_test_row(table, &u), 10);
        }
    }

    /* The row may or may not have been evicted depending on LRU
     * ordering, but the test verifies no crash and no leak. */
    ovsdb_row_cache_destroy(cache);
    ovsdb_table_destroy(table);
}

/* Verify ovsdb_row_count_atoms() returns correct counts. */
static void
test_row_count_atoms(void)
{
    struct ovsdb_table *table;
    struct ovsdb_row *row;
    struct uuid u1;
    size_t n;

    table = create_test_table();
    uuid_generate(&u1);
    row = create_test_row(table, &u1);

    /* Default row with only _uuid and _version columns
     * (both are single-atom UUID datums). */
    n = ovsdb_row_count_atoms(row);
    ovs_assert(n >= 2);  /* At least _uuid + _version. */

    ovsdb_row_destroy(row);
    ovsdb_table_destroy(table);
}

static void
test_row_cache_main(int argc OVS_UNUSED, char *argv[] OVS_UNUSED)
{
    printf("test_basic_insert_lookup\n");
    test_basic_insert_lookup();

    printf("test_eviction_by_atoms\n");
    test_eviction_by_atoms();

    printf("test_pin_prevents_eviction\n");
    test_pin_prevents_eviction();

    printf("test_unpin_allows_eviction\n");
    test_unpin_allows_eviction();

    printf("test_remove\n");
    test_remove();

    printf("test_stats\n");
    test_stats();

    printf("test_zero_budget\n");
    test_zero_budget();

    printf("test_unlimited\n");
    test_unlimited();

    printf("test_destroy_with_rows\n");
    test_destroy_with_rows();

    printf("test_state_tracker\n");
    test_state_tracker();

    printf("test_pin_unpin_lifecycle\n");
    test_pin_unpin_lifecycle();

    printf("test_row_count_atoms\n");
    test_row_count_atoms();

    printf("test-row-cache: ok\n");
}

OVSTEST_REGISTER("test-row-cache", test_row_cache_main);
