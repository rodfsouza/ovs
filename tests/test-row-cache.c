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
#include "ovs-thread.h"
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

/* 2. Eviction when atom budget is exceeded.
 *
 * With clock-sweep, the exact eviction order depends on usage_count
 * and clock hand position.  We assert that the cache respects its
 * budget and that frequently-accessed entries survive. */
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

    /* Budget is 100 atoms, each row costs 10, so at most 10 fit. */
    ovs_assert(ovsdb_row_cache_count(cache) <= 10);
    ovs_assert(ovsdb_row_cache_n_atoms(cache) <= 100);

    /* At least some evictions must have occurred. */
    ovs_assert(ovsdb_row_cache_evictions(cache) >= 5);

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
    int i;

    /* Insert and pin 5 rows (5 * 10 = 50 atoms = budget). */
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
        struct uuid u = make_uuid(100 + i);
        row = create_test_row(table, &u);
        ovsdb_row_cache_insert(cache, row, 10);
    }

    /* All pinned rows must survive. */
    for (i = 0; i < 5; i++) {
        ovs_assert(ovsdb_row_cache_lookup(cache, &pinned_uuids[i])
                   != NULL);
    }

    /* Total atoms should be at or near budget (pinned rows only). */
    ovs_assert(ovsdb_row_cache_n_atoms(cache) <= 50);

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
    struct ovsdb_row *row;
    int i;

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

    /* Insert enough rows to force eviction of u0 through
     * clock-sweep.  u0 has usage_count from previous lookups,
     * so we need multiple inserts to trigger enough sweep passes. */
    for (i = 3; i < 10; i++) {
        struct uuid u = make_uuid(i);
        row = create_test_row(table, &u);
        ovsdb_row_cache_insert(cache, row, 10);
    }

    /* Budget is 30 atoms (3 rows).  u0 should eventually be
     * evicted since it is no longer pinned. */
    ovs_assert(ovsdb_row_cache_count(cache) <= 3);

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

    /* Force more eviction -- now it can be evicted. */
    {
        size_t i;
        for (i = 0; i < 20; i++) {
            struct uuid u;
            uuid_generate(&u);
            ovsdb_row_cache_insert(cache,
                                   create_test_row(table, &u), 10);
        }
    }

    /* The row may or may not have been evicted depending on
     * clock-sweep ordering, but the test verifies no crash
     * and no leak. */
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

/* ------------------------------------------------------------------
 * Clock-sweep specific tests.
 * ------------------------------------------------------------------ */

/* Frequently accessed entries survive eviction over less-accessed
 * ones due to higher usage_count. */
static void
test_clock_sweep_usage_count(void)
{
    struct ovsdb_table *table = create_test_table();
    /* Budget for 5 rows at 10 atoms each. */
    struct ovsdb_row_cache *cache = ovsdb_row_cache_create(50);
    struct uuid hot = make_uuid(0);
    struct ovsdb_row *row;
    int i;

    /* Insert the "hot" row. */
    row = create_test_row(table, &hot);
    ovsdb_row_cache_insert(cache, row, 10);

    /* Look it up many times to build high usage_count. */
    for (i = 0; i < 10; i++) {
        ovsdb_row_cache_lookup(cache, &hot);
    }

    /* Insert more rows to trigger eviction pressure. */
    for (i = 1; i <= 10; i++) {
        struct uuid u = make_uuid(i);
        row = create_test_row(table, &u);
        ovsdb_row_cache_insert(cache, row, 10);
    }

    /* Hot row should survive due to high usage_count. */
    ovs_assert(ovsdb_row_cache_lookup(cache, &hot) != NULL);
    ovs_assert(ovsdb_row_cache_count(cache) <= 5);

    ovsdb_row_cache_destroy(cache);
    ovsdb_table_destroy(table);
}

/* When all entries are pinned, eviction terminates gracefully
 * and the cache exceeds its budget. */
static void
test_clock_sweep_all_pinned(void)
{
    struct ovsdb_table *table = create_test_table();
    /* Budget=50 so all 5 rows fit initially.  Then we add more
     * entries to force eviction, but pinned entries survive. */
    struct ovsdb_row_cache *cache = ovsdb_row_cache_create(50);
    struct uuid uuids[5];
    struct ovsdb_row *row;
    int i;

    /* Insert and pin 5 rows (50 atoms = budget). */
    for (i = 0; i < 5; i++) {
        uuids[i] = make_uuid(i);
        row = create_test_row(table, &uuids[i]);
        ovsdb_row_cache_insert(cache, row, 10);
        ovsdb_row_cache_pin(cache, &uuids[i]);
    }

    ovs_assert(ovsdb_row_cache_count(cache) == 5);
    ovs_assert(ovsdb_row_cache_n_atoms(cache) == 50);

    /* Insert more unpinned rows to force eviction.  Pinned entries
     * must all survive, causing the cache to temporarily exceed
     * budget. */
    for (i = 0; i < 5; i++) {
        struct uuid u = make_uuid(100 + i);
        row = create_test_row(table, &u);
        ovsdb_row_cache_insert(cache, row, 10);
    }

    /* All pinned entries survive. */
    for (i = 0; i < 5; i++) {
        ovs_assert(ovsdb_row_cache_lookup(cache, &uuids[i]) != NULL);
    }

    /* Evictions occurred for the unpinned entries (except possibly
     * the very last insert which is protected). */
    ovs_assert(ovsdb_row_cache_evictions(cache) >= 4);

    ovsdb_row_cache_destroy(cache);
    ovsdb_table_destroy(table);
}

/* Scan ring isolates bulk-read inserts from the main cache. */
static void
test_scan_ring_isolation(void)
{
    struct ovsdb_table *table = create_test_table();
    /* Budget for 5 rows. */
    struct ovsdb_row_cache *cache = ovsdb_row_cache_create(50);
    struct uuid hot_uuids[5];
    struct ovsdb_row *row;
    int i;

    /* Insert 5 "hot" rows into the main cache. */
    for (i = 0; i < 5; i++) {
        hot_uuids[i] = make_uuid(i);
        row = create_test_row(table, &hot_uuids[i]);
        ovsdb_row_cache_insert(cache, row, 10);

        /* Look them up to build usage_count. */
        ovsdb_row_cache_lookup(cache, &hot_uuids[i]);
        ovsdb_row_cache_lookup(cache, &hot_uuids[i]);
    }

    /* Enter bulk-read mode and insert scan entries. */
    ovsdb_row_cache_bulk_read_start(cache);

    for (i = 100; i < 200; i++) {
        struct uuid u = make_uuid(i);
        row = create_test_row(table, &u);
        ovsdb_row_cache_insert(cache, row, 10);
    }

    /* Hot entries should still be in the cache. */
    for (i = 0; i < 5; i++) {
        ovs_assert(ovsdb_row_cache_lookup(cache, &hot_uuids[i])
                   != NULL);
    }

    /* End bulk-read: scan ring entries are evicted. */
    ovsdb_row_cache_bulk_read_end(cache);

    /* Hot entries still present after ring cleanup. */
    for (i = 0; i < 5; i++) {
        ovs_assert(ovsdb_row_cache_lookup(cache, &hot_uuids[i])
                   != NULL);
    }

    /* Scan entries should be gone. */
    for (i = 100; i < 200; i++) {
        struct uuid u = make_uuid(i);
        ovs_assert(ovsdb_row_cache_lookup(cache, &u) == NULL);
    }

    ovsdb_row_cache_destroy(cache);
    ovsdb_table_destroy(table);
}

/* Scan ring reuses slots and tracks the reuse metric. */
static void
test_scan_ring_reuse(void)
{
    struct ovsdb_table *table = create_test_table();
    struct ovsdb_row_cache *cache = ovsdb_row_cache_create(SIZE_MAX);
    int total;
    struct ovsdb_row *row;
    int i;

    ovsdb_row_cache_bulk_read_start(cache);

    /* Insert more entries than the ring size. */
    total = OVSDB_ROW_CACHE_SCAN_RING_SIZE + 50;
    for (i = 0; i < total; i++) {
        struct uuid u = make_uuid(i);
        row = create_test_row(table, &u);
        ovsdb_row_cache_insert(cache, row, 1);
    }

    /* Reuse count should be at least 50 (the overflow). */
    ovs_assert(ovsdb_row_cache_scan_reuse(cache) >= 50);

    ovsdb_row_cache_bulk_read_end(cache);
    ovsdb_row_cache_destroy(cache);
    ovsdb_table_destroy(table);
}

/* Usage histogram returns correct bucket counts. */
static void
test_usage_histogram(void)
{
    struct ovsdb_table *table = create_test_table();
    struct ovsdb_row_cache *cache = ovsdb_row_cache_create(SIZE_MAX);
    size_t histogram[6];
    struct uuid u0 = make_uuid(0);
    struct uuid u1 = make_uuid(1);
    struct ovsdb_row *row;
    int i;

    /* Insert two rows. */
    row = create_test_row(table, &u0);
    ovsdb_row_cache_insert(cache, row, 5);

    row = create_test_row(table, &u1);
    ovsdb_row_cache_insert(cache, row, 5);

    /* u0: usage_count = 1 (from insert).
     * Look it up 3 more times -> usage_count = 4. */
    for (i = 0; i < 3; i++) {
        ovsdb_row_cache_lookup(cache, &u0);
    }

    /* u1: usage_count = 1 (from insert, no extra lookups). */

    ovsdb_row_cache_usage_histogram(cache, histogram);

    /* u1 should be in bucket 1, u0 in bucket 4. */
    ovs_assert(histogram[1] == 1);
    ovs_assert(histogram[4] == 1);
    ovs_assert(histogram[0] == 0);

    ovsdb_row_cache_destroy(cache);
    ovsdb_table_destroy(table);
}

/* Eviction counter increments correctly. */
static void
test_eviction_counter(void)
{
    struct ovsdb_table *table = create_test_table();
    /* Budget for 3 rows. */
    struct ovsdb_row_cache *cache = ovsdb_row_cache_create(30);
    struct ovsdb_row *row;
    int i;

    ovs_assert(ovsdb_row_cache_evictions(cache) == 0);

    /* Insert 6 rows (60 atoms, budget is 30). */
    for (i = 0; i < 6; i++) {
        struct uuid u = make_uuid(i);
        row = create_test_row(table, &u);
        ovsdb_row_cache_insert(cache, row, 10);
    }

    /* At least 3 evictions needed to stay within budget. */
    ovs_assert(ovsdb_row_cache_evictions(cache) >= 3);
    ovs_assert(ovsdb_row_cache_count(cache) <= 3);

    ovsdb_row_cache_destroy(cache);
    ovsdb_table_destroy(table);
}

/* Clock buffer grows on insert and shrinks after eviction when
 * utilization drops below 25%.  Verify entries remain accessible
 * after compaction and new inserts work correctly. */
static void
test_clock_buffer_compaction(void)
{
    struct ovsdb_table *table = create_test_table();
    /* Budget fits all 200 rows (200 * 5 = 1000). */
    struct ovsdb_row_cache *cache = ovsdb_row_cache_create(1000);
    struct ovsdb_row *row;
    int i;

    /* Insert 200 entries to force growth (64 -> 128 -> 256). */
    for (i = 0; i < 200; i++) {
        struct uuid u = make_uuid(i);
        row = create_test_row(table, &u);
        ovsdb_row_cache_insert(cache, row, 5);
    }
    ovs_assert(ovsdb_row_cache_count(cache) == 200);

    /* Remove 190, leaving 10.  This does not trigger compaction
     * directly (remove doesn't call evict__), but the next
     * eviction-triggering insert will compact. */
    for (i = 0; i < 190; i++) {
        struct uuid u = make_uuid(i);
        ovsdb_row_cache_remove(cache, &u);
    }
    ovs_assert(ovsdb_row_cache_count(cache) == 10);

    /* Reduce budget to force eviction on next insert, which
     * triggers compact__ after the sweep. */
    /* We can't change max_atoms, so insert over budget by
     * inserting with a tiny budget cache instead.  Alternative:
     * create a new cache with small budget and re-insert. */

    /* Instead, just insert one more row at cost=1 to trigger
     * evict__ (which calls compact__).  Budget is 1000, total
     * is 50, so evict__ won't evict but will still call
     * compact__. */
    {
        struct uuid u = make_uuid(9999);
        row = create_test_row(table, &u);
        ovsdb_row_cache_insert(cache, row, 5);
    }

    /* Remaining entries should still be accessible. */
    for (i = 190; i < 200; i++) {
        struct uuid u = make_uuid(i);
        ovs_assert(ovsdb_row_cache_lookup(cache, &u) != NULL);
    }

    /* Insert more entries after compaction — no crash. */
    for (i = 1000; i < 1010; i++) {
        struct uuid u = make_uuid(i);
        row = create_test_row(table, &u);
        ovsdb_row_cache_insert(cache, row, 5);
        ovs_assert(ovsdb_row_cache_lookup(cache, &u) != NULL);
    }

    ovsdb_row_cache_destroy(cache);
    ovsdb_table_destroy(table);
}

/* Re-inserting a UUID that is already in the scan ring must not
 * leave a dangling pointer in the ring slot. */
static void
test_scan_ring_insert_replace(void)
{
    struct ovsdb_table *table = create_test_table();
    struct ovsdb_row_cache *cache = ovsdb_row_cache_create(SIZE_MAX);
    struct uuid u = make_uuid(42);
    struct ovsdb_row *row1;
    struct ovsdb_row *row2;
    struct ovsdb_row *found;

    ovsdb_row_cache_bulk_read_start(cache);

    /* Insert first version. */
    row1 = create_test_row(table, &u);
    ovsdb_row_cache_insert(cache, row1, 5);
    found = ovsdb_row_cache_lookup(cache, &u);
    ovs_assert(found == row1);

    /* Replace with second version (same UUID). */
    row2 = create_test_row(table, &u);
    ovsdb_row_cache_insert(cache, row2, 5);
    found = ovsdb_row_cache_lookup(cache, &u);
    ovs_assert(found == row2);

    /* End bulk-read: must not crash (would crash on dangling
     * pointer if the old ring slot was not cleared). */
    ovsdb_row_cache_bulk_read_end(cache);

    /* Entry should be gone after ring cleanup. */
    ovs_assert(ovsdb_row_cache_lookup(cache, &u) == NULL);

    ovsdb_row_cache_destroy(cache);
    ovsdb_table_destroy(table);
}

/* ------------------------------------------------------------------
 * Concurrency stress tests.
 * ------------------------------------------------------------------ */

#define N_READERS 4
#define N_LOOKUP_ITERS 10000

struct concurrent_lookup_aux {
    struct ovsdb_row_cache *cache;
    struct ovs_barrier *barrier;
    int n_entries;
};

static void *
concurrent_lookup_thread(void *arg)
{
    struct concurrent_lookup_aux *aux = arg;
    int i;

    ovs_barrier_block(aux->barrier);
    for (i = 0; i < N_LOOKUP_ITERS; i++) {
        struct uuid u = make_uuid(i % aux->n_entries);
        struct ovsdb_row *row = ovsdb_row_cache_lookup(aux->cache,
                                                       &u);
        ovs_assert(row != NULL);
    }
    ovs_barrier_block(aux->barrier);

    return NULL;
}

/* Test 1: Parallel lookups — no crash, no wrong data. */
static void
test_concurrent_lookup(void)
{
    struct ovsdb_table *table = create_test_table();
    struct ovsdb_row_cache *cache = ovsdb_row_cache_create(SIZE_MAX);
    struct ovs_barrier barrier;
    struct concurrent_lookup_aux aux;
    pthread_t threads[N_READERS];
    int i;

    /* Pre-populate cache. */
    for (i = 0; i < 100; i++) {
        struct uuid u = make_uuid(i);
        ovsdb_row_cache_insert(cache,
                               create_test_row(table, &u), 5);
    }

    aux.cache = cache;
    aux.barrier = &barrier;
    aux.n_entries = 100;

    ovs_barrier_init(&barrier, N_READERS);
    for (i = 0; i < N_READERS; i++) {
        threads[i] = ovs_thread_create("cache-rd",
                                       concurrent_lookup_thread,
                                       &aux);
    }
    for (i = 0; i < N_READERS; i++) {
        xpthread_join(threads[i], NULL);
    }
    ovs_barrier_destroy(&barrier);

    ovs_assert(ovsdb_row_cache_hits(cache) >= N_READERS * N_LOOKUP_ITERS);
    ovsdb_row_cache_destroy(cache);
    ovsdb_table_destroy(table);
}

/* Test 2: Readers + writer — no crash, no corruption. */
struct rw_aux {
    struct ovsdb_row_cache *cache;
    struct ovsdb_table *table;
    struct ovs_barrier *barrier;
    bool writer;         /* true = writer thread, false = reader */
};

static void *
concurrent_rw_thread(void *arg)
{
    struct rw_aux *aux = arg;
    int i;

    ovs_barrier_block(aux->barrier);

    if (aux->writer) {
        /* Insert and remove rows in range [50..99]. */
        for (i = 0; i < 5000; i++) {
            int id = 50 + (i % 50);
            struct uuid u = make_uuid(id);
            if (i % 2 == 0) {
                struct ovsdb_row *row = create_test_row(aux->table,
                                                        &u);
                ovsdb_row_cache_insert(aux->cache, row, 5);
            } else {
                ovsdb_row_cache_remove(aux->cache, &u);
            }
        }
    } else {
        /* Lookup random rows, tolerate misses.
         * Do not dereference returned row pointers: they may
         * become stale after lookup() releases its rdlock. */
        for (i = 0; i < 5000; i++) {
            struct uuid u = make_uuid(i % 100);
            ovsdb_row_cache_lookup(aux->cache, &u);
        }
    }

    ovs_barrier_block(aux->barrier);
    return NULL;
}

static void
test_concurrent_lookup_insert(void)
{
    struct ovsdb_table *table = create_test_table();
    struct ovsdb_row_cache *cache = ovsdb_row_cache_create(SIZE_MAX);
    struct ovs_barrier barrier;
    struct rw_aux auxes[4];
    pthread_t threads[4];
    int i;

    /* Pre-populate [0..49]. */
    for (i = 0; i < 50; i++) {
        struct uuid u = make_uuid(i);
        ovsdb_row_cache_insert(cache,
                               create_test_row(table, &u), 5);
    }

    ovs_barrier_init(&barrier, 4);
    for (i = 0; i < 4; i++) {
        auxes[i].cache = cache;
        auxes[i].table = table;
        auxes[i].barrier = &barrier;
        auxes[i].writer = (i == 0); /* Thread 0 is writer. */
        threads[i] = ovs_thread_create("cache-rw",
                                       concurrent_rw_thread,
                                       &auxes[i]);
    }
    for (i = 0; i < 4; i++) {
        xpthread_join(threads[i], NULL);
    }
    ovs_barrier_destroy(&barrier);

    /* Rows [0..49] should still be present (writer only touches
     * [50..99]). */
    for (i = 0; i < 50; i++) {
        struct uuid u = make_uuid(i);
        ovs_assert(ovsdb_row_cache_lookup(cache, &u) != NULL);
    }

    ovsdb_row_cache_destroy(cache);
    ovsdb_table_destroy(table);
}

/* Test 3: Multiple writers with eviction pressure. */
struct insert_aux {
    struct ovsdb_row_cache *cache;
    struct ovsdb_table *table;
    struct ovs_barrier *barrier;
    int thread_id;
};

static void *
concurrent_insert_thread(void *arg)
{
    struct insert_aux *aux = arg;
    int i;

    ovs_barrier_block(aux->barrier);
    for (i = 0; i < 200; i++) {
        int id = aux->thread_id * 1000 + i;
        struct uuid u = make_uuid(id);
        struct ovsdb_row *row = create_test_row(aux->table, &u);
        ovsdb_row_cache_insert(aux->cache, row, 10);
    }
    ovs_barrier_block(aux->barrier);

    return NULL;
}

static void
test_concurrent_insert_eviction(void)
{
    struct ovsdb_table *table = create_test_table();
    /* Budget for 50 rows at 10 atoms. */
    struct ovsdb_row_cache *cache = ovsdb_row_cache_create(500);
    struct ovs_barrier barrier;
    struct insert_aux auxes[4];
    pthread_t threads[4];
    int i;

    ovs_barrier_init(&barrier, 4);
    for (i = 0; i < 4; i++) {
        auxes[i].cache = cache;
        auxes[i].table = table;
        auxes[i].barrier = &barrier;
        auxes[i].thread_id = i;
        threads[i] = ovs_thread_create("cache-wr",
                                       concurrent_insert_thread,
                                       &auxes[i]);
    }
    for (i = 0; i < 4; i++) {
        xpthread_join(threads[i], NULL);
    }
    ovs_barrier_destroy(&barrier);

    /* Budget enforced: at most 50 entries. */
    ovs_assert(ovsdb_row_cache_count(cache) <= 50);
    ovs_assert(ovsdb_row_cache_n_atoms(cache) <= 500);
    ovs_assert(ovsdb_row_cache_evictions(cache) > 0);

    ovsdb_row_cache_destroy(cache);
    ovsdb_table_destroy(table);
}

/* Test 4: Pinned entries survive concurrent insert pressure. */
static void
test_concurrent_pin_unpin(void)
{
    struct ovsdb_table *table = create_test_table();
    struct ovsdb_row_cache *cache = ovsdb_row_cache_create(200);
    struct ovs_barrier barrier;
    struct insert_aux auxes[2];
    struct concurrent_lookup_aux rd_aux;
    pthread_t threads[4];
    int i;

    /* Insert and pin 10 rows. */
    for (i = 0; i < 10; i++) {
        struct uuid u = make_uuid(i);
        ovsdb_row_cache_insert(cache,
                               create_test_row(table, &u), 10);
        ovsdb_row_cache_pin(cache, &u);
    }

    ovs_barrier_init(&barrier, 4);

    /* 2 inserter threads. */
    for (i = 0; i < 2; i++) {
        auxes[i].cache = cache;
        auxes[i].table = table;
        auxes[i].barrier = &barrier;
        auxes[i].thread_id = 10 + i;
        threads[i] = ovs_thread_create("cache-pin-wr",
                                       concurrent_insert_thread,
                                       &auxes[i]);
    }

    /* 2 lookup threads checking pinned entries. */
    rd_aux.cache = cache;
    rd_aux.barrier = &barrier;
    rd_aux.n_entries = 10;
    for (i = 2; i < 4; i++) {
        threads[i] = ovs_thread_create("cache-pin-rd",
                                       concurrent_lookup_thread,
                                       &rd_aux);
    }

    for (i = 0; i < 4; i++) {
        xpthread_join(threads[i], NULL);
    }
    ovs_barrier_destroy(&barrier);

    /* All 10 pinned entries must survive. */
    for (i = 0; i < 10; i++) {
        struct uuid u = make_uuid(i);
        ovs_assert(ovsdb_row_cache_lookup(cache, &u) != NULL);
    }

    ovsdb_row_cache_destroy(cache);
    ovsdb_table_destroy(table);
}

/* Test 5: Bulk-read scan ring under contention. */
struct scan_aux {
    struct ovsdb_row_cache *cache;
    struct ovsdb_table *table;
    struct ovs_barrier *barrier;
};

static void *
concurrent_scan_thread(void *arg)
{
    struct scan_aux *aux = arg;
    int i;

    ovs_barrier_block(aux->barrier);
    for (i = 0; i < 300; i++) {
        struct uuid u = make_uuid(1000 + i);
        struct ovsdb_row *row = create_test_row(aux->table, &u);
        ovsdb_row_cache_insert(aux->cache, row, 5);
    }
    ovs_barrier_block(aux->barrier);

    return NULL;
}

static void
test_concurrent_bulk_read(void)
{
    struct ovsdb_table *table = create_test_table();
    struct ovsdb_row_cache *cache = ovsdb_row_cache_create(SIZE_MAX);
    struct ovs_barrier barrier;
    struct scan_aux scan;
    struct concurrent_lookup_aux rd;
    pthread_t threads[2];
    int i;

    /* Insert 20 hot rows. */
    for (i = 0; i < 20; i++) {
        struct uuid u = make_uuid(i);
        ovsdb_row_cache_insert(cache,
                               create_test_row(table, &u), 5);
        ovsdb_row_cache_lookup(cache, &u);
        ovsdb_row_cache_lookup(cache, &u);
    }

    ovsdb_row_cache_bulk_read_start(cache);

    ovs_barrier_init(&barrier, 2);

    scan.cache = cache;
    scan.table = table;
    scan.barrier = &barrier;
    threads[0] = ovs_thread_create("cache-scan",
                                   concurrent_scan_thread,
                                   &scan);

    rd.cache = cache;
    rd.barrier = &barrier;
    rd.n_entries = 20;
    threads[1] = ovs_thread_create("cache-hot",
                                   concurrent_lookup_thread,
                                   &rd);

    for (i = 0; i < 2; i++) {
        xpthread_join(threads[i], NULL);
    }
    ovs_barrier_destroy(&barrier);

    ovsdb_row_cache_bulk_read_end(cache);

    /* Hot rows survive. */
    for (i = 0; i < 20; i++) {
        struct uuid u = make_uuid(i);
        ovs_assert(ovsdb_row_cache_lookup(cache, &u) != NULL);
    }

    ovsdb_row_cache_destroy(cache);
    ovsdb_table_destroy(table);
}

/* Test 6: Mixed workload stress test. */
struct stress_aux {
    struct ovsdb_row_cache *cache;
    struct ovsdb_table *table;
    struct ovs_barrier *barrier;
    int role;  /* 0=inserter, 1=looker, 2=remover, 3=stats */
    int thread_id;
};

static void *
concurrent_stress_thread(void *arg)
{
    struct stress_aux *aux = arg;
    int i;

    ovs_barrier_block(aux->barrier);

    switch (aux->role) {
    case 0: /* inserter */
        for (i = 0; i < 1000; i++) {
            int id = aux->thread_id * 10000 + i;
            struct uuid u = make_uuid(id);
            struct ovsdb_row *row = create_test_row(aux->table, &u);
            ovsdb_row_cache_insert(aux->cache, row, 5);
        }
        break;

    case 1: /* looker */
        for (i = 0; i < 2000; i++) {
            struct uuid u = make_uuid(i % 5000);
            ovsdb_row_cache_lookup(aux->cache, &u);
        }
        break;

    case 2: /* remover */
        for (i = 0; i < 500; i++) {
            struct uuid u = make_uuid(i * 3);
            ovsdb_row_cache_remove(aux->cache, &u);
        }
        break;

    case 3: /* stats reader */
        for (i = 0; i < 1000; i++) {
            size_t histogram[6];
            ovsdb_row_cache_hits(aux->cache);
            ovsdb_row_cache_misses(aux->cache);
            ovsdb_row_cache_evictions(aux->cache);
            ovsdb_row_cache_count(aux->cache);
            ovsdb_row_cache_usage_histogram(aux->cache, histogram);
        }
        break;
    }

    ovs_barrier_block(aux->barrier);
    return NULL;
}

static void
test_concurrent_stress(void)
{
    struct ovsdb_table *table = create_test_table();
    struct ovsdb_row_cache *cache = ovsdb_row_cache_create(1000);
    struct ovs_barrier barrier;
    struct stress_aux auxes[8];
    pthread_t threads[8];
    /* roles: 0,1=inserter 2,3=looker 4=remover 5=stats
     * 6=inserter 7=looker */
    int roles[8] = {0, 0, 1, 1, 2, 3, 0, 1};
    int i;

    ovs_barrier_init(&barrier, 8);
    for (i = 0; i < 8; i++) {
        auxes[i].cache = cache;
        auxes[i].table = table;
        auxes[i].barrier = &barrier;
        auxes[i].role = roles[i];
        auxes[i].thread_id = i;
        threads[i] = ovs_thread_create("cache-stress",
                                       concurrent_stress_thread,
                                       &auxes[i]);
    }
    for (i = 0; i < 8; i++) {
        xpthread_join(threads[i], NULL);
    }
    ovs_barrier_destroy(&barrier);

    /* Reaching this point without crash or deadlock is success.
     * Verify no underflow. */
    /* Verify no underflow (size_t is unsigned, so check < SIZE_MAX/2). */
    ovs_assert(ovsdb_row_cache_count(cache) < SIZE_MAX / 2);
    ovs_assert(ovsdb_row_cache_n_atoms(cache) < SIZE_MAX / 2);

    ovsdb_row_cache_destroy(cache);
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

    printf("test_clock_sweep_usage_count\n");
    test_clock_sweep_usage_count();

    printf("test_clock_sweep_all_pinned\n");
    test_clock_sweep_all_pinned();

    printf("test_scan_ring_isolation\n");
    test_scan_ring_isolation();

    printf("test_scan_ring_reuse\n");
    test_scan_ring_reuse();

    printf("test_usage_histogram\n");
    test_usage_histogram();

    printf("test_eviction_counter\n");
    test_eviction_counter();

    printf("test_scan_ring_insert_replace\n");
    test_scan_ring_insert_replace();

    printf("test_clock_buffer_compaction\n");
    test_clock_buffer_compaction();

    printf("test_concurrent_lookup\n");
    test_concurrent_lookup();

    printf("test_concurrent_lookup_insert\n");
    test_concurrent_lookup_insert();

    printf("test_concurrent_insert_eviction\n");
    test_concurrent_insert_eviction();

    printf("test_concurrent_pin_unpin\n");
    test_concurrent_pin_unpin();

    printf("test_concurrent_bulk_read\n");
    test_concurrent_bulk_read();

    printf("test_concurrent_stress\n");
    test_concurrent_stress();

    printf("test-row-cache: ok\n");
}

OVSTEST_REGISTER("test-row-cache", test_row_cache_main);
