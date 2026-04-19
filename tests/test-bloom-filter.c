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
#undef NDEBUG
#include <stdio.h>
#include <string.h>

#include "ovstest.h"
#include "util.h"
#include "openvswitch/uuid.h"
#include "uuid.h"
#include "ovsdb/bloom-filter.h"

/* Test A1: Basic insert and lookup. */
static void
test_basic_insert_lookup(void)
{
    struct ovsdb_bloom_filter *bf;
    struct uuid uuids[50];
    int i;

    bf = ovsdb_bloom_filter_create(100);
    ovs_assert(ovsdb_bloom_filter_count(bf) == 0);

    /* Insert 50 UUIDs. */
    for (i = 0; i < 50; i++) {
        uuid_generate(&uuids[i]);
        ovsdb_bloom_filter_add(bf, &uuids[i]);
    }
    ovs_assert(ovsdb_bloom_filter_count(bf) == 50);

    /* All inserted UUIDs must return true. */
    for (i = 0; i < 50; i++) {
        ovs_assert(ovsdb_bloom_filter_may_contain(bf, &uuids[i]));
    }

    /* Most random UUIDs should return false. */
    int false_positives = 0;
    for (i = 0; i < 50; i++) {
        struct uuid random;
        uuid_generate(&random);
        if (ovsdb_bloom_filter_may_contain(bf, &random)) {
            false_positives++;
        }
    }
    /* With 50 keys in a 1000-bit filter, false positive rate
     * should be very low.  Allow up to 5 out of 50. */
    ovs_assert(false_positives <= 5);

    ovsdb_bloom_filter_destroy(bf);
}

/* Test A2: False positive rate verification. */
static void
test_false_positive_rate(void)
{
    struct ovsdb_bloom_filter *bf;
    struct uuid *inserted;
    int n = 10000;
    int false_positives = 0;
    int i;

    bf = ovsdb_bloom_filter_create(n);
    inserted = xmalloc(n * sizeof *inserted);

    /* Insert 10,000 random UUIDs. */
    for (i = 0; i < n; i++) {
        uuid_generate(&inserted[i]);
        ovsdb_bloom_filter_add(bf, &inserted[i]);
    }

    /* Test 10,000 DIFFERENT random UUIDs (known absent). */
    for (i = 0; i < n; i++) {
        struct uuid test;
        uuid_generate(&test);
        if (ovsdb_bloom_filter_may_contain(bf, &test)) {
            false_positives++;
        }
    }

    /* Theoretical FPR at 10 bits/key, 7 hashes ≈ 0.82%.
     * Allow up to 2% for safety margin. */
    double fpr = (double) false_positives / n * 100.0;
    printf("  false positive rate: %.2f%% (%d/%d)\n",
           fpr, false_positives, n);
    ovs_assert(fpr < 2.0);

    free(inserted);
    ovsdb_bloom_filter_destroy(bf);
}

/* Test A3: Empty filter returns false. */
static void
test_empty_filter(void)
{
    struct ovsdb_bloom_filter *bf;
    struct uuid uuid;
    int i;

    bf = ovsdb_bloom_filter_create(100);

    for (i = 0; i < 100; i++) {
        uuid_generate(&uuid);
        ovs_assert(!ovsdb_bloom_filter_may_contain(bf, &uuid));
    }

    ovsdb_bloom_filter_destroy(bf);
}

/* Test A4: Capacity zero edge case. */
static void
test_capacity_zero(void)
{
    struct ovsdb_bloom_filter *bf;
    struct uuid uuid;

    bf = ovsdb_bloom_filter_create(0);
    ovs_assert(bf != NULL);
    ovs_assert(ovsdb_bloom_filter_count(bf) == 0);

    uuid_generate(&uuid);
    ovs_assert(!ovsdb_bloom_filter_may_contain(bf, &uuid));

    /* Insert should still work (minimum 64 bits). */
    ovsdb_bloom_filter_add(bf, &uuid);
    ovs_assert(ovsdb_bloom_filter_may_contain(bf, &uuid));

    ovsdb_bloom_filter_destroy(bf);
}

/* Test A5: Large scale — 100K entries. */
static void
test_large_scale(void)
{
    struct ovsdb_bloom_filter *bf;
    int n = 100000;
    struct uuid *uuids;
    int i;

    bf = ovsdb_bloom_filter_create(n);
    uuids = xmalloc(n * sizeof *uuids);

    for (i = 0; i < n; i++) {
        uuid_generate(&uuids[i]);
        ovsdb_bloom_filter_add(bf, &uuids[i]);
    }

    /* All must be found. */
    for (i = 0; i < n; i++) {
        ovs_assert(ovsdb_bloom_filter_may_contain(bf, &uuids[i]));
    }

    /* Memory should be approximately 100K * 10 / 8 = 125,000 bytes. */
    size_t size = ovsdb_bloom_filter_size_bytes(bf);
    ovs_assert(size >= 120000 && size <= 130000);

    printf("  100K entries: %"PRIuSIZE" bytes\n", size);

    free(uuids);
    ovsdb_bloom_filter_destroy(bf);
}

/* Test A6: Rebuild (clear) clears old data. */
static void
test_rebuild(void)
{
    struct ovsdb_bloom_filter *bf;
    struct uuid old_uuids[100];
    struct uuid new_uuids[50];
    int i;
    int old_found = 0;

    bf = ovsdb_bloom_filter_create(100);

    /* Insert 100 old UUIDs. */
    for (i = 0; i < 100; i++) {
        uuid_generate(&old_uuids[i]);
        ovsdb_bloom_filter_add(bf, &old_uuids[i]);
    }
    ovs_assert(ovsdb_bloom_filter_count(bf) == 100);

    /* Clear and insert 50 new UUIDs. */
    ovsdb_bloom_filter_clear(bf);
    ovs_assert(ovsdb_bloom_filter_count(bf) == 0);

    for (i = 0; i < 50; i++) {
        uuid_generate(&new_uuids[i]);
        ovsdb_bloom_filter_add(bf, &new_uuids[i]);
    }

    /* New UUIDs must be found. */
    for (i = 0; i < 50; i++) {
        ovs_assert(ovsdb_bloom_filter_may_contain(bf, &new_uuids[i]));
    }

    /* Most old UUIDs should NOT be found (filter was cleared). */
    for (i = 0; i < 100; i++) {
        if (ovsdb_bloom_filter_may_contain(bf, &old_uuids[i])) {
            old_found++;
        }
    }
    /* With 50 keys in a 1000-bit filter, at most a few false
     * positives from old UUIDs. */
    ovs_assert(old_found < 10);

    ovsdb_bloom_filter_destroy(bf);
}

static void
test_bloom_filter_main(int argc OVS_UNUSED,
                       char *argv[] OVS_UNUSED)
{
    printf("test-bloom-filter: basic_insert_lookup\n");
    test_basic_insert_lookup();

    printf("test-bloom-filter: false_positive_rate\n");
    test_false_positive_rate();

    printf("test-bloom-filter: empty_filter\n");
    test_empty_filter();

    printf("test-bloom-filter: capacity_zero\n");
    test_capacity_zero();

    printf("test-bloom-filter: large_scale\n");
    test_large_scale();

    printf("test-bloom-filter: rebuild\n");
    test_rebuild();

    printf("test-bloom-filter: ok\n");
}

OVSTEST_REGISTER("test-bloom-filter", test_bloom_filter_main);
