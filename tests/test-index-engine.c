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

#include "ovsdb/index-engine.h"
#include "ovsdb/disk-store.h"
#include "openvswitch/json.h"
#include "openvswitch/uuid.h"
#include "ovsdb-data.h"
#include "ovsdb-types.h"
#include "util.h"

/* ------------------------------------------------------------------ */
/* Helpers: create fake clustered entries for testing.                 */
/* ------------------------------------------------------------------ */

static struct disk_store_index_entry *
make_entry(const char *uuid_str, off_t offset)
{
    struct disk_store_index_entry *e = xzalloc(sizeof *e);
    uuid_from_string(&e->uuid, uuid_str);
    e->offset = offset;
    e->length = 100;
    e->table_name = xstrdup("TestTable");
    e->deleted = false;
    return e;
}

static void
free_entry(struct disk_store_index_entry *e)
{
    free(e->table_name);
    free(e->name_value);
    free(e);
}

/* ------------------------------------------------------------------ */
/* Test 1: HASH create + add + lookup string key.                     */
/* ------------------------------------------------------------------ */

static void
test_hash_string_lookup(void)
{
    struct ovsdb_index_spec spec = {
        .type = OVSDB_IDX_HASH,
        .name = "idx_name",
        .column_name = "name",
        .key_type = OVSDB_TYPE_STRING,
    };
    struct ovsdb_index *idx = ovsdb_index_create(&spec);
    struct disk_store_index_entry *e1;
    struct disk_store_index_entry *e2;
    struct disk_store_index_entry *result;
    union ovsdb_atom key;

    e1 = make_entry("11111111-1111-1111-1111-111111111111", 1000);
    e2 = make_entry("22222222-2222-2222-2222-222222222222", 2000);

    key.s = json_string_create("router-alpha");
    ovsdb_index_add(idx, &key, e1);
    json_destroy(key.s);

    key.s = json_string_create("router-beta");
    ovsdb_index_add(idx, &key, e2);
    json_destroy(key.s);

    ovs_assert(ovsdb_index_get_count(idx) == 2);

    /* Lookup existing. */
    key.s = json_string_create("router-alpha");
    result = ovsdb_index_lookup(idx, &key);
    ovs_assert(result == e1);
    ovs_assert(result->offset == 1000);
    json_destroy(key.s);

    /* Lookup non-existent. */
    key.s = json_string_create("router-gamma");
    result = ovsdb_index_lookup(idx, &key);
    ovs_assert(result == NULL);
    json_destroy(key.s);

    ovsdb_index_destroy(idx);
    free_entry(e1);
    free_entry(e2);
    printf("hash_string_lookup: PASSED\n");
}

/* ------------------------------------------------------------------ */
/* Test 2: HASH integer key.                                          */
/* ------------------------------------------------------------------ */

static void
test_hash_integer_lookup(void)
{
    struct ovsdb_index_spec spec = {
        .type = OVSDB_IDX_HASH,
        .name = "idx_priority",
        .column_name = "priority",
        .key_type = OVSDB_TYPE_INTEGER,
    };
    struct ovsdb_index *idx = ovsdb_index_create(&spec);
    struct disk_store_index_entry *e1;
    struct disk_store_index_entry *result;
    union ovsdb_atom key;

    e1 = make_entry("33333333-3333-3333-3333-333333333333", 3000);

    key.integer = 42;
    ovsdb_index_add(idx, &key, e1);

    key.integer = 42;
    result = ovsdb_index_lookup(idx, &key);
    ovs_assert(result == e1);

    key.integer = 99;
    result = ovsdb_index_lookup(idx, &key);
    ovs_assert(result == NULL);

    ovsdb_index_destroy(idx);
    free_entry(e1);
    printf("hash_integer_lookup: PASSED\n");
}

/* ------------------------------------------------------------------ */
/* Test 3: HASH UUID key.                                             */
/* ------------------------------------------------------------------ */

static void
test_hash_uuid_lookup(void)
{
    struct ovsdb_index_spec spec = {
        .type = OVSDB_IDX_HASH,
        .name = "idx_datapath",
        .column_name = "logical_datapath",
        .key_type = OVSDB_TYPE_UUID,
    };
    struct ovsdb_index *idx = ovsdb_index_create(&spec);
    struct disk_store_index_entry *e1;
    struct disk_store_index_entry *result;
    union ovsdb_atom key;
    struct uuid target;

    e1 = make_entry("44444444-4444-4444-4444-444444444444", 4000);
    uuid_from_string(&target, "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");

    key.uuid = target;
    ovsdb_index_add(idx, &key, e1);

    key.uuid = target;
    result = ovsdb_index_lookup(idx, &key);
    ovs_assert(result == e1);

    ovsdb_index_destroy(idx);
    free_entry(e1);
    printf("hash_uuid_lookup: PASSED\n");
}

/* ------------------------------------------------------------------ */
/* Test 4: HASH remove.                                               */
/* ------------------------------------------------------------------ */

static void
test_hash_remove(void)
{
    struct ovsdb_index_spec spec = {
        .type = OVSDB_IDX_HASH,
        .name = "idx_name",
        .column_name = "name",
        .key_type = OVSDB_TYPE_STRING,
    };
    struct ovsdb_index *idx = ovsdb_index_create(&spec);
    struct disk_store_index_entry *e1;
    struct disk_store_index_entry *result;
    union ovsdb_atom key;

    e1 = make_entry("55555555-5555-5555-5555-555555555555", 5000);

    key.s = json_string_create("to-remove");
    ovsdb_index_add(idx, &key, e1);
    ovs_assert(ovsdb_index_get_count(idx) == 1);

    ovsdb_index_remove(idx, &key, e1);
    ovs_assert(ovsdb_index_get_count(idx) == 0);

    result = ovsdb_index_lookup(idx, &key);
    ovs_assert(result == NULL);

    json_destroy(key.s);
    ovsdb_index_destroy(idx);
    free_entry(e1);
    printf("hash_remove: PASSED\n");
}

/* ------------------------------------------------------------------ */
/* Test 5: Multiple indexes on same entries (no interference).        */
/* ------------------------------------------------------------------ */

static void
test_multi_index(void)
{
    struct ovsdb_index_spec spec_name = {
        .type = OVSDB_IDX_HASH,
        .name = "idx_name",
        .column_name = "name",
        .key_type = OVSDB_TYPE_STRING,
    };
    struct ovsdb_index_spec spec_port = {
        .type = OVSDB_IDX_HASH,
        .name = "idx_port",
        .column_name = "logical_port",
        .key_type = OVSDB_TYPE_STRING,
    };
    struct ovsdb_index *idx_name = ovsdb_index_create(&spec_name);
    struct ovsdb_index *idx_port = ovsdb_index_create(&spec_port);
    struct disk_store_index_entry *e1;
    struct disk_store_index_entry *r1;
    struct disk_store_index_entry *r2;
    union ovsdb_atom key;

    e1 = make_entry("66666666-6666-6666-6666-666666666666", 6000);

    /* Same entry in both indexes with different keys. */
    key.s = json_string_create("my-router");
    ovsdb_index_add(idx_name, &key, e1);
    json_destroy(key.s);

    key.s = json_string_create("port-eth0");
    ovsdb_index_add(idx_port, &key, e1);
    json_destroy(key.s);

    /* Both should find the same entry. */
    key.s = json_string_create("my-router");
    r1 = ovsdb_index_lookup(idx_name, &key);
    json_destroy(key.s);

    key.s = json_string_create("port-eth0");
    r2 = ovsdb_index_lookup(idx_port, &key);
    json_destroy(key.s);

    ovs_assert(r1 == e1);
    ovs_assert(r2 == e1);
    ovs_assert(r1 == r2);  /* Same clustered entry. */

    ovsdb_index_destroy(idx_name);
    ovsdb_index_destroy(idx_port);
    free_entry(e1);
    printf("multi_index: PASSED\n");
}

/* ------------------------------------------------------------------ */
/* Test 6: Index set — find_for_column and find_bloom.                */
/* ------------------------------------------------------------------ */

static void
test_index_set(void)
{
    struct ovsdb_index_spec spec_bloom = {
        .type = OVSDB_IDX_BLOOM,
        .name = "bloom",
    };
    struct ovsdb_index_spec spec_name = {
        .type = OVSDB_IDX_HASH,
        .name = "idx_name",
        .column_name = "name",
        .key_type = OVSDB_TYPE_STRING,
    };
    struct ovsdb_index_set set;

    ovsdb_index_set_init(&set);
    ovsdb_index_set_add(&set, ovsdb_index_create(&spec_bloom));
    ovsdb_index_set_add(&set, ovsdb_index_create(&spec_name));

    ovs_assert(set.n_indexes == 2);
    ovs_assert(ovsdb_index_set_find_bloom(&set) != NULL);
    ovs_assert(ovsdb_index_set_find_for_column(&set, "name") != NULL);
    ovs_assert(ovsdb_index_set_find_for_column(&set, "nonexistent") == NULL);

    ovsdb_index_set_destroy(&set);
    printf("index_set: PASSED\n");
}

/* ------------------------------------------------------------------ */
/* Test 7: Pointer identity — lookup returns same entry pointer.      */
/* ------------------------------------------------------------------ */

static void
test_pointer_identity(void)
{
    struct ovsdb_index_spec spec = {
        .type = OVSDB_IDX_HASH,
        .name = "idx_name",
        .column_name = "name",
        .key_type = OVSDB_TYPE_STRING,
    };
    struct ovsdb_index *idx = ovsdb_index_create(&spec);
    struct disk_store_index_entry *e1;
    struct disk_store_index_entry *r1;
    struct disk_store_index_entry *r2;
    union ovsdb_atom key;

    e1 = make_entry("77777777-7777-7777-7777-777777777777", 7000);

    key.s = json_string_create("test-ptr");
    ovsdb_index_add(idx, &key, e1);

    /* Two lookups return the same pointer. */
    r1 = ovsdb_index_lookup(idx, &key);
    r2 = ovsdb_index_lookup(idx, &key);
    ovs_assert(r1 == r2);
    ovs_assert(r1 == e1);

    json_destroy(key.s);
    ovsdb_index_destroy(idx);
    free_entry(e1);
    printf("pointer_identity: PASSED\n");
}

/* ------------------------------------------------------------------ */
/* Test 8: 10K entries stress test.                                   */
/* ------------------------------------------------------------------ */

#define STRESS_N 10000

static void
test_stress(void)
{
    struct ovsdb_index_spec spec = {
        .type = OVSDB_IDX_HASH,
        .name = "idx_name",
        .column_name = "name",
        .key_type = OVSDB_TYPE_STRING,
    };
    struct ovsdb_index *idx = ovsdb_index_create(&spec);
    struct disk_store_index_entry **entries;
    size_t i;

    entries = xmalloc(STRESS_N * sizeof *entries);

    /* Add 10K entries. */
    for (i = 0; i < STRESS_N; i++) {
        char uuid_str[UUID_LEN + 1];
        char name[32];
        union ovsdb_atom key;

        snprintf(uuid_str, sizeof uuid_str,
                 "%08x-0000-0000-0000-%012x",
                 (unsigned) i, (unsigned) i);
        entries[i] = make_entry(uuid_str, (off_t)(i * 256));

        snprintf(name, sizeof name, "entry-%"PRIuSIZE, i);
        key.s = json_string_create(name);
        ovsdb_index_add(idx, &key, entries[i]);
        json_destroy(key.s);
    }

    ovs_assert(ovsdb_index_get_count(idx) == STRESS_N);

    /* Lookup all 10K. */
    for (i = 0; i < STRESS_N; i++) {
        char name[32];
        union ovsdb_atom key;
        struct disk_store_index_entry *result;

        snprintf(name, sizeof name, "entry-%"PRIuSIZE, i);
        key.s = json_string_create(name);
        result = ovsdb_index_lookup(idx, &key);
        ovs_assert(result == entries[i]);
        ovs_assert(result->offset == (off_t)(i * 256));
        json_destroy(key.s);
    }

    /* Remove all 10K. */
    for (i = 0; i < STRESS_N; i++) {
        char name[32];
        union ovsdb_atom key;

        snprintf(name, sizeof name, "entry-%"PRIuSIZE, i);
        key.s = json_string_create(name);
        ovsdb_index_remove(idx, &key, entries[i]);
        json_destroy(key.s);
    }

    ovs_assert(ovsdb_index_get_count(idx) == 0);

    ovsdb_index_destroy(idx);
    for (i = 0; i < STRESS_N; i++) {
        free_entry(entries[i]);
    }
    free(entries);
    printf("stress: PASSED (%d entries)\n", STRESS_N);
}

/* ------------------------------------------------------------------ */
/* Test 9: Metadata.                                                  */
/* ------------------------------------------------------------------ */

static void
test_metadata(void)
{
    struct ovsdb_index_spec spec = {
        .type = OVSDB_IDX_HASH,
        .name = "idx_name",
        .column_name = "name",
        .key_type = OVSDB_TYPE_STRING,
    };
    struct ovsdb_index *idx = ovsdb_index_create(&spec);

    ovs_assert(ovsdb_index_get_type(idx) == OVSDB_IDX_HASH);
    ovs_assert(!strcmp(ovsdb_index_get_name(idx), "idx_name"));
    ovs_assert(ovsdb_index_get_count(idx) == 0);

    ovsdb_index_destroy(idx);
    printf("metadata: PASSED\n");
}

/* ------------------------------------------------------------------ */
/* Main.                                                              */
/* ------------------------------------------------------------------ */

static struct {
    const char *name;
    void (*fn)(void);
} tests[] = {
    { "hash_string_lookup", test_hash_string_lookup },
    { "hash_integer_lookup", test_hash_integer_lookup },
    { "hash_uuid_lookup", test_hash_uuid_lookup },
    { "hash_remove", test_hash_remove },
    { "multi_index", test_multi_index },
    { "index_set", test_index_set },
    { "pointer_identity", test_pointer_identity },
    { "stress", test_stress },
    { "metadata", test_metadata },
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
