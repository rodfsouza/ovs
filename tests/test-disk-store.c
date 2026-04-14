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
#include <unistd.h>

#include "ovstest.h"
#include "util.h"
#include "openvswitch/uuid.h"
#include "openvswitch/json.h"
#include "ovsdb/disk-store.h"
#include "ovsdb/ovsdb.h"
#include "ovsdb/table.h"
#include "ovsdb/row.h"
#include "ovsdb/column.h"
#include "ovsdb/storage.h"
#include "ovsdb-error.h"

static void
check_ovsdb_error(struct ovsdb_error *error)
{
    if (error) {
        char *s = ovsdb_error_to_string_free(error);
        ovs_fatal(0, "%s", s);
    }
}

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

static struct ovsdb *
create_test_db(struct ovsdb_schema *schema)
{
    return ovsdb_create(schema,
                        ovsdb_storage_create_unbacked(NULL));
}

static char *
make_test_filename(const char *test_name)
{
    return xasprintf("test-disk-store-%s.db", test_name);
}

static struct ovsdb_row *
create_test_row(struct ovsdb_table *table,
                const struct uuid *uuid)
{
    struct ovsdb_row *row = ovsdb_row_create(table);
    *ovsdb_row_get_uuid_rw(row) = *uuid;
    return row;
}

/* Test 1: Create, close, reopen -- verify no crash. */
static void
test_create_open_close(void)
{
    struct ovsdb_schema *schema;
    struct ovsdb *db;
    struct ovsdb_disk_store *ds;
    char *filename;

    schema = create_test_schema();
    db = create_test_db(ovsdb_schema_clone(schema));
    filename = make_test_filename("open-close");

    ds = ovsdb_disk_store_open(filename, schema);
    ovs_assert(ds != NULL);
    ovsdb_disk_store_close(ds);

    /* Reopen the same file. */
    ds = ovsdb_disk_store_open(filename, schema);
    ovs_assert(ds != NULL);
    ovsdb_disk_store_close(ds);

    ovsdb_destroy(db);
    ovsdb_schema_destroy(schema);
    unlink(filename);
    free(filename);
}

/* Test 2: Write one row, read it back. */
static void
test_write_read_single(void)
{
    struct ovsdb_schema *schema;
    struct ovsdb *db;
    struct ovsdb_table *table;
    struct ovsdb_disk_store *ds;
    struct ovsdb_row *row;
    struct ovsdb_row *read_row;
    struct uuid uuid;
    char *filename;

    schema = create_test_schema();
    db = create_test_db(ovsdb_schema_clone(schema));
    table = ovsdb_get_table(db, "test");
    filename = make_test_filename("write-read-single");

    ds = ovsdb_disk_store_open(filename, schema);
    ovs_assert(ds != NULL);

    uuid_generate(&uuid);
    row = create_test_row(table, &uuid);
    check_ovsdb_error(ovsdb_disk_store_write_row(ds, row));
    ovsdb_row_destroy(row);

    read_row = ovsdb_disk_store_read_row(ds, table, &uuid);
    ovs_assert(read_row != NULL);
    ovs_assert(uuid_equals(ovsdb_row_get_uuid(read_row), &uuid));
    ovsdb_row_destroy(read_row);

    ovsdb_disk_store_close(ds);
    ovsdb_destroy(db);
    ovsdb_schema_destroy(schema);
    unlink(filename);
    free(filename);
}

/* Test 3: Write 100 rows, read each back. */
static void
test_write_read_many(void)
{
    struct ovsdb_schema *schema;
    struct ovsdb *db;
    struct ovsdb_table *table;
    struct ovsdb_disk_store *ds;
    struct uuid uuids[100];
    char *filename;
    int i;

    schema = create_test_schema();
    db = create_test_db(ovsdb_schema_clone(schema));
    table = ovsdb_get_table(db, "test");
    filename = make_test_filename("write-read-many");

    ds = ovsdb_disk_store_open(filename, schema);
    ovs_assert(ds != NULL);

    for (i = 0; i < 100; i++) {
        struct ovsdb_row *row;

        uuid_generate(&uuids[i]);
        row = create_test_row(table, &uuids[i]);
        check_ovsdb_error(ovsdb_disk_store_write_row(ds, row));
        ovsdb_row_destroy(row);
    }

    ovs_assert(ovsdb_disk_store_count(ds, "test") == 100);

    for (i = 0; i < 100; i++) {
        struct ovsdb_row *read_row;

        ovs_assert(ovsdb_disk_store_contains(ds, &uuids[i]));
        read_row = ovsdb_disk_store_read_row(ds, table,
                                             &uuids[i]);
        ovs_assert(read_row != NULL);
        ovs_assert(uuid_equals(ovsdb_row_get_uuid(read_row),
                               &uuids[i]));
        ovsdb_row_destroy(read_row);
    }

    ovsdb_disk_store_close(ds);
    ovsdb_destroy(db);
    ovsdb_schema_destroy(schema);
    unlink(filename);
    free(filename);
}

/* Test 4: Write a row, delete it, verify gone. */
static void
test_delete(void)
{
    struct ovsdb_schema *schema;
    struct ovsdb *db;
    struct ovsdb_table *table;
    struct ovsdb_disk_store *ds;
    struct ovsdb_row *row;
    struct ovsdb_row *read_row;
    struct uuid uuid;
    char *filename;

    schema = create_test_schema();
    db = create_test_db(ovsdb_schema_clone(schema));
    table = ovsdb_get_table(db, "test");
    filename = make_test_filename("delete");

    ds = ovsdb_disk_store_open(filename, schema);
    ovs_assert(ds != NULL);

    uuid_generate(&uuid);
    row = create_test_row(table, &uuid);
    check_ovsdb_error(ovsdb_disk_store_write_row(ds, row));
    ovsdb_row_destroy(row);

    ovs_assert(ovsdb_disk_store_contains(ds, &uuid));
    check_ovsdb_error(ovsdb_disk_store_delete_row(ds, &uuid));
    ovs_assert(!ovsdb_disk_store_contains(ds, &uuid));

    read_row = ovsdb_disk_store_read_row(ds, table, &uuid);
    ovs_assert(read_row == NULL);

    ovsdb_disk_store_close(ds);
    ovsdb_destroy(db);
    ovsdb_schema_destroy(schema);
    unlink(filename);
    free(filename);
}

/* Test 5: Iterate over 50 rows with cursor. */
static void
test_iterate(void)
{
    struct ovsdb_schema *schema;
    struct ovsdb *db;
    struct ovsdb_table *table;
    struct ovsdb_disk_store *ds;
    struct ovsdb_disk_store_cursor *cursor;
    char *filename;
    int i;
    int count;

    schema = create_test_schema();
    db = create_test_db(ovsdb_schema_clone(schema));
    table = ovsdb_get_table(db, "test");
    filename = make_test_filename("iterate");

    ds = ovsdb_disk_store_open(filename, schema);
    ovs_assert(ds != NULL);

    for (i = 0; i < 50; i++) {
        struct ovsdb_row *row;
        struct uuid uuid;

        uuid_generate(&uuid);
        row = create_test_row(table, &uuid);
        check_ovsdb_error(ovsdb_disk_store_write_row(ds, row));
        ovsdb_row_destroy(row);
    }

    cursor = ovsdb_disk_store_cursor_open(ds, "test");
    ovs_assert(cursor != NULL);

    count = 0;
    for (;;) {
        struct ovsdb_row *row;

        row = ovsdb_disk_store_cursor_next(cursor, table);
        if (!row) {
            break;
        }
        count++;
        ovsdb_row_destroy(row);
    }
    ovs_assert(count == 50);

    ovsdb_disk_store_cursor_close(cursor);
    ovsdb_disk_store_close(ds);
    ovsdb_destroy(db);
    ovsdb_schema_destroy(schema);
    unlink(filename);
    free(filename);
}

/* Test 6: Write 100, delete 50, compact, verify 50 remain. */
static void
test_compact(void)
{
    struct ovsdb_schema *schema;
    struct ovsdb *db;
    struct ovsdb_table *table;
    struct ovsdb_disk_store *ds;
    struct uuid uuids[100];
    char *filename;
    int i;

    schema = create_test_schema();
    db = create_test_db(ovsdb_schema_clone(schema));
    table = ovsdb_get_table(db, "test");
    filename = make_test_filename("compact");

    ds = ovsdb_disk_store_open(filename, schema);
    ovs_assert(ds != NULL);

    for (i = 0; i < 100; i++) {
        struct ovsdb_row *row;

        uuid_generate(&uuids[i]);
        row = create_test_row(table, &uuids[i]);
        check_ovsdb_error(ovsdb_disk_store_write_row(ds, row));
        ovsdb_row_destroy(row);
    }

    /* Delete the first 50 rows. */
    for (i = 0; i < 50; i++) {
        check_ovsdb_error(
            ovsdb_disk_store_delete_row(ds, &uuids[i]));
    }

    check_ovsdb_error(ovsdb_disk_store_compact(ds));
    ovs_assert(ovsdb_disk_store_count(ds, "test") == 50);

    /* Verify remaining 50 are readable. */
    for (i = 50; i < 100; i++) {
        struct ovsdb_row *read_row;

        read_row = ovsdb_disk_store_read_row(ds, table,
                                             &uuids[i]);
        ovs_assert(read_row != NULL);
        ovs_assert(uuid_equals(ovsdb_row_get_uuid(read_row),
                               &uuids[i]));
        ovsdb_row_destroy(read_row);
    }

    /* Verify deleted 50 are gone. */
    for (i = 0; i < 50; i++) {
        ovs_assert(!ovsdb_disk_store_contains(ds, &uuids[i]));
    }

    ovsdb_disk_store_close(ds);
    ovsdb_destroy(db);
    ovsdb_schema_destroy(schema);
    unlink(filename);
    free(filename);
}

/* Test 7: Close and reopen -- verify persistence. */
static void
test_reopen_persistence(void)
{
    struct ovsdb_schema *schema;
    struct ovsdb *db;
    struct ovsdb_table *table;
    struct ovsdb_disk_store *ds;
    struct uuid uuids[10];
    char *filename;
    int i;

    schema = create_test_schema();
    db = create_test_db(ovsdb_schema_clone(schema));
    table = ovsdb_get_table(db, "test");
    filename = make_test_filename("reopen");

    ds = ovsdb_disk_store_open(filename, schema);
    ovs_assert(ds != NULL);

    for (i = 0; i < 10; i++) {
        struct ovsdb_row *row;

        uuid_generate(&uuids[i]);
        row = create_test_row(table, &uuids[i]);
        check_ovsdb_error(ovsdb_disk_store_write_row(ds, row));
        ovsdb_row_destroy(row);
    }

    ovsdb_disk_store_close(ds);

    /* Reopen and verify all rows persisted. */
    ds = ovsdb_disk_store_open(filename, schema);
    ovs_assert(ds != NULL);
    ovs_assert(ovsdb_disk_store_count(ds, "test") == 10);

    for (i = 0; i < 10; i++) {
        struct ovsdb_row *read_row;

        ovs_assert(ovsdb_disk_store_contains(ds, &uuids[i]));
        read_row = ovsdb_disk_store_read_row(ds, table,
                                             &uuids[i]);
        ovs_assert(read_row != NULL);
        ovs_assert(uuid_equals(ovsdb_row_get_uuid(read_row),
                               &uuids[i]));
        ovsdb_row_destroy(read_row);
    }

    ovsdb_disk_store_close(ds);
    ovsdb_destroy(db);
    ovsdb_schema_destroy(schema);
    unlink(filename);
    free(filename);
}

/* Test 8: Read with all-zeros UUID on empty store. */
static void
test_null_uuid_lookup(void)
{
    struct ovsdb_schema *schema;
    struct ovsdb *db;
    struct ovsdb_table *table;
    struct ovsdb_disk_store *ds;
    struct ovsdb_row *read_row;
    struct uuid zero_uuid;
    char *filename;

    schema = create_test_schema();
    db = create_test_db(ovsdb_schema_clone(schema));
    table = ovsdb_get_table(db, "test");
    filename = make_test_filename("null-uuid");

    ds = ovsdb_disk_store_open(filename, schema);
    ovs_assert(ds != NULL);

    uuid_zero(&zero_uuid);
    read_row = ovsdb_disk_store_read_row(ds, table, &zero_uuid);
    ovs_assert(read_row == NULL);
    ovs_assert(!ovsdb_disk_store_contains(ds, &zero_uuid));

    ovsdb_disk_store_close(ds);
    ovsdb_destroy(db);
    ovsdb_schema_destroy(schema);
    unlink(filename);
    free(filename);
}

/* Test 9: Verify contains() for present and absent UUIDs. */
static void
test_contains(void)
{
    struct ovsdb_schema *schema;
    struct ovsdb *db;
    struct ovsdb_table *table;
    struct ovsdb_disk_store *ds;
    struct uuid uuids[5];
    struct uuid absent;
    char *filename;
    int i;

    schema = create_test_schema();
    db = create_test_db(ovsdb_schema_clone(schema));
    table = ovsdb_get_table(db, "test");
    filename = make_test_filename("contains");

    ds = ovsdb_disk_store_open(filename, schema);
    ovs_assert(ds != NULL);

    for (i = 0; i < 5; i++) {
        struct ovsdb_row *row;

        uuid_generate(&uuids[i]);
        row = create_test_row(table, &uuids[i]);
        check_ovsdb_error(ovsdb_disk_store_write_row(ds, row));
        ovsdb_row_destroy(row);
    }

    for (i = 0; i < 5; i++) {
        ovs_assert(ovsdb_disk_store_contains(ds, &uuids[i]));
    }

    uuid_generate(&absent);
    ovs_assert(!ovsdb_disk_store_contains(ds, &absent));

    ovsdb_disk_store_close(ds);
    ovsdb_destroy(db);
    ovsdb_schema_destroy(schema);
    unlink(filename);
    free(filename);
}

static void
test_disk_store_main(int argc OVS_UNUSED,
                     char *argv[] OVS_UNUSED)
{
    printf("test-disk-store: create_open_close\n");
    test_create_open_close();

    printf("test-disk-store: write_read_single\n");
    test_write_read_single();

    printf("test-disk-store: write_read_many\n");
    test_write_read_many();

    printf("test-disk-store: delete\n");
    test_delete();

    printf("test-disk-store: iterate\n");
    test_iterate();

    printf("test-disk-store: compact\n");
    test_compact();

    printf("test-disk-store: reopen_persistence\n");
    test_reopen_persistence();

    printf("test-disk-store: null_uuid_lookup\n");
    test_null_uuid_lookup();

    printf("test-disk-store: contains\n");
    test_contains();

    printf("test-disk-store: ok\n");
}

OVSTEST_REGISTER("test-disk-store", test_disk_store_main);
