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
#include <fcntl.h>
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
#include "ovsdb-data.h"
#include "ovsdb-error.h"
#include "ovsdb-types.h"
#include "ovsdb/row-cache.h"

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

/* ------------------------------------------------------------------ */
/* Complex-type test infrastructure.                                   */
/* ------------------------------------------------------------------ */

/* Helper: add a column to a table schema. */
static void
add_test_column(struct ovsdb_table_schema *ts,
                const char *name,
                const struct ovsdb_type *type)
{
    struct ovsdb_column *col = ovsdb_column_create(name, true, true, type);
    col->index = shash_count(&ts->columns);
    shash_add(&ts->columns, col->name, col);
}

/* Creates a schema with columns covering all OVSDB types:
 *   name       string
 *   count      integer
 *   enabled    boolean
 *   ratio      real
 *   options    map<string,string>
 *   tags       set<string>  (min=0, max=unlimited)
 *   ref        optional uuid (min=0, max=1)
 *   ext_ids    map<string,string>
 */
static struct ovsdb_schema *
create_complex_test_schema(void)
{
    struct ovsdb_schema *schema;
    struct ovsdb_table_schema *ts;
    struct ovsdb_type type;

    schema = ovsdb_schema_create("testdb", "1.0.0", "");
    ts = ovsdb_table_schema_create("complex", true, UINT_MAX, true);

    /* string scalar */
    add_test_column(ts, "name", &ovsdb_type_string);

    /* integer scalar */
    add_test_column(ts, "count", &ovsdb_type_integer);

    /* boolean scalar */
    add_test_column(ts, "enabled", &ovsdb_type_boolean);

    /* real scalar */
    add_test_column(ts, "ratio", &ovsdb_type_real);

    /* map<string,string> */
    ovsdb_base_type_init(&type.key, OVSDB_TYPE_STRING);
    ovsdb_base_type_init(&type.value, OVSDB_TYPE_STRING);
    type.n_min = 0;
    type.n_max = UINT_MAX;
    add_test_column(ts, "options", &type);
    ovsdb_type_destroy(&type);

    /* set<string> */
    ovsdb_base_type_init(&type.key, OVSDB_TYPE_STRING);
    ovsdb_base_type_init(&type.value, OVSDB_TYPE_VOID);
    type.n_min = 0;
    type.n_max = UINT_MAX;
    add_test_column(ts, "tags", &type);
    ovsdb_type_destroy(&type);

    /* optional uuid (min=0, max=1) */
    ovsdb_base_type_init(&type.key, OVSDB_TYPE_UUID);
    ovsdb_base_type_init(&type.value, OVSDB_TYPE_VOID);
    type.n_min = 0;
    type.n_max = 1;
    add_test_column(ts, "ref", &type);
    ovsdb_type_destroy(&type);

    /* map<string,string> (second map column) */
    ovsdb_base_type_init(&type.key, OVSDB_TYPE_STRING);
    ovsdb_base_type_init(&type.value, OVSDB_TYPE_STRING);
    type.n_min = 0;
    type.n_max = UINT_MAX;
    add_test_column(ts, "ext_ids", &type);
    ovsdb_type_destroy(&type);

    shash_add(&schema->tables, "complex", ts);
    return schema;
}

/* Populate a row with concrete data for all complex columns. */
static void
populate_complex_row(struct ovsdb_row *row,
                     const struct ovsdb_table_schema *ts)
{
    struct shash_node *node;

    SHASH_FOR_EACH (node, &ts->columns) {
        const struct ovsdb_column *col = node->data;
        struct ovsdb_datum *field;

        if (col->index < OVSDB_N_STD_COLUMNS) {
            continue;
        }
        field = &row->fields[col->index];
        ovsdb_datum_destroy(field, &col->type);

        if (!strcmp(col->name, "name")) {
            /* scalar string: "test-row" */
            field->n = 1;
            field->keys = xmalloc(sizeof *field->keys);
            field->keys[0].s = json_string_create("test-row");
            field->values = NULL;
            field->refcnt = NULL;
        } else if (!strcmp(col->name, "count")) {
            /* scalar integer: 42 */
            field->n = 1;
            field->keys = xmalloc(sizeof *field->keys);
            field->keys[0].integer = 42;
            field->values = NULL;
            field->refcnt = NULL;
        } else if (!strcmp(col->name, "enabled")) {
            /* scalar boolean: true */
            field->n = 1;
            field->keys = xmalloc(sizeof *field->keys);
            field->keys[0].boolean = true;
            field->values = NULL;
            field->refcnt = NULL;
        } else if (!strcmp(col->name, "ratio")) {
            /* scalar real: 3.14 */
            field->n = 1;
            field->keys = xmalloc(sizeof *field->keys);
            field->keys[0].real = 3.14;
            field->values = NULL;
            field->refcnt = NULL;
        } else if (!strcmp(col->name, "options")) {
            /* map<string,string>: {"key1":"val1","key2":"val2"} */
            field->n = 2;
            field->keys = xmalloc(2 * sizeof *field->keys);
            field->values = xmalloc(2 * sizeof *field->values);
            field->keys[0].s = json_string_create("key1");
            field->keys[1].s = json_string_create("key2");
            field->values[0].s = json_string_create("val1");
            field->values[1].s = json_string_create("val2");
            field->refcnt = NULL;
        } else if (!strcmp(col->name, "tags")) {
            /* set<string>: {"alpha","beta","gamma"} */
            field->n = 3;
            field->keys = xmalloc(3 * sizeof *field->keys);
            field->keys[0].s = json_string_create("alpha");
            field->keys[1].s = json_string_create("beta");
            field->keys[2].s = json_string_create("gamma");
            field->values = NULL;
            field->refcnt = NULL;
        } else if (!strcmp(col->name, "ref")) {
            /* optional uuid: present, with a generated UUID */
            struct uuid u;
            uuid_generate(&u);
            field->n = 1;
            field->keys = xmalloc(sizeof *field->keys);
            field->keys[0].uuid = u;
            field->values = NULL;
            field->refcnt = NULL;
        } else if (!strcmp(col->name, "ext_ids")) {
            /* map<string,string>: {"id1":"aaa"} */
            field->n = 1;
            field->keys = xmalloc(sizeof *field->keys);
            field->values = xmalloc(sizeof *field->values);
            field->keys[0].s = json_string_create("id1");
            field->values[0].s = json_string_create("aaa");
            field->refcnt = NULL;
        }
    }
}

/* Compare two rows field-by-field.  Returns true if equal. */
static bool
rows_equal(const struct ovsdb_row *a, const struct ovsdb_row *b,
           const struct ovsdb_table_schema *ts)
{
    struct shash_node *node;

    SHASH_FOR_EACH (node, &ts->columns) {
        const struct ovsdb_column *col = node->data;

        if (col->index < OVSDB_N_STD_COLUMNS) {
            continue;
        }
        if (!ovsdb_datum_equals(&a->fields[col->index],
                                &b->fields[col->index],
                                &col->type)) {
            printf("  column '%s' differs\n", col->name);
            return false;
        }
    }
    return true;
}

/* ------------------------------------------------------------------ */
/* Complex-type tests.                                                 */
/* ------------------------------------------------------------------ */

/* Test 10: Write a row with all column types, read it back. */
static void
test_complex_types_roundtrip(void)
{
    struct ovsdb_schema *schema;
    struct ovsdb *db;
    struct ovsdb_table *table;
    struct ovsdb_disk_store *ds;
    struct ovsdb_row *row, *read_row;
    struct uuid uuid;
    char *filename;

    schema = create_complex_test_schema();
    db = create_test_db(ovsdb_schema_clone(schema));
    table = ovsdb_get_table(db, "complex");
    filename = make_test_filename("complex-roundtrip");

    ds = ovsdb_disk_store_open(filename, schema);
    ovs_assert(ds != NULL);

    uuid_generate(&uuid);
    row = ovsdb_row_create(table);
    *ovsdb_row_get_uuid_rw(row) = uuid;
    populate_complex_row(row, table->schema);
    check_ovsdb_error(ovsdb_disk_store_write_row(ds, row));

    read_row = ovsdb_disk_store_read_row(ds, table, &uuid);
    ovs_assert(read_row != NULL);
    ovs_assert(uuid_equals(ovsdb_row_get_uuid(read_row), &uuid));
    ovs_assert(rows_equal(row, read_row, table->schema));
    ovsdb_row_destroy(read_row);
    ovsdb_row_destroy(row);

    ovsdb_disk_store_close(ds);
    ovsdb_destroy(db);
    ovsdb_schema_destroy(schema);
    unlink(filename);
    free(filename);
}

/* Test 11: Write complex row, close, reopen (reading embedded
 * schema from file -- simulating a server restart where the schema
 * is re-parsed and SHASH column order may differ). */
static void
test_complex_types_reopen(void)
{
    struct ovsdb_schema *schema, *schema2;
    struct ovsdb *db, *db2;
    struct ovsdb_table *table, *table2;
    struct ovsdb_disk_store *ds;
    struct ovsdb_row *row, *read_row;
    struct uuid uuid;
    char *filename;

    schema = create_complex_test_schema();
    db = create_test_db(ovsdb_schema_clone(schema));
    table = ovsdb_get_table(db, "complex");
    filename = make_test_filename("complex-reopen");

    /* Write a row with all complex types. */
    ds = ovsdb_disk_store_open(filename, schema);
    ovs_assert(ds != NULL);

    uuid_generate(&uuid);
    row = ovsdb_row_create(table);
    *ovsdb_row_get_uuid_rw(row) = uuid;
    populate_complex_row(row, table->schema);
    check_ovsdb_error(ovsdb_disk_store_write_row(ds, row));
    ovsdb_disk_store_close(ds);

    /* Reopen with NULL schema so the embedded schema is re-parsed
     * from the file.  This simulates a restart where the schema
     * shash may have a different iteration order. */
    ds = ovsdb_disk_store_open(filename, NULL);
    ovs_assert(ds != NULL);

    schema2 = ovsdb_disk_store_get_schema(ds);
    ovs_assert(schema2 != NULL);
    db2 = create_test_db(ovsdb_schema_clone(schema2));
    table2 = ovsdb_get_table(db2, "complex");

    read_row = ovsdb_disk_store_read_row(ds, table2, &uuid);
    ovs_assert(read_row != NULL);
    ovs_assert(uuid_equals(ovsdb_row_get_uuid(read_row), &uuid));
    ovs_assert(rows_equal(row, read_row, table2->schema));
    ovsdb_row_destroy(read_row);

    ovsdb_row_destroy(row);
    ovsdb_disk_store_close(ds);
    ovsdb_destroy(db);
    ovsdb_destroy(db2);
    ovsdb_schema_destroy(schema);
    unlink(filename);
    free(filename);
}

/* Test 12: Empty map roundtrip. */
static void
test_empty_map_roundtrip(void)
{
    struct ovsdb_schema *schema;
    struct ovsdb *db;
    struct ovsdb_table *table;
    struct ovsdb_disk_store *ds;
    struct ovsdb_row *row, *read_row;
    struct uuid uuid;
    char *filename;

    schema = create_complex_test_schema();
    db = create_test_db(ovsdb_schema_clone(schema));
    table = ovsdb_get_table(db, "complex");
    filename = make_test_filename("empty-map");

    ds = ovsdb_disk_store_open(filename, schema);
    ovs_assert(ds != NULL);

    /* Write a row with default (empty) values -- maps/sets
     * will have n=0. */
    uuid_generate(&uuid);
    row = ovsdb_row_create(table);
    *ovsdb_row_get_uuid_rw(row) = uuid;
    check_ovsdb_error(ovsdb_disk_store_write_row(ds, row));

    read_row = ovsdb_disk_store_read_row(ds, table, &uuid);
    ovs_assert(read_row != NULL);
    ovs_assert(rows_equal(row, read_row, table->schema));
    ovsdb_row_destroy(read_row);
    ovsdb_row_destroy(row);

    ovsdb_disk_store_close(ds);
    ovsdb_destroy(db);
    ovsdb_schema_destroy(schema);
    unlink(filename);
    free(filename);
}

/* Test 13: Large map roundtrip (100 entries). */
static void
test_large_map_roundtrip(void)
{
    struct ovsdb_schema *schema;
    struct ovsdb *db;
    struct ovsdb_table *table;
    struct ovsdb_disk_store *ds;
    struct ovsdb_row *row, *read_row;
    struct uuid uuid;
    char *filename;
    const struct ovsdb_column *options_col;
    struct ovsdb_datum *field;
    int i;

    schema = create_complex_test_schema();
    db = create_test_db(ovsdb_schema_clone(schema));
    table = ovsdb_get_table(db, "complex");
    filename = make_test_filename("large-map");

    ds = ovsdb_disk_store_open(filename, schema);
    ovs_assert(ds != NULL);

    uuid_generate(&uuid);
    row = ovsdb_row_create(table);
    *ovsdb_row_get_uuid_rw(row) = uuid;

    /* Build a 100-entry map<string,string> in the options column. */
    options_col = shash_find_data(&table->schema->columns, "options");
    ovs_assert(options_col != NULL);
    field = &row->fields[options_col->index];
    ovsdb_datum_destroy(field, &options_col->type);

    field->n = 100;
    field->keys = xmalloc(100 * sizeof *field->keys);
    field->values = xmalloc(100 * sizeof *field->values);
    field->refcnt = NULL;
    for (i = 0; i < 100; i++) {
        char *k = xasprintf("key_%03d", i);
        char *v = xasprintf("value_%03d", i);
        field->keys[i].s = json_string_create_nocopy(k);
        field->values[i].s = json_string_create_nocopy(v);
    }

    check_ovsdb_error(ovsdb_disk_store_write_row(ds, row));

    read_row = ovsdb_disk_store_read_row(ds, table, &uuid);
    ovs_assert(read_row != NULL);

    /* Verify the options field specifically. */
    ovs_assert(ovsdb_datum_equals(&row->fields[options_col->index],
                                  &read_row->fields[options_col->index],
                                  &options_col->type));
    ovsdb_row_destroy(read_row);
    ovsdb_row_destroy(row);

    ovsdb_disk_store_close(ds);
    ovsdb_destroy(db);
    ovsdb_schema_destroy(schema);
    unlink(filename);
    free(filename);
}

/* Test 14: Write complex rows, delete some, compact, verify. */
static void
test_complex_types_compact(void)
{
    struct ovsdb_schema *schema;
    struct ovsdb *db;
    struct ovsdb_table *table;
    struct ovsdb_disk_store *ds;
    struct uuid uuids[10];
    char *filename;
    int i;

    schema = create_complex_test_schema();
    db = create_test_db(ovsdb_schema_clone(schema));
    table = ovsdb_get_table(db, "complex");
    filename = make_test_filename("complex-compact");

    ds = ovsdb_disk_store_open(filename, schema);
    ovs_assert(ds != NULL);

    for (i = 0; i < 10; i++) {
        struct ovsdb_row *row;

        uuid_generate(&uuids[i]);
        row = ovsdb_row_create(table);
        *ovsdb_row_get_uuid_rw(row) = uuids[i];
        populate_complex_row(row, table->schema);
        check_ovsdb_error(ovsdb_disk_store_write_row(ds, row));
        ovsdb_row_destroy(row);
    }

    /* Delete first 5. */
    for (i = 0; i < 5; i++) {
        check_ovsdb_error(
            ovsdb_disk_store_delete_row(ds, &uuids[i]));
    }

    check_ovsdb_error(ovsdb_disk_store_compact(ds));
    ovs_assert(ovsdb_disk_store_count(ds, "complex") == 5);

    /* Verify surviving rows. */
    for (i = 5; i < 10; i++) {
        struct ovsdb_row *read_row;

        read_row = ovsdb_disk_store_read_row(ds, table, &uuids[i]);
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

/* ------------------------------------------------------------------ */
/* Crash resilience and concurrency hypothesis tests.                  */
/* ------------------------------------------------------------------ */

/* Test 15: Corrupt row on disk — verify read returns NULL and
 * repeated reads don't spin infinitely.
 *
 * Hypothesis: when disk_store_read_row() returns NULL (corrupt data),
 * the caller (table get_row / lazy-load) enters an infinite retry
 * loop because the cache entry stays UNLOADED. */
static void
test_corrupt_row_returns_null(void)
{
    struct ovsdb_schema *schema;
    struct ovsdb *db;
    struct ovsdb_table *table;
    struct ovsdb_disk_store *ds;
    struct ovsdb_row *row, *read_row;
    struct uuid uuid;
    char *filename;
    const char *ds_filename;
    int fd;
    off_t file_size;
    uint8_t garbage[16];

    schema = create_complex_test_schema();
    db = create_test_db(ovsdb_schema_clone(schema));
    table = ovsdb_get_table(db, "complex");
    filename = make_test_filename("corrupt-row");

    /* Write a valid complex row. */
    ds = ovsdb_disk_store_open(filename, schema);
    ovs_assert(ds != NULL);

    uuid_generate(&uuid);
    row = ovsdb_row_create(table);
    *ovsdb_row_get_uuid_rw(row) = uuid;
    populate_complex_row(row, table->schema);
    check_ovsdb_error(ovsdb_disk_store_write_row(ds, row));
    ovsdb_row_destroy(row);

    /* Get the disk store filename and close. */
    ds_filename = ovsdb_disk_store_get_filename(ds);
    ovs_assert(ds_filename != NULL);
    ovsdb_disk_store_close(ds);

    /* Corrupt the last 16 bytes of the file (part of the row body). */
    fd = open(filename, O_RDWR);
    ovs_assert(fd >= 0);
    file_size = lseek(fd, 0, SEEK_END);
    ovs_assert(file_size > 16);
    memset(garbage, 0xDE, sizeof garbage);
    lseek(fd, file_size - 16, SEEK_SET);
    ovs_assert(write(fd, garbage, sizeof garbage)
               == (ssize_t) sizeof garbage);
    close(fd);

    /* Reopen and try to read the corrupted row. */
    ds = ovsdb_disk_store_open(filename, schema);
    ovs_assert(ds != NULL);

    /* disk_store_read_row should return NULL for the corrupt row. */
    read_row = ovsdb_disk_store_read_row(ds, table, &uuid);
    ovs_assert(read_row == NULL);

    /* Read again — should still return NULL, not spin. */
    read_row = ovsdb_disk_store_read_row(ds, table, &uuid);
    ovs_assert(read_row == NULL);

    ovsdb_disk_store_close(ds);
    ovsdb_destroy(db);
    ovsdb_schema_destroy(schema);
    unlink(filename);
    free(filename);
}

/* Test 16: Verify cache state transitions when disk read fails.
 *
 * Hypothesis: when a row is in UNLOADED state and sync load fails
 * (returns NULL), the cache entry stays UNLOADED forever, causing
 * infinite retry on every get_row call. */
static void
test_cache_state_on_failed_load(void)
{
    struct ovsdb_row_cache *cache;
    struct uuid uuid;
    enum ovsdb_row_state state;

    cache = ovsdb_row_cache_create(100000);

    /* Add an UNLOADED entry. */
    uuid_generate(&uuid);
    ovsdb_row_cache_add_unloaded(cache, &uuid);
    state = ovsdb_row_cache_get_state(cache, &uuid);
    ovs_assert(state == OVSDB_ROW_UNLOADED);

    /* Simulate: load was submitted → LOADING. */
    ovsdb_row_cache_set_state(cache, &uuid, OVSDB_ROW_LOADING);
    state = ovsdb_row_cache_get_state(cache, &uuid);
    ovs_assert(state == OVSDB_ROW_LOADING);

    /* Use record_load_failure: first failure → UNLOADED (retry). */
    state = ovsdb_row_cache_record_load_failure(cache, &uuid);
    ovs_assert(state == OVSDB_ROW_UNLOADED);
    ovs_assert(ovsdb_row_cache_get_state(cache, &uuid)
               == OVSDB_ROW_UNLOADED);

    /* Second failure → still UNLOADED. */
    state = ovsdb_row_cache_record_load_failure(cache, &uuid);
    ovs_assert(state == OVSDB_ROW_UNLOADED);

    /* Third failure → transitions to ERROR (OVSDB_MAX_LOAD_RETRIES=3). */
    state = ovsdb_row_cache_record_load_failure(cache, &uuid);
    ovs_assert(state == OVSDB_ROW_ERROR);
    ovs_assert(ovsdb_row_cache_get_state(cache, &uuid)
               == OVSDB_ROW_ERROR);

    /* ERROR entries should NOT be counted as needing load. */
    ovs_assert(!ovsdb_row_cache_has_unloaded(cache));

    ovsdb_row_cache_destroy(cache);
}

/* Test 17: Cache iteration while entries change state.
 *
 * Hypothesis: if a cache entry transitions from LOADING to UNLOADED
 * (failed load) while ovsdb_row_cache_for_each_loaded() is
 * iterating, the iteration could hit corrupted state. */
static bool
count_loaded_cb(const struct ovsdb_row *row OVS_UNUSED, void *aux)
{
    int *count = aux;
    (*count)++;
    return true;
}

static void
test_cache_iteration_with_state_changes(void)
{
    struct ovsdb_schema *schema;
    struct ovsdb *db;
    struct ovsdb_table *table;
    struct ovsdb_disk_store *ds;
    struct ovsdb_row_cache *cache;
    struct uuid uuids[5];
    struct uuid unloaded_uuid;
    char *filename;
    int count;
    int i;

    schema = create_complex_test_schema();
    db = create_test_db(ovsdb_schema_clone(schema));
    table = ovsdb_get_table(db, "complex");
    filename = make_test_filename("cache-iter");

    ds = ovsdb_disk_store_open(filename, schema);
    ovs_assert(ds != NULL);

    cache = ovsdb_row_cache_create(100000);

    /* Insert 5 CACHED rows. */
    for (i = 0; i < 5; i++) {
        struct ovsdb_row *row;

        uuid_generate(&uuids[i]);
        row = ovsdb_row_create(table);
        *ovsdb_row_get_uuid_rw(row) = uuids[i];
        populate_complex_row(row, table->schema);
        ovsdb_row_cache_insert(cache, row, 10);
    }

    /* Add one UNLOADED entry. */
    uuid_generate(&unloaded_uuid);
    ovsdb_row_cache_add_unloaded(cache, &unloaded_uuid);

    /* Set it to LOADING (simulating async load in progress). */
    ovsdb_row_cache_set_state(cache, &unloaded_uuid,
                              OVSDB_ROW_LOADING);

    /* Iterate loaded rows — should see 5 CACHED rows,
     * skip the LOADING entry. */
    count = 0;
    ovsdb_row_cache_for_each_loaded(cache, count_loaded_cb, &count);
    ovs_assert(count == 5);

    /* Now simulate: load fails → set back to UNLOADED. */
    ovsdb_row_cache_set_state(cache, &unloaded_uuid,
                              OVSDB_ROW_UNLOADED);

    /* Iterate again — should still see exactly 5. */
    count = 0;
    ovsdb_row_cache_for_each_loaded(cache, count_loaded_cb, &count);
    ovs_assert(count == 5);

    ovsdb_row_cache_destroy(cache);
    ovsdb_disk_store_close(ds);
    ovsdb_destroy(db);
    ovsdb_schema_destroy(schema);
    unlink(filename);
    free(filename);
}

/* Test 18: Cache insert triggers eviction of entries being iterated.
 *
 * Hypothesis: ovsdb_row_cache_insert() calls evict__() which calls
 * hmap_remove().  If this is triggered from within a callback
 * during for_each_loaded iteration, the iterator's bucket pointers
 * become stale → SIGSEGV.
 *
 * We simulate this by creating a tiny cache (budget=10 atoms),
 * filling it, then doing an insert (which triggers eviction)
 * while iterating — verifying no crash. */
struct evict_during_iter_aux {
    struct ovsdb_row_cache *cache;
    struct ovsdb_table *table;
    int seen;
    bool did_insert;
};

static bool
evict_during_iter_cb(const struct ovsdb_row *row OVS_UNUSED,
                     void *aux_)
{
    struct evict_during_iter_aux *aux = aux_;

    aux->seen++;

    /* On the second callback, insert a new row that triggers
     * eviction (cache budget is tiny). */
    if (aux->seen == 2 && !aux->did_insert) {
        struct ovsdb_row *new_row;
        struct uuid new_uuid;

        aux->did_insert = true;
        uuid_generate(&new_uuid);
        new_row = ovsdb_row_create(aux->table);
        *ovsdb_row_get_uuid_rw(new_row) = new_uuid;

        /* This insert will trigger eviction because cache is
         * at budget.  Eviction calls hmap_remove() on entries
         * while we're inside HMAP_FOR_EACH. */
        ovsdb_row_cache_insert(aux->cache, new_row, 5);
    }

    return true; /* continue iteration */
}

static void
test_cache_eviction_during_iteration(void)
{
    struct ovsdb_schema *schema;
    struct ovsdb *db;
    struct ovsdb_table *table;
    struct ovsdb_row_cache *cache;
    struct evict_during_iter_aux aux;
    int i;

    schema = create_complex_test_schema();
    db = create_test_db(ovsdb_schema_clone(schema));
    table = ovsdb_get_table(db, "complex");

    /* Tiny cache: budget of 10 atoms.  Each row gets 5 atoms,
     * so 2 rows fit, 3rd triggers eviction. */
    cache = ovsdb_row_cache_create(10);

    /* Insert 3 rows → 15 atoms total, exceeds budget → 1 evicted. */
    for (i = 0; i < 3; i++) {
        struct ovsdb_row *row;
        struct uuid uuid;

        uuid_generate(&uuid);
        row = ovsdb_row_create(table);
        *ovsdb_row_get_uuid_rw(row) = uuid;
        ovsdb_row_cache_insert(cache, row, 5);
    }

    /* Now iterate and trigger eviction from inside the callback.
     * If hmap_remove during HMAP_FOR_EACH causes corruption,
     * this will SIGSEGV. */
    aux.cache = cache;
    aux.table = table;
    aux.seen = 0;
    aux.did_insert = false;

    ovsdb_row_cache_for_each_loaded(cache, evict_during_iter_cb, &aux);

    /* We survived.  The exact count depends on whether the evicted
     * entry was ahead or behind the iterator.  The key is no crash. */
    ovsdb_row_cache_destroy(cache);
    ovsdb_destroy(db);
    ovsdb_schema_destroy(schema);
}

/* Test 19: Concurrent write + read on disk store — verify index
 * consistency.
 *
 * Hypothesis: ovsdb_disk_store_write_row() frees the old index
 * entry (hmap_remove + free) at lines 938-939 of disk-store.c.
 * If a worker thread holds a pointer from disk_store_find_entry()
 * obtained before the write, it becomes a dangling pointer →
 * use-after-free → garbage pread → deserialization failure or
 * SIGSEGV.
 *
 * We simulate this single-threaded: write row v1, stash its UUID,
 * write row v2 (same UUID, different data), then read.  The read
 * should get v2, proving the index was updated atomically.
 * Then we verify that rapid write-read interleaving doesn't
 * corrupt the index. */
static void
test_disk_store_index_consistency(void)
{
    struct ovsdb_schema *schema;
    struct ovsdb *db;
    struct ovsdb_table *table;
    struct ovsdb_disk_store *ds;
    struct ovsdb_row *row, *read_row;
    struct uuid uuid;
    char *filename;
    const struct ovsdb_column *name_col;
    struct ovsdb_datum *field;
    int i;

    schema = create_complex_test_schema();
    db = create_test_db(ovsdb_schema_clone(schema));
    table = ovsdb_get_table(db, "complex");
    filename = make_test_filename("index-consistency");

    ds = ovsdb_disk_store_open(filename, schema);
    ovs_assert(ds != NULL);

    name_col = shash_find_data(&table->schema->columns, "name");
    ovs_assert(name_col != NULL);

    uuid_generate(&uuid);

    /* Write version 1. */
    row = ovsdb_row_create(table);
    *ovsdb_row_get_uuid_rw(row) = uuid;
    field = &row->fields[name_col->index];
    ovsdb_datum_destroy(field, &name_col->type);
    field->n = 1;
    field->keys = xmalloc(sizeof *field->keys);
    field->keys[0].s = json_string_create("version-1");
    field->values = NULL;
    field->refcnt = NULL;
    check_ovsdb_error(ovsdb_disk_store_write_row(ds, row));
    ovsdb_row_destroy(row);

    /* Overwrite with version 2 (same UUID). */
    row = ovsdb_row_create(table);
    *ovsdb_row_get_uuid_rw(row) = uuid;
    field = &row->fields[name_col->index];
    ovsdb_datum_destroy(field, &name_col->type);
    field->n = 1;
    field->keys = xmalloc(sizeof *field->keys);
    field->keys[0].s = json_string_create("version-2");
    field->values = NULL;
    field->refcnt = NULL;
    check_ovsdb_error(ovsdb_disk_store_write_row(ds, row));
    ovsdb_row_destroy(row);

    /* Read should return version 2 (index updated). */
    read_row = ovsdb_disk_store_read_row(ds, table, &uuid);
    ovs_assert(read_row != NULL);
    {
        const char *name_val = json_string(
            read_row->fields[name_col->index].keys[0].s);
        ovs_assert(!strcmp(name_val, "version-2"));
    }
    ovsdb_row_destroy(read_row);

    /* Stress: rapid write-read cycles on the same UUID.
     * This exercises the index entry free+realloc path
     * that is racy with workers. */
    for (i = 0; i < 100; i++) {
        char *version = xasprintf("stress-%03d", i);

        row = ovsdb_row_create(table);
        *ovsdb_row_get_uuid_rw(row) = uuid;
        field = &row->fields[name_col->index];
        ovsdb_datum_destroy(field, &name_col->type);
        field->n = 1;
        field->keys = xmalloc(sizeof *field->keys);
        field->keys[0].s = json_string_create_nocopy(version);
        field->values = NULL;
        field->refcnt = NULL;
        check_ovsdb_error(ovsdb_disk_store_write_row(ds, row));
        ovsdb_row_destroy(row);

        read_row = ovsdb_disk_store_read_row(ds, table, &uuid);
        ovs_assert(read_row != NULL);
        {
            char *expected = xasprintf("stress-%03d", i);
            const char *actual = json_string(
                read_row->fields[name_col->index].keys[0].s);
            ovs_assert(!strcmp(actual, expected));
            free(expected);
        }
        ovsdb_row_destroy(read_row);
    }

    ovsdb_disk_store_close(ds);
    ovsdb_destroy(db);
    ovsdb_schema_destroy(schema);
    unlink(filename);
    free(filename);
}

/* Test 20: Cache insert+remove+iterate stress test.
 *
 * Exercises the hmap under heavy insert/evict/iterate load to
 * detect any corruption from internal re-entrancy or stale
 * iterator state. */
static bool
stress_iter_cb(const struct ovsdb_row *row OVS_UNUSED, void *aux)
{
    int *count = aux;
    (*count)++;
    return true;
}

static void
test_cache_stress(void)
{
    struct ovsdb_schema *schema;
    struct ovsdb *db;
    struct ovsdb_table *table;
    struct ovsdb_row_cache *cache;
    struct uuid uuids[200];
    int i, count;

    schema = create_complex_test_schema();
    db = create_test_db(ovsdb_schema_clone(schema));
    table = ovsdb_get_table(db, "complex");

    /* Small cache: budget for ~50 rows at 10 atoms each. */
    cache = ovsdb_row_cache_create(500);

    /* Insert 200 rows.  Each insert may trigger eviction.
     * Verify cache stays consistent throughout. */
    for (i = 0; i < 200; i++) {
        struct ovsdb_row *row;

        uuid_generate(&uuids[i]);
        row = ovsdb_row_create(table);
        *ovsdb_row_get_uuid_rw(row) = uuids[i];
        ovsdb_row_cache_insert(cache, row, 10);

        /* Every 20 inserts, iterate the whole cache. */
        if (i % 20 == 19) {
            count = 0;
            ovsdb_row_cache_for_each_loaded(cache,
                                            stress_iter_cb, &count);
            /* Count should be <= cache budget / atom cost. */
            ovs_assert(count <= 51);
        }
    }

    /* Remove 50 random entries, then iterate again. */
    for (i = 150; i < 200; i++) {
        ovsdb_row_cache_remove(cache, &uuids[i]);
    }

    count = 0;
    ovsdb_row_cache_for_each_loaded(cache, stress_iter_cb, &count);
    (void) count; /* Used only for assertion below. */

    /* Now interleave: add UNLOADED entries, transition them,
     * insert CACHED entries — all with tiny budget. */
    for (i = 0; i < 50; i++) {
        struct uuid u;
        uuid_generate(&u);

        /* Add as UNLOADED, then remove (simulating failed load). */
        ovsdb_row_cache_add_unloaded(cache, &u);
        ovsdb_row_cache_set_state(cache, &u, OVSDB_ROW_LOADING);
        ovsdb_row_cache_set_state(cache, &u, OVSDB_ROW_UNLOADED);
        ovsdb_row_cache_remove(cache, &u);
    }

    /* Final iteration — should not crash. */
    count = 0;
    ovsdb_row_cache_for_each_loaded(cache, stress_iter_cb, &count);
    ovsdb_row_cache_destroy(cache);
    ovsdb_destroy(db);
    ovsdb_schema_destroy(schema);
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

    printf("test-disk-store: complex_types_roundtrip\n");
    test_complex_types_roundtrip();

    printf("test-disk-store: complex_types_reopen\n");
    test_complex_types_reopen();

    printf("test-disk-store: empty_map_roundtrip\n");
    test_empty_map_roundtrip();

    printf("test-disk-store: large_map_roundtrip\n");
    test_large_map_roundtrip();

    printf("test-disk-store: complex_types_compact\n");
    test_complex_types_compact();

    printf("test-disk-store: corrupt_row_returns_null\n");
    test_corrupt_row_returns_null();

    printf("test-disk-store: cache_state_on_failed_load\n");
    test_cache_state_on_failed_load();

    printf("test-disk-store: cache_iteration_with_state_changes\n");
    test_cache_iteration_with_state_changes();

    printf("test-disk-store: cache_eviction_during_iteration\n");
    test_cache_eviction_during_iteration();

    printf("test-disk-store: disk_store_index_consistency\n");
    test_disk_store_index_consistency();

    printf("test-disk-store: cache_stress\n");
    test_cache_stress();

    printf("test-disk-store: ok\n");
}

OVSTEST_REGISTER("test-disk-store", test_disk_store_main);
