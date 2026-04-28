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

#include <float.h>
#include <math.h>
#include <stdio.h>
#include <string.h>

#include "binary-codec.h"
#include "binary-protocol.h"
#include "openvswitch/json.h"
#include "openvswitch/uuid.h"
#include "ovsdb/column.h"
#include "ovsdb/table.h"
#include "ovsdb-error.h"
#include "ovsdb-types.h"
#include "ovsdb-data.h"
#include "util.h"

/* ------------------------------------------------------------------ */
/* Atom round-trip tests.                                             */
/* ------------------------------------------------------------------ */

static void
test_atom_integer(bool nbo)
{
    int64_t values[] = { 0, 1, -1, INT64_MAX, INT64_MIN, 42, -99999 };
    size_t i;

    for (i = 0; i < ARRAY_SIZE(values); i++) {
        struct ovsdb_binary_buf buf;
        struct ovsdb_binary_reader reader;
        union ovsdb_atom src, dst;

        src.integer = values[i];
        ovsdb_binary_buf_init(&buf);
        ovsdb_binary_serialize_atom(&buf, &src, OVSDB_TYPE_INTEGER, nbo);

        ovsdb_binary_reader_init(&reader, buf.data, buf.size);
        memset(&dst, 0, sizeof dst);
        ovs_assert(ovsdb_binary_deserialize_atom(&reader, &dst,
                                                  OVSDB_TYPE_INTEGER, nbo));
        ovs_assert(dst.integer == values[i]);

        ovsdb_binary_buf_destroy(&buf);
    }
}

static void
test_atom_real(bool nbo)
{
    double values[] = { 0.0, 1.5, -1.5, DBL_MAX, DBL_MIN, 3.14159 };
    size_t i;

    for (i = 0; i < ARRAY_SIZE(values); i++) {
        struct ovsdb_binary_buf buf;
        struct ovsdb_binary_reader reader;
        union ovsdb_atom src, dst;

        src.real = values[i];
        ovsdb_binary_buf_init(&buf);
        ovsdb_binary_serialize_atom(&buf, &src, OVSDB_TYPE_REAL, nbo);

        ovsdb_binary_reader_init(&reader, buf.data, buf.size);
        memset(&dst, 0, sizeof dst);
        ovs_assert(ovsdb_binary_deserialize_atom(&reader, &dst,
                                                  OVSDB_TYPE_REAL, nbo));
        ovs_assert(dst.real == values[i]);

        ovsdb_binary_buf_destroy(&buf);
    }
}

static void
test_atom_boolean(bool nbo)
{
    bool values[] = { true, false };
    size_t i;

    for (i = 0; i < ARRAY_SIZE(values); i++) {
        struct ovsdb_binary_buf buf;
        struct ovsdb_binary_reader reader;
        union ovsdb_atom src, dst;

        src.boolean = values[i];
        ovsdb_binary_buf_init(&buf);
        ovsdb_binary_serialize_atom(&buf, &src, OVSDB_TYPE_BOOLEAN, nbo);

        ovsdb_binary_reader_init(&reader, buf.data, buf.size);
        memset(&dst, 0, sizeof dst);
        ovs_assert(ovsdb_binary_deserialize_atom(&reader, &dst,
                                                  OVSDB_TYPE_BOOLEAN, nbo));
        ovs_assert(dst.boolean == values[i]);

        ovsdb_binary_buf_destroy(&buf);
    }
}

static void
test_atom_string(bool nbo)
{
    const char *values[] = { "", "hello", "a longer test string",
                             "utf8: \xc3\xa9\xc3\xa0\xc3\xbc" };
    size_t i;

    for (i = 0; i < ARRAY_SIZE(values); i++) {
        struct ovsdb_binary_buf buf;
        struct ovsdb_binary_reader reader;
        union ovsdb_atom src, dst;

        src.s = json_string_create(values[i]);
        ovsdb_binary_buf_init(&buf);
        ovsdb_binary_serialize_atom(&buf, &src, OVSDB_TYPE_STRING, nbo);

        ovsdb_binary_reader_init(&reader, buf.data, buf.size);
        memset(&dst, 0, sizeof dst);
        ovs_assert(ovsdb_binary_deserialize_atom(&reader, &dst,
                                                  OVSDB_TYPE_STRING, nbo));
        ovs_assert(!strcmp(json_string(dst.s), values[i]));

        json_destroy(src.s);
        json_destroy(dst.s);
        ovsdb_binary_buf_destroy(&buf);
    }
}

static void
test_atom_uuid(bool nbo)
{
    struct ovsdb_binary_buf buf;
    struct ovsdb_binary_reader reader;
    union ovsdb_atom src, dst;
    struct uuid test_uuid;

    uuid_from_string(&test_uuid,
                     "550e8400-e29b-41d4-a716-446655440000");
    src.uuid = test_uuid;
    ovsdb_binary_buf_init(&buf);
    ovsdb_binary_serialize_atom(&buf, &src, OVSDB_TYPE_UUID, nbo);

    ovsdb_binary_reader_init(&reader, buf.data, buf.size);
    memset(&dst, 0, sizeof dst);
    ovs_assert(ovsdb_binary_deserialize_atom(&reader, &dst,
                                              OVSDB_TYPE_UUID, nbo));
    ovs_assert(uuid_equals(&dst.uuid, &test_uuid));

    ovsdb_binary_buf_destroy(&buf);
}

static void
test_atoms(void)
{
    bool nbo_values[] = { false, true };
    size_t i;

    for (i = 0; i < ARRAY_SIZE(nbo_values); i++) {
        bool nbo = nbo_values[i];

        test_atom_integer(nbo);
        test_atom_real(nbo);
        test_atom_boolean(nbo);
        test_atom_string(nbo);
        test_atom_uuid(nbo);
    }
    printf("atoms: PASSED\n");
}

/* ------------------------------------------------------------------ */
/* Datum round-trip tests.                                            */
/* ------------------------------------------------------------------ */

static void
test_datum_scalar_integer(bool nbo)
{
    struct ovsdb_binary_buf buf;
    struct ovsdb_binary_reader reader;
    struct ovsdb_datum src, dst;
    struct ovsdb_type type;

    ovsdb_base_type_init(&type.key, OVSDB_TYPE_INTEGER);
    ovsdb_base_type_init(&type.value, OVSDB_TYPE_VOID);
    type.n_min = 1;
    type.n_max = 1;

    ovsdb_datum_init_empty(&src);
    src.n = 1;
    src.keys = xmalloc(sizeof *src.keys);
    src.keys[0].integer = 12345;

    ovsdb_binary_buf_init(&buf);
    ovsdb_binary_serialize_datum(&buf, &src, &type, nbo);

    ovsdb_binary_reader_init(&reader, buf.data, buf.size);
    ovsdb_datum_init_empty(&dst);
    ovs_assert(ovsdb_binary_deserialize_datum(&reader, &dst, &type, nbo));
    ovs_assert(dst.n == 1);
    ovs_assert(dst.keys[0].integer == 12345);

    ovsdb_datum_destroy(&src, &type);
    ovsdb_datum_destroy(&dst, &type);
    ovsdb_binary_buf_destroy(&buf);
}

static void
test_datum_set_of_strings(bool nbo)
{
    struct ovsdb_binary_buf buf;
    struct ovsdb_binary_reader reader;
    struct ovsdb_datum src, dst;
    struct ovsdb_type type;

    ovsdb_base_type_init(&type.key, OVSDB_TYPE_STRING);
    ovsdb_base_type_init(&type.value, OVSDB_TYPE_VOID);
    type.n_min = 0;
    type.n_max = UINT_MAX;

    ovsdb_datum_init_empty(&src);
    src.n = 3;
    src.keys = xmalloc(3 * sizeof *src.keys);
    src.keys[0].s = json_string_create("alpha");
    src.keys[1].s = json_string_create("beta");
    src.keys[2].s = json_string_create("gamma");

    ovsdb_binary_buf_init(&buf);
    ovsdb_binary_serialize_datum(&buf, &src, &type, nbo);

    ovsdb_binary_reader_init(&reader, buf.data, buf.size);
    ovsdb_datum_init_empty(&dst);
    ovs_assert(ovsdb_binary_deserialize_datum(&reader, &dst, &type, nbo));
    ovs_assert(dst.n == 3);

    ovsdb_datum_destroy(&src, &type);
    ovsdb_datum_destroy(&dst, &type);
    ovsdb_binary_buf_destroy(&buf);
}

static void
test_datum_map(bool nbo)
{
    struct ovsdb_binary_buf buf;
    struct ovsdb_binary_reader reader;
    struct ovsdb_datum src, dst;
    struct ovsdb_type type;

    ovsdb_base_type_init(&type.key, OVSDB_TYPE_STRING);
    ovsdb_base_type_init(&type.value, OVSDB_TYPE_INTEGER);
    type.n_min = 0;
    type.n_max = UINT_MAX;

    ovsdb_datum_init_empty(&src);
    src.n = 2;
    src.keys = xmalloc(2 * sizeof *src.keys);
    src.values = xmalloc(2 * sizeof *src.values);
    src.keys[0].s = json_string_create("port");
    src.values[0].integer = 8080;
    src.keys[1].s = json_string_create("timeout");
    src.values[1].integer = 30;

    ovsdb_binary_buf_init(&buf);
    ovsdb_binary_serialize_datum(&buf, &src, &type, nbo);

    ovsdb_binary_reader_init(&reader, buf.data, buf.size);
    ovsdb_datum_init_empty(&dst);
    ovs_assert(ovsdb_binary_deserialize_datum(&reader, &dst, &type, nbo));
    ovs_assert(dst.n == 2);

    ovsdb_datum_destroy(&src, &type);
    ovsdb_datum_destroy(&dst, &type);
    ovsdb_binary_buf_destroy(&buf);
}

static void
test_datum_empty(bool nbo)
{
    struct ovsdb_binary_buf buf;
    struct ovsdb_binary_reader reader;
    struct ovsdb_datum src, dst;
    struct ovsdb_type type;

    ovsdb_base_type_init(&type.key, OVSDB_TYPE_STRING);
    ovsdb_base_type_init(&type.value, OVSDB_TYPE_VOID);
    type.n_min = 0;
    type.n_max = UINT_MAX;

    ovsdb_datum_init_empty(&src);

    ovsdb_binary_buf_init(&buf);
    ovsdb_binary_serialize_datum(&buf, &src, &type, nbo);

    ovsdb_binary_reader_init(&reader, buf.data, buf.size);
    ovsdb_datum_init_empty(&dst);
    ovs_assert(ovsdb_binary_deserialize_datum(&reader, &dst, &type, nbo));
    ovs_assert(dst.n == 0);

    ovsdb_datum_destroy(&dst, &type);
    ovsdb_binary_buf_destroy(&buf);
}

static void
test_datums(void)
{
    bool nbo_values[] = { false, true };
    size_t i;

    for (i = 0; i < ARRAY_SIZE(nbo_values); i++) {
        bool nbo = nbo_values[i];

        test_datum_scalar_integer(nbo);
        test_datum_set_of_strings(nbo);
        test_datum_map(nbo);
        test_datum_empty(nbo);
    }
    printf("datums: PASSED\n");
}

/* ------------------------------------------------------------------ */
/* Frame round-trip tests.                                            */
/* ------------------------------------------------------------------ */

static void
test_frames(void)
{
    uint8_t hdr[OVSDB_BINARY_FRAME_HDR_SIZE];
    uint8_t msg_type;
    uint32_t payload_len;
    size_t n;

    /* Test each message type. */
    enum ovsdb_binary_msg_type types[] = {
        OVSDB_BIN_INITIAL_BEGIN, OVSDB_BIN_ROW_BATCH,
        OVSDB_BIN_INITIAL_END, OVSDB_BIN_UPDATE,
        OVSDB_BIN_UPDATE_BATCH
    };
    size_t i;

    for (i = 0; i < ARRAY_SIZE(types); i++) {
        n = ovsdb_binary_frame_encode(hdr, types[i], 1024);
        ovs_assert(n == OVSDB_BINARY_FRAME_HDR_SIZE);
        ovs_assert(hdr[0] == OVSDB_BINARY_MAGIC);
        ovs_assert(hdr[1] == OVSDB_BINARY_VERSION);

        ovs_assert(ovsdb_binary_frame_decode(hdr, &msg_type, &payload_len));
        ovs_assert(msg_type == types[i]);
        ovs_assert(payload_len == 1024);
    }

    /* Zero-length payload. */
    n = ovsdb_binary_frame_encode(hdr, OVSDB_BIN_ROW_BATCH, 0);
    ovs_assert(n == OVSDB_BINARY_FRAME_HDR_SIZE);
    ovs_assert(ovsdb_binary_frame_decode(hdr, &msg_type, &payload_len));
    ovs_assert(payload_len == 0);

    /* Large payload (just under max). */
    n = ovsdb_binary_frame_encode(hdr, OVSDB_BIN_ROW_BATCH,
                                   OVSDB_BINARY_MAX_PAYLOAD);
    ovs_assert(n == OVSDB_BINARY_FRAME_HDR_SIZE);
    ovs_assert(ovsdb_binary_frame_decode(hdr, &msg_type, &payload_len));
    ovs_assert(payload_len == OVSDB_BINARY_MAX_PAYLOAD);

    printf("frames: PASSED\n");
}

/* ------------------------------------------------------------------ */
/* Invalid frame tests.                                               */
/* ------------------------------------------------------------------ */

static void
test_invalid_frames(void)
{
    uint8_t hdr[OVSDB_BINARY_FRAME_HDR_SIZE];
    uint8_t msg_type;
    uint32_t payload_len;

    /* Wrong magic. */
    memset(hdr, 0, sizeof hdr);
    hdr[0] = 0xAB;
    hdr[1] = OVSDB_BINARY_VERSION;
    ovs_assert(!ovsdb_binary_frame_decode(hdr, &msg_type, &payload_len));

    /* Wrong version. */
    hdr[0] = OVSDB_BINARY_MAGIC;
    hdr[1] = 0xFF;
    ovs_assert(!ovsdb_binary_frame_decode(hdr, &msg_type, &payload_len));

    /* Payload exceeds max (encode with oversized value via raw write). */
    hdr[0] = OVSDB_BINARY_MAGIC;
    hdr[1] = OVSDB_BINARY_VERSION;
    hdr[2] = OVSDB_BIN_ROW_BATCH;
    hdr[3] = 0;
    /* Encode 128MB > 64MB max in network byte order. */
    {
        uint32_t big = htonl(128 * 1024 * 1024);
        memcpy(hdr + 4, &big, 4);
    }
    ovs_assert(!ovsdb_binary_frame_decode(hdr, &msg_type, &payload_len));

    printf("invalid: PASSED\n");
}

/* ------------------------------------------------------------------ */
/* Magic byte detection test.                                         */
/* ------------------------------------------------------------------ */

static void
test_magic(void)
{
    ovs_assert(ovsdb_binary_is_magic(0xDB));
    ovs_assert(!ovsdb_binary_is_magic(0x7B));  /* '{' */
    ovs_assert(!ovsdb_binary_is_magic(0x00));
    ovs_assert(!ovsdb_binary_is_magic(0xFF));

    printf("magic: PASSED\n");
}

/* ------------------------------------------------------------------ */
/* Buffer edge case tests.                                            */
/* ------------------------------------------------------------------ */

static void
test_edges(void)
{
    /* INT64_MIN / INT64_MAX round-trip. */
    {
        struct ovsdb_binary_buf buf;
        struct ovsdb_binary_reader reader;
        union ovsdb_atom src, dst;

        ovsdb_binary_buf_init(&buf);

        src.integer = INT64_MIN;
        ovsdb_binary_serialize_atom(&buf, &src, OVSDB_TYPE_INTEGER, true);
        src.integer = INT64_MAX;
        ovsdb_binary_serialize_atom(&buf, &src, OVSDB_TYPE_INTEGER, true);

        ovsdb_binary_reader_init(&reader, buf.data, buf.size);
        ovs_assert(ovsdb_binary_deserialize_atom(&reader, &dst,
                                                  OVSDB_TYPE_INTEGER, true));
        ovs_assert(dst.integer == INT64_MIN);
        ovs_assert(ovsdb_binary_deserialize_atom(&reader, &dst,
                                                  OVSDB_TYPE_INTEGER, true));
        ovs_assert(dst.integer == INT64_MAX);

        ovsdb_binary_buf_destroy(&buf);
    }

    /* Empty string round-trip. */
    {
        struct ovsdb_binary_buf buf;
        struct ovsdb_binary_reader reader;
        union ovsdb_atom src, dst;

        ovsdb_binary_buf_init(&buf);
        src.s = json_string_create("");
        ovsdb_binary_serialize_atom(&buf, &src, OVSDB_TYPE_STRING, true);

        ovsdb_binary_reader_init(&reader, buf.data, buf.size);
        ovs_assert(ovsdb_binary_deserialize_atom(&reader, &dst,
                                                  OVSDB_TYPE_STRING, true));
        ovs_assert(!strcmp(json_string(dst.s), ""));

        json_destroy(src.s);
        json_destroy(dst.s);
        ovsdb_binary_buf_destroy(&buf);
    }

    /* Zero UUID. */
    {
        struct ovsdb_binary_buf buf;
        struct ovsdb_binary_reader reader;
        union ovsdb_atom src, dst;
        struct uuid zero = UUID_ZERO;

        ovsdb_binary_buf_init(&buf);
        src.uuid = zero;
        ovsdb_binary_serialize_atom(&buf, &src, OVSDB_TYPE_UUID, true);

        ovsdb_binary_reader_init(&reader, buf.data, buf.size);
        ovs_assert(ovsdb_binary_deserialize_atom(&reader, &dst,
                                                  OVSDB_TYPE_UUID, true));
        ovs_assert(uuid_equals(&dst.uuid, &zero));

        ovsdb_binary_buf_destroy(&buf);
    }

    printf("edges: PASSED\n");
}

/* ------------------------------------------------------------------ */
/* Row round-trip test.                                               */
/*                                                                    */
/* Builds a table schema from JSON, creates datums for each column,   */
/* serializes with ovsdb_binary_serialize_row, deserializes with      */
/* ovsdb_binary_deserialize_row, and verifies all columns match.      */
/* ------------------------------------------------------------------ */

static void
test_rows(void)
{
    /* Build a simple table schema with 3 columns:
     *   "name"  : string
     *   "count" : integer
     *   "active": boolean */
    const char *schema_json_str =
        "{"
        "  \"columns\": {"
        "    \"name\":   {\"type\": \"string\"},"
        "    \"count\":  {\"type\": \"integer\"},"
        "    \"active\": {\"type\": \"boolean\"}"
        "  }"
        "}";
    struct json *schema_json;
    struct ovsdb_table_schema *ts;
    struct ovsdb_error *error;
    bool nbo_values[] = { false, true };
    size_t v;

    schema_json = json_from_string(schema_json_str);
    ovs_assert(schema_json->type == JSON_OBJECT);

    error = ovsdb_table_schema_from_json(schema_json, "TestTable", &ts);
    ovs_assert(!error);

    for (v = 0; v < ARRAY_SIZE(nbo_values); v++) {
        bool nbo = nbo_values[v];
        struct ovsdb_binary_buf buf;
        struct ovsdb_binary_reader reader;
        struct ovsdb_column_set columns;
        struct uuid row_uuid;
        struct uuid out_uuid;
        size_t n_all_columns;
        size_t i;
        const struct ovsdb_column *col_name;
        const struct ovsdb_column *col_count;
        const struct ovsdb_column *col_active;

        /* Look up columns by name. */
        col_name = shash_find_data(&ts->columns, "name");
        col_count = shash_find_data(&ts->columns, "count");
        col_active = shash_find_data(&ts->columns, "active");
        ovs_assert(col_name && col_count && col_active);

        /* Build column set with the 3 user columns. */
        ovsdb_column_set_init(&columns);
        ovsdb_column_set_add(&columns, col_name);
        ovsdb_column_set_add(&columns, col_count);
        ovsdb_column_set_add(&columns, col_active);

        /* Build datums for the row (indexed by column->index). */
        n_all_columns = shash_count(&ts->columns);
        struct ovsdb_datum *datums = xmalloc(n_all_columns
                                              * sizeof *datums);
        for (i = 0; i < n_all_columns; i++) {
            ovsdb_datum_init_empty(&datums[i]);
        }

        /* name = "test-row" */
        datums[col_name->index].n = 1;
        datums[col_name->index].keys = xmalloc(sizeof(union ovsdb_atom));
        datums[col_name->index].keys[0].s = json_string_create("test-row");

        /* count = 42 */
        datums[col_count->index].n = 1;
        datums[col_count->index].keys = xmalloc(sizeof(union ovsdb_atom));
        datums[col_count->index].keys[0].integer = 42;

        /* active = true */
        datums[col_active->index].n = 1;
        datums[col_active->index].keys = xmalloc(sizeof(union ovsdb_atom));
        datums[col_active->index].keys[0].boolean = true;

        /* Serialize. */
        uuid_from_string(&row_uuid,
                         "12345678-1234-1234-1234-123456789abc");
        ovsdb_binary_buf_init(&buf);
        ovsdb_binary_serialize_row(&buf, &row_uuid, datums, &columns, nbo);
        ovs_assert(buf.size > 0);

        /* Deserialize. */
        struct ovsdb_datum *out_datums = xmalloc(n_all_columns
                                                  * sizeof *out_datums);
        for (i = 0; i < n_all_columns; i++) {
            ovsdb_datum_init_empty(&out_datums[i]);
        }

        ovsdb_binary_reader_init(&reader, buf.data, buf.size);
        ovs_assert(ovsdb_binary_deserialize_row(&reader, &out_uuid,
                                                 out_datums, n_all_columns,
                                                 ts, nbo));

        /* Verify UUID. */
        ovs_assert(uuid_equals(&out_uuid, &row_uuid));

        /* Verify name. */
        ovs_assert(out_datums[col_name->index].n == 1);
        ovs_assert(!strcmp(
            json_string(out_datums[col_name->index].keys[0].s),
            "test-row"));

        /* Verify count. */
        ovs_assert(out_datums[col_count->index].n == 1);
        ovs_assert(out_datums[col_count->index].keys[0].integer == 42);

        /* Verify active. */
        ovs_assert(out_datums[col_active->index].n == 1);
        ovs_assert(out_datums[col_active->index].keys[0].boolean == true);

        /* Cleanup. */
        for (i = 0; i < n_all_columns; i++) {
            const struct ovsdb_column *c;
            struct shash_node *sn;

            SHASH_FOR_EACH (sn, &ts->columns) {
                c = sn->data;
                if (c->index == i) {
                    ovsdb_datum_destroy(&datums[i], &c->type);
                    ovsdb_datum_destroy(&out_datums[i], &c->type);
                    break;
                }
            }
        }
        free(datums);
        free(out_datums);
        ovsdb_column_set_destroy(&columns);
        ovsdb_binary_buf_destroy(&buf);
    }

    ovsdb_table_schema_destroy(ts);
    json_destroy(schema_json);

    printf("rows: PASSED\n");
}

/* ------------------------------------------------------------------ */
/* Main.                                                              */
/* ------------------------------------------------------------------ */

static struct {
    const char *name;
    void (*fn)(void);
} tests[] = {
    { "atoms", test_atoms },
    { "datums", test_datums },
    { "rows", test_rows },
    { "frames", test_frames },
    { "invalid", test_invalid_frames },
    { "magic", test_magic },
    { "edges", test_edges },
};

int
main(int argc, char *argv[])
{
    size_t i;

    if (argc < 2) {
        /* Run all tests. */
        for (i = 0; i < ARRAY_SIZE(tests); i++) {
            tests[i].fn();
        }
        return 0;
    }

    for (i = 0; i < ARRAY_SIZE(tests); i++) {
        if (!strcmp(argv[1], tests[i].name)) {
            tests[i].fn();
            return 0;
        }
    }

    fprintf(stderr, "unknown test: %s\n", argv[1]);
    return 1;
}
