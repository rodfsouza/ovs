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
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "ovsdb/index-config.h"
#include "ovsdb/index-engine.h"
#include "ovsdb/column.h"
#include "ovsdb/table.h"
#include "openvswitch/json.h"
#include "ovsdb-error.h"
#include "util.h"

/* Write a temporary config file and return its path.
 * Caller must free the returned string. */
static char *
write_temp_config(const char *content)
{
    char *path = xstrdup("/tmp/test-index-config-XXXXXX");
    int fd = mkstemp(path);

    ovs_assert(fd >= 0);
    ovs_assert(write(fd, content, strlen(content))
               == (ssize_t) strlen(content));
    close(fd);
    return path;
}

/* ------------------------------------------------------------------ */
/* T6: Parse valid config file.                                       */
/* ------------------------------------------------------------------ */

static void
test_parse_valid(void)
{
    const char *conf =
        "[Logical_Flow]\n"
        "hash = logical_datapath\n"
        "hash = pipeline\n"
        "\n"
        "[SB_Global]\n"
        "bloom = false\n";
    char *path = write_temp_config(conf);
    struct ovsdb_index_config *config;
    const char **cols;
    size_t n;

    config = ovsdb_index_config_from_file(path);
    ovs_assert(config != NULL);

    /* Logical_Flow: 2 hash columns. */
    n = ovsdb_index_config_get_hash_columns(config, "Logical_Flow", &cols);
    ovs_assert(n == 2);
    ovs_assert(!strcmp(cols[0], "logical_datapath"));
    ovs_assert(!strcmp(cols[1], "pipeline"));
    ovs_assert(ovsdb_index_config_get_bloom(config, "Logical_Flow") == true);

    /* SB_Global: bloom=false. */
    ovs_assert(ovsdb_index_config_get_bloom(config, "SB_Global") == false);
    n = ovsdb_index_config_get_hash_columns(config, "SB_Global", &cols);
    ovs_assert(n == 0);

    ovsdb_index_config_destroy(config);
    unlink(path);
    free(path);
    printf("parse_valid: PASSED\n");
}

/* ------------------------------------------------------------------ */
/* T7: Parse empty config file.                                       */
/* ------------------------------------------------------------------ */

static void
test_parse_empty(void)
{
    const char *conf = "# Just a comment\n\n";
    char *path = write_temp_config(conf);
    struct ovsdb_index_config *config;
    const char **cols;
    size_t n;

    config = ovsdb_index_config_from_file(path);
    ovs_assert(config != NULL);

    /* No tables configured — defaults. */
    ovs_assert(ovsdb_index_config_get_bloom(config, "AnyTable") == true);
    n = ovsdb_index_config_get_hash_columns(config, "AnyTable", &cols);
    ovs_assert(n == 0);

    ovsdb_index_config_destroy(config);
    unlink(path);
    free(path);
    printf("parse_empty: PASSED\n");
}

/* ------------------------------------------------------------------ */
/* T8: Parse config with unknown table (silently ignored).            */
/* ------------------------------------------------------------------ */

static void
test_parse_unknown_table(void)
{
    const char *conf =
        "[NonExistentTable]\n"
        "hash = some_column\n";
    char *path = write_temp_config(conf);
    struct ovsdb_index_config *config;
    const char **cols;
    size_t n;

    config = ovsdb_index_config_from_file(path);
    ovs_assert(config != NULL);

    /* Table is stored — it's the schema that validates later. */
    n = ovsdb_index_config_get_hash_columns(config, "NonExistentTable", &cols);
    ovs_assert(n == 1);
    ovs_assert(!strcmp(cols[0], "some_column"));

    ovsdb_index_config_destroy(config);
    unlink(path);
    free(path);
    printf("parse_unknown_table: PASSED\n");
}

/* ------------------------------------------------------------------ */
/* T9: from_schema_with_config adds extra HASH.                       */
/* ------------------------------------------------------------------ */

static void
test_from_schema_with_config(void)
{
    /* Schema with "name" index. */
    const char *schema_str =
        "{"
        "  \"columns\": {"
        "    \"name\":   {\"type\": \"string\"},"
        "    \"count\":  {\"type\": \"integer\"}"
        "  },"
        "  \"indexes\": [[\"name\"]]"
        "}";
    struct json *j = json_from_string(schema_str);
    struct ovsdb_table_schema *ts;
    struct ovsdb_error *err;

    /* Config adds "count" hash. */
    const char *conf =
        "[TestTable]\n"
        "hash = count\n";
    char *path = write_temp_config(conf);
    struct ovsdb_index_config *config;
    struct ovsdb_index_set *set;

    err = ovsdb_table_schema_from_json(j, "TestTable", &ts);
    ovs_assert(!err);

    config = ovsdb_index_config_from_file(path);
    ovs_assert(config != NULL);

    set = ovsdb_index_set_from_schema_with_config(ts, NULL, config);
    ovs_assert(set != NULL);

    /* Should have: BLOOM + HASH("name") from schema + HASH("count") from config. */
    ovs_assert(ovsdb_index_set_find_bloom(set) != NULL);
    ovs_assert(ovsdb_index_set_find_for_column(set, "name") != NULL);
    ovs_assert(ovsdb_index_set_find_for_column(set, "count") != NULL);
    ovs_assert(set->n_indexes == 3);

    ovsdb_index_set_destroy(set);
    free(set);
    ovsdb_index_config_destroy(config);
    ovsdb_table_schema_destroy(ts);
    json_destroy(j);
    unlink(path);
    free(path);
    printf("from_schema_with_config: PASSED\n");
}

/* ------------------------------------------------------------------ */
/* T10: bloom=false skips BLOOM.                                      */
/* ------------------------------------------------------------------ */

static void
test_bloom_false(void)
{
    const char *schema_str =
        "{"
        "  \"columns\": {"
        "    \"name\": {\"type\": \"string\"}"
        "  }"
        "}";
    struct json *j = json_from_string(schema_str);
    struct ovsdb_table_schema *ts;
    struct ovsdb_error *err;

    const char *conf =
        "[TestTable]\n"
        "bloom = false\n";
    char *path = write_temp_config(conf);
    struct ovsdb_index_config *config;
    struct ovsdb_index_set *set;

    err = ovsdb_table_schema_from_json(j, "TestTable", &ts);
    ovs_assert(!err);

    config = ovsdb_index_config_from_file(path);
    set = ovsdb_index_set_from_schema_with_config(ts, NULL, config);

    /* BLOOM should be absent. */
    ovs_assert(ovsdb_index_set_find_bloom(set) == NULL);
    ovs_assert(set->n_indexes == 0);

    ovsdb_index_set_destroy(set);
    free(set);
    ovsdb_index_config_destroy(config);
    ovsdb_table_schema_destroy(ts);
    json_destroy(j);
    unlink(path);
    free(path);
    printf("bloom_false: PASSED\n");
}

/* ------------------------------------------------------------------ */
/* T12: NULL config = schema-only behavior.                           */
/* ------------------------------------------------------------------ */

static void
test_null_config(void)
{
    const char *schema_str =
        "{"
        "  \"columns\": {"
        "    \"name\": {\"type\": \"string\"}"
        "  },"
        "  \"indexes\": [[\"name\"]]"
        "}";
    struct json *j = json_from_string(schema_str);
    struct ovsdb_table_schema *ts;
    struct ovsdb_error *err;
    struct ovsdb_index_set *set;

    err = ovsdb_table_schema_from_json(j, "TestTable", &ts);
    ovs_assert(!err);

    set = ovsdb_index_set_from_schema_with_config(ts, NULL, NULL);

    /* Same as from_schema: BLOOM + HASH("name"). */
    ovs_assert(ovsdb_index_set_find_bloom(set) != NULL);
    ovs_assert(ovsdb_index_set_find_for_column(set, "name") != NULL);
    ovs_assert(set->n_indexes == 2);

    ovsdb_index_set_destroy(set);
    free(set);
    ovsdb_table_schema_destroy(ts);
    json_destroy(j);
    printf("null_config: PASSED\n");
}

/* ------------------------------------------------------------------ */
/* Main.                                                              */
/* ------------------------------------------------------------------ */

static struct {
    const char *name;
    void (*fn)(void);
} tests[] = {
    { "parse_valid", test_parse_valid },
    { "parse_empty", test_parse_empty },
    { "parse_unknown_table", test_parse_unknown_table },
    { "from_schema_with_config", test_from_schema_with_config },
    { "bloom_false", test_bloom_false },
    { "null_config", test_null_config },
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
