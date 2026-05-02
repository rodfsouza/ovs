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

#include "index-config.h"

#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>

#include "openvswitch/shash.h"
#include "openvswitch/vlog.h"
#include "util.h"

VLOG_DEFINE_THIS_MODULE(ovsdb_index_config);

/* Per-table config entry. */
struct table_config {
    bool bloom;                /* Default: true. */
    char **hash_columns;       /* Array of column names. */
    size_t n_hash_columns;
    size_t allocated_columns;
};

struct ovsdb_index_config {
    struct shash tables;       /* table_name → table_config. */
};

static struct table_config *
table_config_create(void)
{
    struct table_config *tc = xzalloc(sizeof *tc);
    tc->bloom = true;
    return tc;
}

static void
table_config_destroy(struct table_config *tc)
{
    size_t i;

    for (i = 0; i < tc->n_hash_columns; i++) {
        free(tc->hash_columns[i]);
    }
    free(tc->hash_columns);
    free(tc);
}

static void
table_config_add_hash(struct table_config *tc, const char *column)
{
    if (tc->n_hash_columns >= tc->allocated_columns) {
        tc->allocated_columns = tc->allocated_columns
            ? tc->allocated_columns * 2 : 4;
        tc->hash_columns = xrealloc(tc->hash_columns,
            tc->allocated_columns * sizeof *tc->hash_columns);
    }
    tc->hash_columns[tc->n_hash_columns++] = xstrdup(column);
}

/* Trim leading/trailing whitespace in-place.  Returns pointer
 * into the original string (not a new allocation). */
static char *
trim(char *s)
{
    char *end;

    while (isspace((unsigned char) *s)) {
        s++;
    }
    if (*s == '\0') {
        return s;
    }
    end = s + strlen(s) - 1;
    while (end > s && isspace((unsigned char) *end)) {
        *end-- = '\0';
    }
    return s;
}

struct ovsdb_index_config *
ovsdb_index_config_from_file(const char *filename)
{
    struct ovsdb_index_config *config;
    struct table_config *current_table = NULL;
    FILE *f;
    char line[1024];
    int lineno = 0;

    f = fopen(filename, "r");
    if (!f) {
        VLOG_WARN("cannot open index config file '%s': %s",
                  filename, ovs_strerror(errno));
        return NULL;
    }

    config = xzalloc(sizeof *config);
    shash_init(&config->tables);

    while (fgets(line, sizeof line, f)) {
        char *s;

        lineno++;
        s = trim(line);

        /* Skip empty lines and comments. */
        if (*s == '\0' || *s == '#') {
            continue;
        }

        /* Section header: [TableName] */
        if (*s == '[') {
            char *end = strchr(s, ']');
            if (!end) {
                VLOG_WARN("%s:%d: missing ']' in section header",
                          filename, lineno);
                continue;
            }
            *end = '\0';
            s++;  /* Skip '['. */
            s = trim(s);

            current_table = shash_find_data(&config->tables, s);
            if (!current_table) {
                current_table = table_config_create();
                shash_add(&config->tables, s, current_table);
            }
            continue;
        }

        /* Key = value. */
        {
            char *eq = strchr(s, '=');
            char *key;
            char *val;

            if (!eq) {
                VLOG_WARN("%s:%d: expected 'key = value'",
                          filename, lineno);
                continue;
            }
            if (!current_table) {
                VLOG_WARN("%s:%d: key=value before [Table] section",
                          filename, lineno);
                continue;
            }

            *eq = '\0';
            key = trim(s);
            val = trim(eq + 1);

            if (!strcmp(key, "bloom")) {
                current_table->bloom = !strcmp(val, "true");
            } else if (!strcmp(key, "hash")) {
                table_config_add_hash(current_table, val);
            } else {
                VLOG_WARN("%s:%d: unknown key '%s'",
                          filename, lineno, key);
            }
        }
    }

    fclose(f);
    VLOG_INFO("loaded index config from '%s' (%"PRIuSIZE" tables)",
              filename, shash_count(&config->tables));
    return config;
}

void
ovsdb_index_config_destroy(struct ovsdb_index_config *config)
{
    struct shash_node *node;

    if (!config) {
        return;
    }

    SHASH_FOR_EACH_SAFE (node, &config->tables) {
        table_config_destroy(node->data);
    }
    shash_destroy(&config->tables);
    free(config);
}

size_t
ovsdb_index_config_get_hash_columns(const struct ovsdb_index_config *config,
                                     const char *table_name,
                                     const char ***column_names)
{
    struct table_config *tc;

    if (!config) {
        return 0;
    }
    tc = shash_find_data(&config->tables, table_name);
    if (!tc) {
        return 0;
    }
    *column_names = (const char **) tc->hash_columns;
    return tc->n_hash_columns;
}

bool
ovsdb_index_config_get_bloom(const struct ovsdb_index_config *config,
                              const char *table_name)
{
    struct table_config *tc;

    if (!config) {
        return true;  /* Default: bloom enabled. */
    }
    tc = shash_find_data(&config->tables, table_name);
    if (!tc) {
        return true;
    }
    return tc->bloom;
}
