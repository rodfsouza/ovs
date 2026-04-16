/* Copyright (c) 2009, 2010, 2011 Nicira, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef OVSDB_TABLE_H
#define OVSDB_TABLE_H 1

#include <stdbool.h>
#include "compiler.h"
#include "openvswitch/hmap.h"
#include "openvswitch/shash.h"

struct json;
struct uuid;
struct ovsdb_txn;

/* Schema for a database table. */
struct ovsdb_table_schema {
    char *name;
    bool mutable;
    bool is_root;               /* Part of garbage collection root set? */
    unsigned int max_rows;      /* Maximum number of rows. */
    struct shash columns;       /* Contains "struct ovsdb_column *"s. */
    struct ovsdb_column_set *indexes;
    size_t n_indexes;
};

struct ovsdb_table_schema *ovsdb_table_schema_create(
    const char *name, bool mutable, unsigned int max_rows, bool is_root);
struct ovsdb_table_schema *ovsdb_table_schema_clone(
    const struct ovsdb_table_schema *);
void ovsdb_table_schema_destroy(struct ovsdb_table_schema *);

struct ovsdb_error *ovsdb_table_schema_from_json(const struct json *,
                                                 const char *name,
                                                 struct ovsdb_table_schema **)
    OVS_WARN_UNUSED_RESULT;
struct json *ovsdb_table_schema_to_json(const struct ovsdb_table_schema *,
                                        bool default_is_root);

const struct ovsdb_column *ovsdb_table_schema_get_column(
    const struct ovsdb_table_schema *, const char *name);

/* Database table. */

struct ovsdb_row_cache;
struct ovsdb_disk_store;

struct ovsdb_table {
    struct ovsdb_table_schema *schema;
    struct ovsdb_txn_table *txn_table; /* Only if table is in a transaction. */
    struct hmap rows;           /* Contains "struct ovsdb_row"s. */

    /* An array of schema->n_indexes hmaps, each of which contains "struct
     * ovsdb_row"s.  Each of the hmap_nodes in indexes[i] are at index 'i' at
     * the end of struct ovsdb_row, following the 'fields' member. */
    struct hmap *indexes;

    bool log; /* True if logging is enabled for this table. */

    /* Disk-backed storage (Phase 1).  NULL if disabled. */
    struct ovsdb_row_cache *cache;
    struct ovsdb_disk_store *disk_store;

    /* Back-pointer to owning database (for lazy-load). */
    struct ovsdb *db;
};

struct ovsdb_table *ovsdb_table_create(struct ovsdb_table_schema *);
void ovsdb_table_destroy(struct ovsdb_table *);
void ovsdb_table_logging_enable(struct ovsdb_table *, bool);
bool ovsdb_table_is_logging_enabled(struct ovsdb_table *table);

const struct ovsdb_row *ovsdb_table_get_row(const struct ovsdb_table *,
                                            const struct uuid *);

/* Row iteration callback.  Return true to continue, false to stop. */
typedef bool (*ovsdb_table_row_cb)(const struct ovsdb_row *row, void *aux);

/* Iterates all rows in 'table', calling 'cb' for each.
 *
 * If the table has a disk_store, rows are read from disk via cursor
 * (synchronous I/O — intended for background threads only, e.g.
 * compaction_thread).  If no disk_store, iterates table->rows hmap.
 *
 * Rows loaded from disk are NOT inserted into the cache — the caller
 * is responsible for the returned row's lifetime via the callback. */
void ovsdb_table_for_each_row(const struct ovsdb_table *,
                              ovsdb_table_row_cb cb, void *aux);

/* Below functions adds row modification for ovsdb table to the transaction. */
struct ovsdb_error *ovsdb_table_execute_insert(struct ovsdb_txn *txn,
                                               const struct uuid *row_uuid,
                                               struct ovsdb_table *table,
                                               struct json *new);
struct ovsdb_error *ovsdb_table_execute_delete(struct ovsdb_txn *txn,
                                               const struct uuid *row_uuid,
                                               struct ovsdb_table *table);
struct ovsdb_error *ovsdb_table_execute_update(struct ovsdb_txn *txn,
                                               const struct uuid *row_uuid,
                                               struct ovsdb_table *table,
                                               struct json *new, bool xor);

#endif /* ovsdb/table.h */
