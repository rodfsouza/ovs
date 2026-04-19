/* Copyright (c) 2009, 2010, 2011, 2012 Nicira, Inc.
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

#include <config.h>

#include "table.h"

#include <limits.h>

#include "openvswitch/json.h"
#include "column.h"
#include "ovsdb-error.h"
#include "ovsdb-parser.h"
#include "ovsdb-types.h"
#include "row.h"
#include "row-cache.h"
#include "disk-store.h"
#include "lazy-load.h"
#include "ovsdb.h"
#include "transaction.h"
#include "openvswitch/vlog.h"

VLOG_DEFINE_THIS_MODULE(ovsdb_table);

static void
add_column(struct ovsdb_table_schema *ts, struct ovsdb_column *column)
{
    ovs_assert(!shash_find(&ts->columns, column->name));
    column->index = shash_count(&ts->columns);
    shash_add(&ts->columns, column->name, column);
}

struct ovsdb_table_schema *
ovsdb_table_schema_create(const char *name, bool mutable,
                          unsigned int max_rows, bool is_root)
{
    struct ovsdb_column *uuid, *version;
    struct ovsdb_table_schema *ts;

    ts = xzalloc(sizeof *ts);
    ts->name = xstrdup(name);
    ts->mutable = mutable;
    shash_init(&ts->columns);
    ts->max_rows = max_rows;
    ts->is_root = is_root;

    uuid = ovsdb_column_create("_uuid", false, true, &ovsdb_type_uuid);
    add_column(ts, uuid);
    ovs_assert(uuid->index == OVSDB_COL_UUID);

    version = ovsdb_column_create("_version", false, false, &ovsdb_type_uuid);
    add_column(ts, version);
    ovs_assert(version->index == OVSDB_COL_VERSION);

    ts->n_indexes = 0;
    ts->indexes = NULL;

    return ts;
}

struct ovsdb_table_schema *
ovsdb_table_schema_clone(const struct ovsdb_table_schema *old)
{
    struct ovsdb_table_schema *new;
    struct shash_node *node;
    size_t i;

    new = ovsdb_table_schema_create(old->name, old->mutable,
                                    old->max_rows, old->is_root);
    SHASH_FOR_EACH (node, &old->columns) {
        const struct ovsdb_column *column = node->data;

        if (column->name[0] == '_') {
            /* Added automatically by ovsdb_table_schema_create(). */
            continue;
        }

        add_column(new, ovsdb_column_clone(column));
    }

    new->n_indexes = old->n_indexes;
    new->indexes = xmalloc(new->n_indexes * sizeof *new->indexes);
    for (i = 0; i < new->n_indexes; i++) {
        const struct ovsdb_column_set *old_index = &old->indexes[i];
        struct ovsdb_column_set *new_index = &new->indexes[i];
        size_t j;

        ovsdb_column_set_init(new_index);
        for (j = 0; j < old_index->n_columns; j++) {
            const struct ovsdb_column *old_column = old_index->columns[j];
            const struct ovsdb_column *new_column;

            new_column = ovsdb_table_schema_get_column(new, old_column->name);
            ovsdb_column_set_add(new_index, new_column);
        }
    }

    return new;
}

void
ovsdb_table_schema_destroy(struct ovsdb_table_schema *ts)
{
    struct shash_node *node;
    size_t i;

    for (i = 0; i < ts->n_indexes; i++) {
        ovsdb_column_set_destroy(&ts->indexes[i]);
    }
    free(ts->indexes);

    SHASH_FOR_EACH (node, &ts->columns) {
        ovsdb_column_destroy(node->data);
    }
    shash_destroy(&ts->columns);
    free(ts->name);
    free(ts);
}

struct ovsdb_error *
ovsdb_table_schema_from_json(const struct json *json, const char *name,
                             struct ovsdb_table_schema **tsp)
{
    struct ovsdb_table_schema *ts;
    const struct json *columns, *mutable, *max_rows, *is_root, *indexes;
    struct shash_node *node;
    struct ovsdb_parser parser;
    struct ovsdb_error *error;
    long long int n_max_rows;

    *tsp = NULL;

    ovsdb_parser_init(&parser, json, "table schema for table %s", name);
    columns = ovsdb_parser_member(&parser, "columns", OP_OBJECT);
    mutable = ovsdb_parser_member(&parser, "mutable",
                                  OP_TRUE | OP_FALSE | OP_OPTIONAL);
    max_rows = ovsdb_parser_member(&parser, "maxRows",
                                   OP_INTEGER | OP_OPTIONAL);
    is_root = ovsdb_parser_member(&parser, "isRoot", OP_BOOLEAN | OP_OPTIONAL);
    indexes = ovsdb_parser_member(&parser, "indexes", OP_ARRAY | OP_OPTIONAL);
    error = ovsdb_parser_finish(&parser);
    if (error) {
        return error;
    }

    if (max_rows) {
        if (json_integer(max_rows) <= 0) {
            return ovsdb_syntax_error(json, NULL,
                                      "maxRows must be at least 1");
        }
        n_max_rows = max_rows->integer;
    } else {
        n_max_rows = UINT_MAX;
    }

    if (shash_is_empty(json_object(columns))) {
        return ovsdb_syntax_error(json, NULL,
                                  "table must have at least one column");
    }

    ts = ovsdb_table_schema_create(name,
                                   mutable ? json_boolean(mutable) : true,
                                   MIN(n_max_rows, UINT_MAX),
                                   is_root ? json_boolean(is_root) : false);
    SHASH_FOR_EACH (node, json_object(columns)) {
        struct ovsdb_column *column;

        if (node->name[0] == '_') {
            error = ovsdb_syntax_error(json, NULL, "names beginning with "
                                       "\"_\" are reserved");
        } else if (!ovsdb_parser_is_id(node->name)) {
            error = ovsdb_syntax_error(json, NULL, "name must be a valid id");
        } else {
            error = ovsdb_column_from_json(node->data, node->name, &column);
        }
        if (error) {
            goto error;
        }

        add_column(ts, column);
    }

    if (indexes) {
        size_t i;

        ts->indexes = xmalloc(indexes->array.n * sizeof *ts->indexes);
        for (i = 0; i < indexes->array.n; i++) {
            struct ovsdb_column_set *index = &ts->indexes[i];
            size_t j;

            error = ovsdb_column_set_from_json(indexes->array.elems[i],
                                               ts, index);
            if (error) {
                goto error;
            }
            if (index->n_columns == 0) {
                error = ovsdb_syntax_error(json, NULL, "index must have "
                                           "at least one column");
                goto error;
            }
            ts->n_indexes++;

            for (j = 0; j < index->n_columns; j++) {
                const struct ovsdb_column *column = index->columns[j];

                if (!column->persistent) {
                    error = ovsdb_syntax_error(json, NULL, "ephemeral columns "
                                               "(such as %s) may not be "
                                               "indexed", column->name);
                    goto error;
                }
            }
        }
    }

    *tsp = ts;
    return NULL;

error:
    ovsdb_table_schema_destroy(ts);
    return error;
}

/* Returns table schema 'ts' serialized into JSON.
 *
 * The "isRoot" member is included in the JSON only if its value would differ
 * from 'default_is_root'.  Ordinarily 'default_is_root' should be false,
 * because ordinarily a table would be not be part of the root set if its
 * "isRoot" member is omitted.  However, garbage collection was not originally
 * included in OVSDB, so in older schemas that do not include any "isRoot"
 * members, every table is implicitly part of the root set.  To serialize such
 * a schema in a way that can be read by older OVSDB tools, specify
 * 'default_is_root' as true. */
struct json *
ovsdb_table_schema_to_json(const struct ovsdb_table_schema *ts,
                           bool default_is_root)
{
    struct json *json, *columns;
    struct shash_node *node;

    json = json_object_create();
    if (!ts->mutable) {
        json_object_put(json, "mutable", json_boolean_create(false));
    }
    if (default_is_root != ts->is_root) {
        json_object_put(json, "isRoot", json_boolean_create(ts->is_root));
    }

    columns = json_object_create();

    SHASH_FOR_EACH (node, &ts->columns) {
        const struct ovsdb_column *column = node->data;
        if (node->name[0] != '_') {
            json_object_put(columns, column->name,
                            ovsdb_column_to_json(column));
        }
    }
    json_object_put(json, "columns", columns);
    if (ts->max_rows != UINT_MAX) {
        json_object_put(json, "maxRows", json_integer_create(ts->max_rows));
    }

    if (ts->n_indexes) {
        struct json **indexes;
        size_t i;

        indexes = xmalloc(ts->n_indexes * sizeof *indexes);
        for (i = 0; i < ts->n_indexes; i++) {
            indexes[i] = ovsdb_column_set_to_json(&ts->indexes[i]);
        }
        json_object_put(json, "indexes",
                        json_array_create(indexes, ts->n_indexes));
    }

    return json;
}

const struct ovsdb_column *
ovsdb_table_schema_get_column(const struct ovsdb_table_schema *ts,
                              const char *name)
{
    return shash_find_data(&ts->columns, name);
}

struct ovsdb_table *
ovsdb_table_create(struct ovsdb_table_schema *ts)
{
    struct ovsdb_table *table;
    size_t i;

    table = xmalloc(sizeof *table);
    table->schema = ts;
    table->txn_table = NULL;
    table->indexes = xmalloc(ts->n_indexes * sizeof *table->indexes);
    for (i = 0; i < ts->n_indexes; i++) {
        hmap_init(&table->indexes[i]);
    }
    hmap_init(&table->rows);
    table->log = false;
    table->cache = NULL;
    table->disk_store = NULL;
    table->db = NULL;

    return table;
}

void
ovsdb_table_logging_enable(struct ovsdb_table *table, bool enabled)
{
    table->log = enabled;
}

bool
ovsdb_table_is_logging_enabled(struct ovsdb_table *table)
{
    return table->log;
}

void
ovsdb_table_destroy(struct ovsdb_table *table)
{
    if (table) {
        struct ovsdb_row *row;
        size_t i;

        HMAP_FOR_EACH_SAFE (row, hmap_node, &table->rows) {
            ovsdb_row_destroy(row);
        }
        hmap_destroy(&table->rows);

        for (i = 0; i < table->schema->n_indexes; i++) {
            hmap_destroy(&table->indexes[i]);
        }
        free(table->indexes);

        if (table->cache) {
            ovsdb_row_cache_destroy(table->cache);
        }
        /* disk_store is a shared pointer owned by ovsdb_storage.
         * Do NOT close it here — it is closed via
         * ovsdb_storage_close(). */
        table->disk_store = NULL;

        ovsdb_table_schema_destroy(table->schema);
        free(table);
    }
}

const struct ovsdb_row *
ovsdb_table_get_row(const struct ovsdb_table *table, const struct uuid *uuid)
{
    struct ovsdb_row *row;

    /* Fast path: search in-memory rows hmap. */
    HMAP_FOR_EACH_WITH_HASH (row, hmap_node, uuid_hash(uuid), &table->rows) {
        if (uuid_equals(ovsdb_row_get_uuid(row), uuid)) {
            return row;
        }
    }

    /* Check the row cache (Phase 1). */
    if (table->cache) {
        row = ovsdb_row_cache_lookup(table->cache, uuid);
        if (row) {
            return row;
        }
    }

    /* Disk-store path (Phase 1+2).
     * Check row state in cache to decide: return cached row,
     * submit async load, or wait for in-progress load. */
    if (table->disk_store && table->cache) {
        enum ovsdb_row_state state;
        state = ovsdb_row_cache_get_state(table->cache, uuid);

        switch (state) {
        case OVSDB_ROW_CACHED:
            /* Already loaded — return it. */
            return ovsdb_row_cache_lookup(table->cache, uuid);

        case OVSDB_ROW_LOADING:
            /* Load in progress — caller must park. */
            return NULL;

        case OVSDB_ROW_ERROR:
            /* Load permanently failed — don't retry. */
            return NULL;

        case OVSDB_ROW_UNLOADED:
            /* If in backoff period after a failed retry, don't
             * re-submit yet — return NULL so the trigger parks
             * and retries on the next poll cycle. */
            if (!ovsdb_row_cache_is_retry_ready(table->cache,
                                                uuid)) {
                return NULL;
            }
            /* Submit async load if worker pool available.
             * ovsdb_lazy_load_request() transitions state to
             * LOADING internally on success. */
            if (table->db
                && ovsdb_lazy_load_request(
                       table->db,
                       CONST_CAST(struct ovsdb_table *, table),
                       uuid)) {
                return NULL;  /* Caller must park. */
            }
            /* No worker pool — fall back to sync load. */
            row = ovsdb_disk_store_read_row(
                table->disk_store,
                CONST_CAST(struct ovsdb_table *, table),
                uuid);
            if (row) {
                ovsdb_row_cache_insert(
                    table->cache, row,
                    ovsdb_row_count_atoms(row));
                return row;
            }
            /* Sync load failed — record failure (may transition
             * to ERROR after OVSDB_MAX_LOAD_RETRIES). */
            ovsdb_row_cache_record_load_failure(
                table->cache, uuid);
            break;
        }
    }

    return NULL;
}

/* Iterates all rows in 'table', calling 'cb' for each.
 *
 * If the table has a disk_store, rows are read synchronously from
 * disk via cursor.  Intended for background threads (e.g.
 * compaction_thread) and for ovsdb-tool callers without a cache.
 *
 * Rows yielded by the cursor are destroyed immediately after the
 * callback returns; see ovsdb_table_for_each_row() docs for the
 * lifetime contract.
 *
 * If no disk_store, iterates the in-memory table->rows hmap. */
void
ovsdb_table_for_each_row(const struct ovsdb_table *table,
                         ovsdb_table_row_cb cb, void *aux)
{
    if (table->disk_store) {
        struct ovsdb_disk_store_cursor *cursor;

        cursor = ovsdb_disk_store_cursor_open(table->disk_store,
                                              table->schema->name);
        if (!cursor) {
            VLOG_WARN("ovsdb_table_for_each_row: failed to open "
                      "disk store cursor for table %s",
                      table->schema->name);
            return;
        }

        struct ovsdb_row *row;
        while ((row = ovsdb_disk_store_cursor_next(
                    cursor,
                    CONST_CAST(struct ovsdb_table *, table)))) {
            bool cont = cb(row, aux);
            ovsdb_row_destroy(row);
            if (!cont) {
                break;
            }
        }
        ovsdb_disk_store_cursor_close(cursor);
    } else {
        const struct ovsdb_row *row;

        HMAP_FOR_EACH (row, hmap_node, &table->rows) {
            if (!cb(row, aux)) {
                break;
            }
        }
    }
}

/* Trampoline state for ovsdb_table_for_each_loaded_row() to bridge
 * the cache callback signature into the table callback signature
 * and de-duplicate against table->rows. */
struct loaded_row_aux {
    ovsdb_table_row_cb cb;
    void *aux;
    const struct hmap *table_rows;  /* Skip UUIDs already yielded. */
    bool stop;
};

static bool
loaded_row_cb(const struct ovsdb_row *row, void *aux_)
{
    struct loaded_row_aux *trampoline = aux_;

    /* Skip if this UUID was already yielded from table->rows. */
    if (trampoline->table_rows) {
        const struct uuid *uuid = ovsdb_row_get_uuid(row);
        const struct ovsdb_row *r;
        HMAP_FOR_EACH_WITH_HASH (r, hmap_node, uuid_hash(uuid),
                                 trampoline->table_rows) {
            if (uuid_equals(ovsdb_row_get_uuid(r), uuid)) {
                return true;  /* Continue, but skip this row. */
            }
        }
    }

    if (!trampoline->cb(row, trampoline->aux)) {
        trampoline->stop = true;
        return false;
    }
    return true;
}

/* Iterates all currently-loaded rows in 'table' with stable row
 * pointers.  See ovsdb_table_for_each_loaded_row() in table.h for
 * the lifetime contract.
 *
 * For tables with both table->rows entries (post-startup
 * modifications) and a row cache (disk-loaded snapshot), yields
 * table->rows entries first and then cache CACHED entries whose
 * UUID is not already in table->rows.  This matches the precedence
 * of ovsdb_table_get_row(), which checks table->rows before cache.
 *
 * Does NOT trigger lazy loading; callers must have already
 * submitted a bulk load and waited for completion. */
void
ovsdb_table_for_each_loaded_row(const struct ovsdb_table *table,
                                ovsdb_table_row_cb cb, void *aux)
{
    /* First, yield rows from table->rows (most recent versions). */
    const struct ovsdb_row *row;
    HMAP_FOR_EACH (row, hmap_node, &table->rows) {
        if (!cb(row, aux)) {
            return;
        }
    }

    /* Then, yield cache CACHED entries that aren't already in
     * table->rows. */
    if (table->cache) {
        struct loaded_row_aux trampoline = {
            .cb = cb,
            .aux = aux,
            .table_rows = &table->rows,
            .stop = false,
        };
        ovsdb_row_cache_for_each_loaded(table->cache, loaded_row_cb,
                                        &trampoline);
    }
}

/* Returns true if 'table->rows' already contains a row with 'uuid'. */
static bool
table_rows_contains(const struct ovsdb_table *table,
                    const struct uuid *uuid)
{
    const struct ovsdb_row *row;

    HMAP_FOR_EACH_WITH_HASH (row, hmap_node, uuid_hash(uuid),
                             &table->rows) {
        if (uuid_equals(ovsdb_row_get_uuid(row), uuid)) {
            return true;
        }
    }
    return false;
}

/* Iterates every row logically present in 'table'.  See the header
 * comment in table.h for the full lifetime contract.
 *
 * Implementation: yield 'table->rows' first (stable pointers), then
 * open a disk-store cursor and walk every record whose UUID isn't
 * already in table->rows.  For CACHED entries we prefer the stable
 * cached pointer over the transient cursor row.  For UNLOADED entries
 * the cursor row is handed to the callback TRANSIENTLY, after which
 * we insert it into the cache so follow-up queries hit a warm path;
 * LRU eviction is expected and safe because the callback has already
 * returned (transient contract).  Insertion also transfers ownership
 * to the cache, so we must not destroy the row on that path. */
void
ovsdb_table_for_each_row_from_disk(const struct ovsdb_table *table,
                                   ovsdb_table_row_cb cb, void *aux)
{
    /* Step 1: yield rows from table->rows (stable, hmap-owned). */
    const struct ovsdb_row *row;
    HMAP_FOR_EACH (row, hmap_node, &table->rows) {
        if (!cb(row, aux)) {
            return;
        }
    }

    /* Step 2: plain in-memory table — nothing more to iterate. */
    if (!table->disk_store) {
        return;
    }

    /* Step 3: walk the disk-store cursor for this table. */
    struct ovsdb_disk_store_cursor *cursor;
    cursor = ovsdb_disk_store_cursor_open(table->disk_store,
                                          table->schema->name);
    if (!cursor) {
        VLOG_WARN("ovsdb_table_for_each_row_from_disk: failed to open "
                  "disk store cursor for table %s",
                  table->schema->name);
        return;
    }

    struct ovsdb_row *disk_row;
    while ((disk_row = ovsdb_disk_store_cursor_next(
                cursor,
                CONST_CAST(struct ovsdb_table *, table)))) {
        const struct uuid *uuid = ovsdb_row_get_uuid(disk_row);

        /* Step 4a: skip if already yielded via table->rows. */
        if (table_rows_contains(table, uuid)) {
            ovsdb_row_destroy(disk_row);
            continue;
        }

        /* Step 4b: if CACHED, yield the stable cached pointer and
         * drop the transient disk copy. */
        if (table->cache) {
            const struct ovsdb_row *cached =
                ovsdb_row_cache_lookup(table->cache, uuid);
            if (cached) {
                bool cont = cb(cached, aux);
                ovsdb_row_destroy(disk_row);
                if (!cont) {
                    ovsdb_disk_store_cursor_close(cursor);
                    return;
                }
                continue;
            }
        }

        /* Step 4c: UNLOADED (or no cache).  Yield the transient disk
         * row to 'cb' first, then insert it into the cache so
         * subsequent lookups are cheap.  Ownership transfers to the
         * cache on insert; do not destroy afterwards. */
        bool cont = cb(disk_row, aux);

        if (table->cache) {
            size_t n_atoms = ovsdb_row_count_atoms(disk_row);
            ovsdb_row_cache_insert(table->cache, disk_row, n_atoms);
            /* disk_row is now owned by the cache.  LRU eviction may
             * destroy entries yielded EARLIER in this iteration, but
             * the callback already consumed those (transient contract).
             * disk_row itself is safe from self-eviction: insert places
             * it at the MRU end of the LRU list, and evict__ sweeps
             * from the LRU end, so the just-inserted entry is the
             * last candidate. */
        } else {
            ovsdb_row_destroy(disk_row);
        }

        if (!cont) {
            break;
        }
    }

    ovsdb_disk_store_cursor_close(cursor);
}

struct ovsdb_error *
ovsdb_table_execute_insert(struct ovsdb_txn *txn, const struct uuid *row_uuid,
                           struct ovsdb_table *table, struct json *json_row)
{
    const struct ovsdb_row *old_row = ovsdb_table_get_row(table, row_uuid);
    if (old_row) {
        return ovsdb_error(
                    "consistency violation",
                    "cannot delete missing row "UUID_FMT" from table %s",
                    UUID_ARGS(row_uuid), table->schema->name);
    }

    struct ovsdb_row *row = ovsdb_row_create(table);

    struct ovsdb_error *error = ovsdb_row_from_json(row, json_row,
                                                    NULL, NULL, false);
    if (!error) {
        *ovsdb_row_get_uuid_rw(row) = *row_uuid;
        ovsdb_txn_row_insert(txn, row);
    } else {
        ovsdb_row_destroy(row);
    }

    return error;
}

struct ovsdb_error *
ovsdb_table_execute_delete(struct ovsdb_txn *txn, const struct uuid *row_uuid,
                           struct ovsdb_table *table)
{
    const struct ovsdb_row *row = ovsdb_table_get_row(table, row_uuid);
    if (!row) {
        return ovsdb_error(
                    "consistency violation",
                    "cannot delete missing row "UUID_FMT" from table %s",
                    UUID_ARGS(row_uuid), table->schema->name);
    }

    ovsdb_txn_row_delete(txn, row);
    return NULL;
}

struct ovsdb_error *
ovsdb_table_execute_update(struct ovsdb_txn *txn, const struct uuid *row_uuid,
                           struct ovsdb_table *table, struct json *json_row,
                           bool xor)
{
    const struct ovsdb_row *row = ovsdb_table_get_row(table, row_uuid);
    if (!row) {
        return ovsdb_error(
                    "consistency violation",
                    "cannot modify missing row "UUID_FMT" from table %s",
                    UUID_ARGS(row_uuid), table->schema->name);
    }

    struct ovsdb_column_set columns = OVSDB_COLUMN_SET_INITIALIZER;
    struct ovsdb_row *update = ovsdb_row_create(table);
    struct ovsdb_error *error = ovsdb_row_from_json(update, json_row,
                                                    NULL, &columns, xor);

    if (!error && (xor || !ovsdb_row_equal_columns(row, update, &columns))) {
        struct ovsdb_row *rw_row;

        ovsdb_txn_row_modify(txn, row, &rw_row, NULL);
        error = ovsdb_row_update_columns(rw_row, update, &columns, xor);
    }

    ovsdb_column_set_destroy(&columns);
    ovsdb_row_destroy(update);
    return error;
}
