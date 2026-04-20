/* Copyright (c) 2009, 2010 Nicira, Inc.
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

#include "query.h"

#include "column.h"
#include "condition.h"
#include "disk-store.h"
#include "ovsdb.h"
#include "row.h"
#include "row-cache.h"
#include "table.h"

void
ovsdb_query(struct ovsdb_table *table, const struct ovsdb_condition *cnd,
            bool (*output_row)(const struct ovsdb_row *, void *aux), void *aux)
{
    if (cnd->n_clauses > 0
        && cnd->clauses[0].column->index == OVSDB_COL_UUID
        && cnd->clauses[0].function == OVSDB_F_EQ) {
        /* Optimize the case where the query has a clause of the form "uuid ==
         * <some-uuid>", since we have an index on UUID. */
        const struct ovsdb_row *row;

        row = ovsdb_table_get_row(table, &cnd->clauses[0].arg.keys[0].uuid);
        if (row && row->table == table &&
            ovsdb_condition_match_every_clause(row, cnd)) {
            output_row(row, aux);
        }
    } else if (table->disk_store && table->cache) {
        /* Condition-aware disk scan: read rows one at a time
         * from disk, check condition, cache only matches.
         * This avoids cache pollution — non-matching rows are
         * deserialized and immediately discarded without ever
         * entering the cache or evicting useful entries.
         *
         * First, yield matches from table->rows (in-memory
         * modifications take precedence over disk).  Then scan
         * disk rows via cursor, skipping UUIDs already in
         * table->rows. */
        const struct ovsdb_row *mem_row;
        struct ovsdb_disk_store_cursor *cursor;
        struct ovsdb_row *disk_row;
        bool stop = false;

        HMAP_FOR_EACH (mem_row, hmap_node, &table->rows) {
            if (ovsdb_condition_match_every_clause(mem_row, cnd)
                && !output_row(mem_row, aux)) {
                stop = true;
                break;
            }
        }
        if (stop) {
            goto query_done;
        }

        /* Scan disk rows with condition filtering. */
        cursor = ovsdb_disk_store_cursor_open(
            table->disk_store, table->schema->name);
        if (!cursor) {
            goto query_done;
        }

        while ((disk_row = ovsdb_disk_store_cursor_next(
                    cursor,
                    CONST_CAST(struct ovsdb_table *, table)))) {
            const struct uuid *uuid = ovsdb_row_get_uuid(disk_row);
            const struct ovsdb_row *dup;

            /* Skip if this UUID is in table->rows (already
             * yielded above, in-memory version takes precedence). */
            HMAP_FOR_EACH_WITH_HASH (dup, hmap_node,
                                     uuid_hash(uuid),
                                     &table->rows) {
                if (uuid_equals(ovsdb_row_get_uuid(dup), uuid)) {
                    goto skip_disk_row;
                }
            }

            if (ovsdb_condition_match_every_clause(disk_row, cnd)) {
                /* Match: yield to callback first, then cache.
                 * Yielding before cache transfer avoids the
                 * case where insert + eviction drops the row. */
                bool cont = output_row(disk_row, aux);
                size_t atoms = ovsdb_row_count_atoms(disk_row);
                ovsdb_row_cache_insert(table->cache,
                                       disk_row, atoms);
                if (!cont) {
                    ovsdb_disk_store_cursor_close(cursor);
                    goto query_done;
                }
                continue;
            }

        skip_disk_row:
            ovsdb_row_destroy(disk_row);
        }
        ovsdb_disk_store_cursor_close(cursor);
    query_done:
        ;
    } else {
        /* Linear scan. */
        const struct ovsdb_row *row;

        HMAP_FOR_EACH_SAFE (row, hmap_node, &table->rows) {
            if (ovsdb_condition_match_every_clause(row, cnd) &&
                !output_row(row, aux)) {
                break;
            }
        }
    }
}

static bool
query_row_set_cb(const struct ovsdb_row *row, void *results_)
{
    struct ovsdb_row_set *results = results_;
    ovsdb_row_set_add_row(results, row);
    return true;
}

void
ovsdb_query_row_set(struct ovsdb_table *table,
                    const struct ovsdb_condition *condition,
                    struct ovsdb_row_set *results)
{
    ovsdb_query(table, condition, query_row_set_cb, results);
}

static bool
query_distinct_cb(const struct ovsdb_row *row, void *hash_)
{
    struct ovsdb_row_hash *hash = hash_;
    ovsdb_row_hash_insert(hash, row);
    return true;
}

void
ovsdb_query_distinct(struct ovsdb_table *table,
                     const struct ovsdb_condition *condition,
                     const struct ovsdb_column_set *columns,
                     struct ovsdb_row_set *results)
{
    if (!columns || ovsdb_column_set_contains(columns, OVSDB_COL_UUID)) {
        /* All the result rows are guaranteed to be distinct anyway. */
        ovsdb_query_row_set(table, condition, results);
        return;
    } else {
        /* Use hash table to drop duplicates. */
        struct ovsdb_row_hash_node *node;
        struct ovsdb_row_hash hash;

        ovsdb_row_hash_init(&hash, columns);
        ovsdb_query(table, condition, query_distinct_cb, &hash);
        HMAP_FOR_EACH (node, hmap_node, &hash.rows) {
            ovsdb_row_set_add_row(results, node->row);
        }
        ovsdb_row_hash_destroy(&hash, false);
    }
}
