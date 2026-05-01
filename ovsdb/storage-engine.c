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

#include "storage-engine.h"

#include "disk-store.h"
#include "openvswitch/vlog.h"
#include "util.h"

VLOG_DEFINE_THIS_MODULE(ovsdb_storage_engine);

struct ovsdb_storage_engine {
    struct ovsdb_disk_store *ds;
};

/* The cursor is a thin wrapper over the disk store cursor.
 * We cast between the two since the storage engine doesn't
 * add any state. */

struct ovsdb_storage_engine *
ovsdb_storage_engine_create(struct ovsdb_disk_store *ds)
{
    struct ovsdb_storage_engine *se;

    se = xzalloc(sizeof *se);
    se->ds = ds;
    return se;
}

void
ovsdb_storage_engine_destroy(struct ovsdb_storage_engine *se)
{
    if (se) {
        /* The disk store is owned by ovsdb_storage — we don't
         * close it here. */
        free(se);
    }
}

struct ovsdb_disk_store *
ovsdb_storage_engine_get_disk_store(const struct ovsdb_storage_engine *se)
{
    return se->ds;
}

struct ovsdb_row *
ovsdb_storage_engine_read_row(struct ovsdb_storage_engine *se,
                               struct ovsdb_table *table,
                               const struct uuid *uuid)
{
    return ovsdb_disk_store_read_row(se->ds, table, uuid);
}

struct ovsdb_storage_cursor *
ovsdb_storage_engine_cursor_open(struct ovsdb_storage_engine *se,
                                  const char *table_name)
{
    return (struct ovsdb_storage_cursor *)
        ovsdb_disk_store_cursor_open(se->ds, table_name);
}

struct ovsdb_row *
ovsdb_storage_engine_cursor_next(struct ovsdb_storage_cursor *cursor,
                                  struct ovsdb_table *table)
{
    return ovsdb_disk_store_cursor_next(
        (struct ovsdb_disk_store_cursor *) cursor, table);
}

void
ovsdb_storage_engine_cursor_close(struct ovsdb_storage_cursor *cursor)
{
    ovsdb_disk_store_cursor_close(
        (struct ovsdb_disk_store_cursor *) cursor);
}

size_t
ovsdb_storage_engine_count(const struct ovsdb_storage_engine *se,
                            const char *table_name)
{
    return ovsdb_disk_store_count(se->ds, table_name);
}

bool
ovsdb_storage_engine_contains(const struct ovsdb_storage_engine *se,
                               const struct uuid *uuid)
{
    return ovsdb_disk_store_contains(se->ds, uuid);
}
