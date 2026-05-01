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

#ifndef OVSDB_STORAGE_ENGINE_H
#define OVSDB_STORAGE_ENGINE_H 1

#include <stdbool.h>
#include <stddef.h>
#include "openvswitch/uuid.h"

struct ovsdb_disk_store;
struct ovsdb_row;
struct ovsdb_table;

/* Storage engine — pure disk I/O interface.
 *
 * Wraps the disk-store layer to provide a clean abstraction
 * for reading and writing rows.  No cache, no indexes, no
 * query logic — those belong to the cache, index engine,
 * and query engine respectively.
 *
 * Thread safety: read operations (read_row, cursor) are
 * thread-safe (use pread, no shared file offset).  Write
 * operations must be called from the main thread only. */

struct ovsdb_storage_engine;

/* Opaque cursor for sequential table iteration. */
struct ovsdb_storage_cursor;

/* --- Lifecycle --- */

struct ovsdb_storage_engine *ovsdb_storage_engine_create(
    struct ovsdb_disk_store *);
void ovsdb_storage_engine_destroy(struct ovsdb_storage_engine *);

/* Returns the underlying disk store (for callers that need
 * direct access during the transition period). */
struct ovsdb_disk_store *ovsdb_storage_engine_get_disk_store(
    const struct ovsdb_storage_engine *);

/* --- Point Read --- */

/* Reads a single row by UUID.  Internally looks up the UUID
 * in the disk store's clustered index (uuid → offset) and
 * performs a pread.
 *
 * Returns the deserialized row, or NULL if not found.
 * Caller takes ownership of the returned row. */
struct ovsdb_row *ovsdb_storage_engine_read_row(
    struct ovsdb_storage_engine *,
    struct ovsdb_table *,
    const struct uuid *);

/* --- Sequential Scan (cursor) --- */

/* Opens a cursor for all rows in 'table_name'.  The cursor
 * snapshots the index entries at open time — concurrent
 * modifications don't affect the iteration.
 *
 * Returns NULL if the table has no rows or on error. */
struct ovsdb_storage_cursor *ovsdb_storage_engine_cursor_open(
    struct ovsdb_storage_engine *, const char *table_name);

/* Returns the next row from the cursor, or NULL when done.
 * Caller takes ownership of the returned row. */
struct ovsdb_row *ovsdb_storage_engine_cursor_next(
    struct ovsdb_storage_cursor *, struct ovsdb_table *);

/* Closes the cursor and frees resources. */
void ovsdb_storage_engine_cursor_close(
    struct ovsdb_storage_cursor *);

/* --- Metadata --- */

/* Returns the number of non-deleted rows for 'table_name'. */
size_t ovsdb_storage_engine_count(
    const struct ovsdb_storage_engine *, const char *table_name);

/* Returns true if 'uuid' exists in the disk store index. */
bool ovsdb_storage_engine_contains(
    const struct ovsdb_storage_engine *, const struct uuid *);

#endif /* ovsdb/storage-engine.h */
