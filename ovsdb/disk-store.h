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

#ifndef OVSDB_DISK_STORE_H
#define OVSDB_DISK_STORE_H 1

#include <stddef.h>
#include <stdint.h>
#include "compiler.h"
#include "openvswitch/hmap.h"
#include "openvswitch/uuid.h"

struct ovsdb_bloom_filter;
struct ovsdb_row;
struct ovsdb_table;
struct ovsdb_schema;
struct ovsdb_disk_store;
struct ovsdb_disk_store_cursor;

/* Lifecycle. */
struct ovsdb_disk_store *ovsdb_disk_store_open(
    const char *filename, const struct ovsdb_schema *);
void ovsdb_disk_store_close(struct ovsdb_disk_store *);

/* Single-row operations. */
struct ovsdb_error *ovsdb_disk_store_write_row(
    struct ovsdb_disk_store *, const struct ovsdb_row *)
    OVS_WARN_UNUSED_RESULT;
struct ovsdb_error *ovsdb_disk_store_delete_row(
    struct ovsdb_disk_store *, const struct uuid *)
    OVS_WARN_UNUSED_RESULT;
struct ovsdb_row *ovsdb_disk_store_read_row(
    struct ovsdb_disk_store *, struct ovsdb_table *,
    const struct uuid *);

/* Iteration. */
struct ovsdb_disk_store_cursor *ovsdb_disk_store_cursor_open(
    struct ovsdb_disk_store *, const char *table_name);
struct ovsdb_row *ovsdb_disk_store_cursor_next(
    struct ovsdb_disk_store_cursor *, struct ovsdb_table *);
void ovsdb_disk_store_cursor_close(
    struct ovsdb_disk_store_cursor *);

/* Index queries. */
size_t ovsdb_disk_store_count(const struct ovsdb_disk_store *,
                              const char *table_name);
bool ovsdb_disk_store_contains(const struct ovsdb_disk_store *,
                               const struct uuid *);

/* Maintenance. */
struct ovsdb_error *ovsdb_disk_store_compact(
    struct ovsdb_disk_store *)
    OVS_WARN_UNUSED_RESULT;

/* Secondary name index. */
struct ovsdb_name_index {
    struct hmap entries;           /* name_hash → disk_store_index_entry
                                   * (via entry->name_node). */
    char *column_name;            /* Indexed column name (for disk scan). */
    unsigned int column_index;    /* Column index in table schema
                                   * (for fast comparison in query path). */
};

struct ovsdb_name_index *ovsdb_name_index_create(const char *column_name,
                                                  unsigned int column_index);
void ovsdb_name_index_destroy(struct ovsdb_name_index *);
const struct uuid *ovsdb_name_index_find(
    const struct ovsdb_name_index *, const char *name);

void ovsdb_disk_store_set_indexed_column(
    struct ovsdb_disk_store *, const char *column_name);
void ovsdb_disk_store_build_name_index(
    struct ovsdb_disk_store *, struct ovsdb_name_index *);
void ovsdb_disk_store_name_index_add(
    struct ovsdb_disk_store *, struct ovsdb_name_index *,
    const struct uuid *, const char *name);
void ovsdb_disk_store_name_index_remove(
    struct ovsdb_disk_store *, struct ovsdb_name_index *,
    const struct uuid *);

/* Bloom filter rebuild (call after compaction). */
void ovsdb_disk_store_rebuild_bloom(struct ovsdb_disk_store *,
                                    struct ovsdb_bloom_filter **bloom_p,
                                    const char *table_name);

/* Format detection and schema extraction. */
bool ovsdb_disk_store_is_binary(const char *filename);
struct ovsdb_schema *ovsdb_disk_store_read_schema(
    const char *filename);

/* Accessors for an open store. */
struct ovsdb_schema *ovsdb_disk_store_get_schema(
    const struct ovsdb_disk_store *);
const char *ovsdb_disk_store_get_filename(
    const struct ovsdb_disk_store *);

/* UUID iteration (no disk I/O — walks in-memory index).
 * Calls 'cb' once for each non-deleted row UUID in 'table_name'. */
void ovsdb_disk_store_for_each_uuid(
    struct ovsdb_disk_store *, const char *table_name,
    void (*cb)(const struct uuid *, void *aux), void *aux);

#endif /* ovsdb/disk-store.h */
