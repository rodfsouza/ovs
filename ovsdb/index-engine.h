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

#ifndef OVSDB_INDEX_ENGINE_H
#define OVSDB_INDEX_ENGINE_H 1

#include <stdbool.h>
#include <stddef.h>
#include "openvswitch/uuid.h"
#include "ovsdb-types.h"
#include "ovsdb-data.h"

struct disk_store_index_entry;
struct ovsdb_bloom_filter;

/* Index engine — generic secondary index interface.
 *
 * All secondary indexes point to the clustered index entry
 * (disk_store_index_entry) via pointer.  The clustered entry
 * owns uuid + offset + length — secondary indexes never copy
 * the UUID.
 *
 * Two index types:
 *   BLOOM — probabilistic UUID existence check (contains only)
 *   HASH  — exact match on column value → clustered entry pointer
 *
 * Thread safety: indexes are built at startup on the main thread
 * and read-only during normal operation.  Mutations (add/remove)
 * happen only during transaction commit (main thread). */

/* --- Index Types --- */
enum ovsdb_index_type {
    OVSDB_IDX_BLOOM,     /* Probabilistic UUID existence */
    OVSDB_IDX_HASH,      /* Column value → clustered entry */
};

/* --- Index Specification (declarative) --- */
struct ovsdb_index_spec {
    enum ovsdb_index_type type;
    const char *name;              /* Index name (for EXPLAIN) */
    const char *column_name;       /* Column covered (HASH only) */
    enum ovsdb_atomic_type key_type; /* Column atom type (HASH only) */
};

/* --- Index Handle --- */
struct ovsdb_index;

/* --- Lifecycle --- */
struct ovsdb_index *ovsdb_index_create(const struct ovsdb_index_spec *);
void ovsdb_index_destroy(struct ovsdb_index *);

/* --- Mutation --- */

/* Add a key → clustered entry mapping. */
void ovsdb_index_add(struct ovsdb_index *,
                      const union ovsdb_atom *key,
                      struct disk_store_index_entry *entry);

/* Remove a key → clustered entry mapping. */
void ovsdb_index_remove(struct ovsdb_index *,
                         const union ovsdb_atom *key,
                         struct disk_store_index_entry *entry);

/* --- Lookup --- */

/* BLOOM: fast negative UUID check.
 * Returns false = definitely not in table.
 * Returns true = may exist (proceed to pread). */
bool ovsdb_index_contains(const struct ovsdb_index *,
                           const struct uuid *uuid);

/* HASH: exact match on column value.
 * Returns pointer to clustered entry, or NULL.
 * Caller uses entry->offset for pread. */
struct disk_store_index_entry *ovsdb_index_lookup(
    const struct ovsdb_index *,
    const union ovsdb_atom *key);

/* --- BLOOM-specific --- */

/* Attach an existing bloom filter to a BLOOM index.
 * The index does NOT take ownership — caller manages lifetime. */
void ovsdb_index_set_bloom_filter(struct ovsdb_index *,
                                   struct ovsdb_bloom_filter *);

/* --- Metadata --- */
enum ovsdb_index_type ovsdb_index_get_type(const struct ovsdb_index *);
const char *ovsdb_index_get_name(const struct ovsdb_index *);
size_t ovsdb_index_get_count(const struct ovsdb_index *);

/* --- Index Set (per-table collection) --- */

struct ovsdb_index_set {
    struct ovsdb_index **indexes;
    size_t n_indexes;
};

void ovsdb_index_set_init(struct ovsdb_index_set *);
void ovsdb_index_set_destroy(struct ovsdb_index_set *);
void ovsdb_index_set_add(struct ovsdb_index_set *, struct ovsdb_index *);

/* Auto-detect indexes from table schema:
 *   - BLOOM for UUID existence (always)
 *   - HASH for each single-column index declared in schema
 *     (string, integer, or UUID columns with n_max==1)
 * The BLOOM index wraps 'bloom' (not owned by the set).
 * HASH indexes are created empty — caller must populate. */
struct ovsdb_table_schema;
struct ovsdb_index_set *ovsdb_index_set_from_schema(
    const struct ovsdb_table_schema *,
    struct ovsdb_bloom_filter *bloom);  /* Attached to BLOOM index */

/* Like from_schema, but overlays config: extra HASH indexes
 * and bloom on/off overrides.  'config' may be NULL. */
struct ovsdb_index_config;
struct ovsdb_index_set *ovsdb_index_set_from_schema_with_config(
    const struct ovsdb_table_schema *,
    struct ovsdb_bloom_filter *bloom,
    const struct ovsdb_index_config *config);

/* Find the first HASH index covering 'column_name', or NULL. */
struct ovsdb_index *ovsdb_index_set_find_for_column(
    const struct ovsdb_index_set *, const char *column_name);

/* Find the BLOOM index, or NULL. */
struct ovsdb_index *ovsdb_index_set_find_bloom(
    const struct ovsdb_index_set *);

#endif /* ovsdb/index-engine.h */
