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

#include "index-engine.h"

#include "bloom-filter.h"
#include "column.h"
#include "disk-store.h"
#include "ovsdb-data.h"
#include "table.h"
#include "openvswitch/hmap.h"
#include "openvswitch/json.h"
#include "openvswitch/vlog.h"
#include "hash.h"
#include "util.h"

VLOG_DEFINE_THIS_MODULE(ovsdb_index_engine);

/* External node for HASH indexes.  Points back to the clustered
 * entry — no UUID copy needed. */
struct ovsdb_index_node {
    struct hmap_node hmap_node;            /* In index's hmap. */
    struct disk_store_index_entry *entry;  /* Clustered entry. */
    union ovsdb_atom key;                  /* Indexed column value. */
};

struct ovsdb_index {
    enum ovsdb_index_type type;
    char *name;
    char *column_name;                     /* HASH only. */
    enum ovsdb_atomic_type key_type;       /* HASH only. */

    union {
        /* BLOOM: wraps an existing bloom filter. */
        struct {
            struct ovsdb_bloom_filter *filter;
            bool owns_filter;              /* True if we created it. */
        } bloom;

        /* HASH: hmap of ovsdb_index_node. */
        struct {
            struct hmap entries;
            size_t count;
        } hash;
    };
};

/* ------------------------------------------------------------------ */
/* Atom hashing helper.                                                */
/* ------------------------------------------------------------------ */

static uint32_t
atom_hash(const union ovsdb_atom *atom, enum ovsdb_atomic_type type)
{
    switch (type) {
    case OVSDB_TYPE_INTEGER:
        return hash_2words((uint32_t) atom->integer,
                           (uint32_t) (atom->integer >> 32));
    case OVSDB_TYPE_REAL:
        return hash_bytes(&atom->real, sizeof atom->real, 0);
    case OVSDB_TYPE_BOOLEAN:
        return hash_boolean(atom->boolean, 0);
    case OVSDB_TYPE_STRING:
        return hash_string(json_string(atom->s), 0);
    case OVSDB_TYPE_UUID:
        return uuid_hash(&atom->uuid);
    case OVSDB_TYPE_VOID:
    case OVSDB_N_TYPES:
    default:
        OVS_NOT_REACHED();
    }
}

static bool
atom_equals(const union ovsdb_atom *a, const union ovsdb_atom *b,
            enum ovsdb_atomic_type type)
{
    switch (type) {
    case OVSDB_TYPE_INTEGER:
        return a->integer == b->integer;
    case OVSDB_TYPE_REAL:
        return a->real == b->real;
    case OVSDB_TYPE_BOOLEAN:
        return a->boolean == b->boolean;
    case OVSDB_TYPE_STRING:
        return !strcmp(json_string(a->s), json_string(b->s));
    case OVSDB_TYPE_UUID:
        return uuid_equals(&a->uuid, &b->uuid);
    case OVSDB_TYPE_VOID:
    case OVSDB_N_TYPES:
    default:
        OVS_NOT_REACHED();
    }
}

/* ------------------------------------------------------------------ */
/* Lifecycle.                                                          */
/* ------------------------------------------------------------------ */

struct ovsdb_index *
ovsdb_index_create(const struct ovsdb_index_spec *spec)
{
    struct ovsdb_index *idx = xzalloc(sizeof *idx);

    idx->type = spec->type;
    idx->name = xstrdup(spec->name);

    switch (spec->type) {
    case OVSDB_IDX_BLOOM:
        /* Bloom filter must be set after creation via the bloom
         * field directly, or populated via ovsdb_index_add. */
        idx->bloom.filter = NULL;
        idx->bloom.owns_filter = false;
        break;

    case OVSDB_IDX_HASH:
        idx->column_name = xstrdup(spec->column_name);
        idx->key_type = spec->key_type;
        hmap_init(&idx->hash.entries);
        idx->hash.count = 0;
        break;
    }

    return idx;
}

void
ovsdb_index_destroy(struct ovsdb_index *idx)
{
    if (!idx) {
        return;
    }

    switch (idx->type) {
    case OVSDB_IDX_BLOOM:
        /* Bloom filter lifecycle managed externally (Phase 4
         * will transfer ownership). */
        break;

    case OVSDB_IDX_HASH: {
        struct ovsdb_index_node *node;
        HMAP_FOR_EACH_SAFE (node, hmap_node, &idx->hash.entries) {
            hmap_remove(&idx->hash.entries, &node->hmap_node);
            ovsdb_atom_destroy(&node->key, idx->key_type);
            free(node);
        }
        hmap_destroy(&idx->hash.entries);
        free(idx->column_name);
        break;
    }
    }

    free(idx->name);
    free(idx);
}

/* ------------------------------------------------------------------ */
/* Mutation.                                                           */
/* ------------------------------------------------------------------ */

void
ovsdb_index_add(struct ovsdb_index *idx,
                 const union ovsdb_atom *key,
                 struct disk_store_index_entry *entry)
{
    switch (idx->type) {
    case OVSDB_IDX_BLOOM:
        /* Bloom doesn't use add — it's populated separately. */
        break;

    case OVSDB_IDX_HASH: {
        struct ovsdb_index_node *node = xmalloc(sizeof *node);
        node->entry = entry;
        ovsdb_atom_clone(&node->key, key, idx->key_type);
        hmap_insert(&idx->hash.entries, &node->hmap_node,
                    atom_hash(key, idx->key_type));
        idx->hash.count++;
        break;
    }
    }
}

void
ovsdb_index_remove(struct ovsdb_index *idx,
                    const union ovsdb_atom *key,
                    struct disk_store_index_entry *entry)
{
    switch (idx->type) {
    case OVSDB_IDX_BLOOM:
        /* Bloom doesn't support removal (rebuild needed). */
        break;

    case OVSDB_IDX_HASH: {
        uint32_t h = atom_hash(key, idx->key_type);
        struct ovsdb_index_node *node;

        HMAP_FOR_EACH_WITH_HASH (node, hmap_node, h, &idx->hash.entries) {
            if (node->entry == entry
                && atom_equals(&node->key, key, idx->key_type)) {
                hmap_remove(&idx->hash.entries, &node->hmap_node);
                ovsdb_atom_destroy(&node->key, idx->key_type);
                free(node);
                idx->hash.count--;
                return;
            }
        }
        break;
    }
    }
}

/* ------------------------------------------------------------------ */
/* BLOOM-specific.                                                     */
/* ------------------------------------------------------------------ */

void
ovsdb_index_set_bloom_filter(struct ovsdb_index *idx,
                              struct ovsdb_bloom_filter *filter)
{
    ovs_assert(idx->type == OVSDB_IDX_BLOOM);
    idx->bloom.filter = filter;
    idx->bloom.owns_filter = false;
}

/* ------------------------------------------------------------------ */
/* Lookup.                                                             */
/* ------------------------------------------------------------------ */

bool
ovsdb_index_contains(const struct ovsdb_index *idx,
                      const struct uuid *uuid)
{
    if (idx->type == OVSDB_IDX_BLOOM && idx->bloom.filter) {
        return ovsdb_bloom_filter_may_contain(idx->bloom.filter, uuid);
    }
    /* Non-bloom indexes don't support contains on UUID. */
    return true;
}

struct disk_store_index_entry *
ovsdb_index_lookup(const struct ovsdb_index *idx,
                    const union ovsdb_atom *key)
{
    if (idx->type != OVSDB_IDX_HASH) {
        return NULL;
    }

    {
        uint32_t h = atom_hash(key, idx->key_type);
        struct ovsdb_index_node *node;

        HMAP_FOR_EACH_WITH_HASH (node, hmap_node, h, &idx->hash.entries) {
            if (atom_equals(&node->key, key, idx->key_type)) {
                return node->entry;
            }
        }
    }
    return NULL;
}

/* ------------------------------------------------------------------ */
/* Metadata.                                                           */
/* ------------------------------------------------------------------ */

enum ovsdb_index_type
ovsdb_index_get_type(const struct ovsdb_index *idx)
{
    return idx->type;
}

const char *
ovsdb_index_get_name(const struct ovsdb_index *idx)
{
    return idx->name;
}

size_t
ovsdb_index_get_count(const struct ovsdb_index *idx)
{
    switch (idx->type) {
    case OVSDB_IDX_BLOOM:
        return 0;  /* Bloom doesn't track count. */
    case OVSDB_IDX_HASH:
        return idx->hash.count;
    }
    return 0;
}

/* ------------------------------------------------------------------ */
/* Index Set.                                                          */
/* ------------------------------------------------------------------ */

void
ovsdb_index_set_init(struct ovsdb_index_set *set)
{
    set->indexes = NULL;
    set->n_indexes = 0;
}

void
ovsdb_index_set_destroy(struct ovsdb_index_set *set)
{
    size_t i;

    for (i = 0; i < set->n_indexes; i++) {
        ovsdb_index_destroy(set->indexes[i]);
    }
    free(set->indexes);
    set->indexes = NULL;
    set->n_indexes = 0;
}

void
ovsdb_index_set_add(struct ovsdb_index_set *set, struct ovsdb_index *idx)
{
    set->indexes = xrealloc(set->indexes,
                             (set->n_indexes + 1) * sizeof *set->indexes);
    set->indexes[set->n_indexes++] = idx;
}

struct ovsdb_index *
ovsdb_index_set_find_for_column(const struct ovsdb_index_set *set,
                                 const char *column_name)
{
    size_t i;

    for (i = 0; i < set->n_indexes; i++) {
        struct ovsdb_index *idx = set->indexes[i];
        if (idx->type == OVSDB_IDX_HASH
            && idx->column_name
            && !strcmp(idx->column_name, column_name)) {
            return idx;
        }
    }
    return NULL;
}

struct ovsdb_index *
ovsdb_index_set_find_bloom(const struct ovsdb_index_set *set)
{
    size_t i;

    for (i = 0; i < set->n_indexes; i++) {
        if (set->indexes[i]->type == OVSDB_IDX_BLOOM) {
            return set->indexes[i];
        }
    }
    return NULL;
}

/* ------------------------------------------------------------------ */
/* Auto-detect from schema.                                            */
/* ------------------------------------------------------------------ */

struct ovsdb_index_set *
ovsdb_index_set_from_schema(const struct ovsdb_table_schema *ts,
                             struct ovsdb_bloom_filter *bloom)
{
    struct ovsdb_index_set *set = xmalloc(sizeof *set);
    struct ovsdb_index_spec spec;
    struct ovsdb_index *idx;
    size_t i;

    ovsdb_index_set_init(set);

    /* Always create BLOOM index for UUID existence. */
    memset(&spec, 0, sizeof spec);
    spec.type = OVSDB_IDX_BLOOM;
    spec.name = "bloom";
    idx = ovsdb_index_create(&spec);
    if (bloom) {
        ovsdb_index_set_bloom_filter(idx, bloom);
    }
    ovsdb_index_set_add(set, idx);

    /* HASH indexes — one per single-column schema index.
     * Supports string, integer, and UUID key types.
     * Multi-column indexes are skipped (future). */
    for (i = 0; i < ts->n_indexes; i++) {
        const struct ovsdb_column_set *sidx = &ts->indexes[i];

        if (sidx->n_columns != 1) {
            continue;  /* Multi-column — skip. */
        }
        if (sidx->columns[0]->type.n_max != 1) {
            continue;  /* Set/map column — skip. */
        }

        switch (sidx->columns[0]->type.key.type) {
        case OVSDB_TYPE_STRING:
        case OVSDB_TYPE_INTEGER:
        case OVSDB_TYPE_UUID:
            memset(&spec, 0, sizeof spec);
            spec.type = OVSDB_IDX_HASH;
            spec.name = sidx->columns[0]->name;
            spec.column_name = sidx->columns[0]->name;
            spec.key_type = sidx->columns[0]->type.key.type;
            ovsdb_index_set_add(set, ovsdb_index_create(&spec));
            break;

        case OVSDB_TYPE_REAL:
        case OVSDB_TYPE_BOOLEAN:
        case OVSDB_TYPE_VOID:
        case OVSDB_N_TYPES:
        default:
            break;  /* Not indexable. */
        }
    }

    return set;
}
