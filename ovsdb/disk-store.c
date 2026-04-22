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

#include "disk-store.h"

#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "bloom-filter.h"
#include "column.h"
#include "hash.h"
#include "openvswitch/dynamic-string.h"
#include "ovsdb-data.h"
#include "ovsdb-error.h"
#include "ovsdb.h"
#include "openvswitch/hmap.h"
#include "openvswitch/json.h"
#include "openvswitch/shash.h"
#include "openvswitch/uuid.h"
#include "openvswitch/vlog.h"
#include "ovs-thread.h"
#include "row.h"
#include "sha1.h"
#include "table.h"
#include "util.h"

VLOG_DEFINE_THIS_MODULE(disk_store);

/* File header magic and version. */
#define DISK_STORE_MAGIC      "BINARYV1"
#define DISK_STORE_MAGIC_LEN  8
#define DISK_STORE_VERSION    1

/* Row flags. */
#define DISK_STORE_FLAG_DELETED  0x01

/* Maximum table/column name length stored on disk. */
#define DISK_STORE_MAX_TABLE_NAME   256
#define DISK_STORE_MAX_COLUMN_NAME  256

/* File header layout (on disk):
 *   8 bytes   magic ("BINARYV1")
 *   4 bytes   format_version (uint32_t, native byte order)
 *  20 bytes   schema_hash (SHA-1 digest)
 *   4 bytes   schema_json_len (uint32_t, native byte order):
 *             length of embedded schema JSON that follows the fixed
 *             header.  Zero in legacy files.
 *  ----
 *  36 bytes fixed header
 *
 * If schema_json_len > 0, 'schema_json_len' bytes of UTF-8 schema
 * JSON follow the fixed header.  Row data begins at offset
 * 36 + schema_json_len.
 *
 * All integer fields use native byte order.  Files are not portable
 * across architectures with different endianness.
 */
#define DISK_STORE_HEADER_SIZE  36

/* Per-row header layout (on disk):
 *  16 bytes   uuid
 *   4 bytes   total_len (uint32_t) -- length of entire record incl header
 *   2 bytes   n_columns (uint16_t)
 *   2 bytes   flags    (uint16_t)
 *  ----
 *  24 bytes total
 */
#define DISK_STORE_ROW_HEADER_SIZE  24
#define DISK_STORE_N_COLUMNS_OFFSET 20  /* Byte offset of n_columns
                                         * within the row header. */

/* In-memory index entry mapping UUID -> file location.
 * Also serves as the anchor for the secondary name index:
 * both the UUID hmap and name hmap reference the same entry. */
struct disk_store_index_entry {
    struct hmap_node hmap_node;   /* In ovsdb_disk_store.index. */
    struct uuid uuid;             /* Row UUID. */
    off_t offset;                 /* Byte offset in file. */
    uint32_t length;              /* Total record length on disk. */
    char *table_name;             /* Owning table name. */
    bool deleted;                 /* Marked for lazy deletion. */

    /* Secondary name index linkage. */
    char *name_value;             /* Indexed column value, or NULL. */
    struct hmap_node name_node;   /* In name_index.entries (by name hash). */
    bool in_name_index;           /* True if name_node is inserted. */
};

/* The disk store handle. */
struct ovsdb_disk_store {
    char *filename;               /* Path to the data file. */
    int fd;                       /* File descriptor (-1 if closed). */
    struct hmap index;            /* UUID -> disk_store_index_entry. */
    struct ovs_rwlock index_rwlock; /* Protects 'index' hmap.  Workers
                                     * acquire read lock for lookups;
                                     * main thread acquires write lock
                                     * for insert/remove/rebuild. */
    uint8_t schema_hash[SHA1_DIGEST_SIZE]; /* SHA-1 of schema JSON. */
    struct ovsdb_schema *schema;  /* Embedded schema (owned). */
    uint32_t schema_json_len;     /* Length of embedded schema JSON.
                                   * Zero in legacy files. */
    char *indexed_column_name;    /* Column to extract for name index,
                                   * or NULL if none.  Set before open. */
};

/* Cursor for iterating rows of a single table. */
struct ovsdb_disk_store_cursor {
    struct ovsdb_disk_store *store;
    char *table_name;

    /* Snapshot of matching index entries at cursor-open time. */
    struct disk_store_index_entry **entries;
    size_t n_entries;
    size_t next_idx;
};

/* ------------------------------------------------------------------ */
/* Serialization buffer helpers.                                       */
/* ------------------------------------------------------------------ */

struct disk_store_buf {
    uint8_t *data;
    size_t size;
    size_t allocated;
};

static void
disk_store_buf_init(struct disk_store_buf *buf)
{
    buf->data = NULL;
    buf->size = 0;
    buf->allocated = 0;
}

static void
disk_store_buf_destroy(struct disk_store_buf *buf)
{
    free(buf->data);
    buf->data = NULL;
    buf->size = 0;
    buf->allocated = 0;
}

static void
disk_store_buf_ensure(struct disk_store_buf *buf, size_t extra)
{
    if (buf->size + extra > buf->allocated) {
        size_t new_alloc = buf->allocated ? buf->allocated * 2 : 1024;
        while (new_alloc < buf->size + extra) {
            new_alloc *= 2;
        }
        buf->data = xrealloc(buf->data, new_alloc);
        buf->allocated = new_alloc;
    }
}

static void
disk_store_buf_put(struct disk_store_buf *buf,
                   const void *p, size_t n)
{
    disk_store_buf_ensure(buf, n);
    memcpy(buf->data + buf->size, p, n);
    buf->size += n;
}

static void
disk_store_buf_put_uint8(struct disk_store_buf *buf, uint8_t v)
{
    disk_store_buf_put(buf, &v, sizeof v);
}

static void
disk_store_buf_put_uint16(struct disk_store_buf *buf, uint16_t v)
{
    disk_store_buf_put(buf, &v, sizeof v);
}

static void
disk_store_buf_put_uint32(struct disk_store_buf *buf, uint32_t v)
{
    disk_store_buf_put(buf, &v, sizeof v);
}

static void
disk_store_buf_put_int64(struct disk_store_buf *buf, int64_t v)
{
    disk_store_buf_put(buf, &v, sizeof v);
}

static void
disk_store_buf_put_double(struct disk_store_buf *buf, double v)
{
    disk_store_buf_put(buf, &v, sizeof v);
}

static void
disk_store_buf_put_uuid(struct disk_store_buf *buf,
                        const struct uuid *uuid)
{
    disk_store_buf_put(buf, uuid, sizeof *uuid);
}

/* ------------------------------------------------------------------ */
/* Deserialization reader over a raw byte buffer.                      */
/* ------------------------------------------------------------------ */

struct disk_store_reader {
    const uint8_t *data;
    size_t size;
    size_t pos;
};

static bool
disk_store_reader_remaining(const struct disk_store_reader *r,
                            size_t n)
{
    return r->pos + n <= r->size;
}

static bool
disk_store_reader_get(struct disk_store_reader *r,
                      void *out, size_t n)
{
    if (!disk_store_reader_remaining(r, n)) {
        return false;
    }
    memcpy(out, r->data + r->pos, n);
    r->pos += n;
    return true;
}

static bool
disk_store_reader_get_uint8(struct disk_store_reader *r,
                            uint8_t *out)
{
    return disk_store_reader_get(r, out, sizeof *out);
}

static bool
disk_store_reader_get_uint16(struct disk_store_reader *r,
                             uint16_t *out)
{
    return disk_store_reader_get(r, out, sizeof *out);
}

static bool
disk_store_reader_get_uint32(struct disk_store_reader *r,
                             uint32_t *out)
{
    return disk_store_reader_get(r, out, sizeof *out);
}

static bool
disk_store_reader_get_int64(struct disk_store_reader *r,
                            int64_t *out)
{
    return disk_store_reader_get(r, out, sizeof *out);
}

static bool
disk_store_reader_get_double(struct disk_store_reader *r,
                             double *out)
{
    return disk_store_reader_get(r, out, sizeof *out);
}

static bool
disk_store_reader_get_uuid(struct disk_store_reader *r,
                           struct uuid *out)
{
    return disk_store_reader_get(r, out, sizeof *out);
}

/* ------------------------------------------------------------------ */
/* Atom serialization / deserialization.                                */
/* ------------------------------------------------------------------ */

static void
disk_store_serialize_atom(struct disk_store_buf *buf,
                          const union ovsdb_atom *atom,
                          enum ovsdb_atomic_type type)
{
    switch (type) {
    case OVSDB_TYPE_INTEGER:
        disk_store_buf_put_int64(buf, atom->integer);
        break;

    case OVSDB_TYPE_REAL:
        disk_store_buf_put_double(buf, atom->real);
        break;

    case OVSDB_TYPE_BOOLEAN:
        disk_store_buf_put_uint8(buf, atom->boolean ? 1 : 0);
        break;

    case OVSDB_TYPE_STRING: {
        const char *s = json_string(atom->s);
        uint32_t len = (uint32_t) strlen(s);
        disk_store_buf_put_uint32(buf, len);
        disk_store_buf_put(buf, s, len);
        break;
    }

    case OVSDB_TYPE_UUID:
        disk_store_buf_put_uuid(buf, &atom->uuid);
        break;

    case OVSDB_TYPE_VOID:
    case OVSDB_N_TYPES:
    default:
        OVS_NOT_REACHED();
    }
}

/* Deserialize a single atom.  Returns true on success. */
static bool
disk_store_deserialize_atom(struct disk_store_reader *r,
                            union ovsdb_atom *atom,
                            enum ovsdb_atomic_type type)
{
    switch (type) {
    case OVSDB_TYPE_INTEGER:
        return disk_store_reader_get_int64(r, &atom->integer);

    case OVSDB_TYPE_REAL:
        return disk_store_reader_get_double(r, &atom->real);

    case OVSDB_TYPE_BOOLEAN: {
        uint8_t v;
        if (!disk_store_reader_get_uint8(r, &v)) {
            return false;
        }
        atom->boolean = (v != 0);
        return true;
    }

    case OVSDB_TYPE_STRING: {
        uint32_t len;
        if (!disk_store_reader_get_uint32(r, &len)) {
            return false;
        }
        if (!disk_store_reader_remaining(r, len)) {
            return false;
        }
        char *s = xmalloc(len + 1);
        memcpy(s, r->data + r->pos, len);
        s[len] = '\0';
        r->pos += len;
        atom->s = json_string_create_nocopy(s);
        return true;
    }

    case OVSDB_TYPE_UUID:
        return disk_store_reader_get_uuid(r, &atom->uuid);

    case OVSDB_TYPE_VOID:
    case OVSDB_N_TYPES:
    default:
        return false;
    }
}

/* ------------------------------------------------------------------ */
/* Datum serialization / deserialization.                               */
/*                                                                     */
/* Format:                                                             */
/*   4 bytes  n       (number of key/value pairs)                      */
/*   n atoms  keys    (each serialized per atom type)                  */
/*   n atoms  values  (only if value_type != VOID)                     */
/* ------------------------------------------------------------------ */

static void
disk_store_serialize_datum(struct disk_store_buf *buf,
                           const struct ovsdb_datum *datum,
                           const struct ovsdb_type *type)
{
    uint32_t n = datum->n;
    unsigned int i;

    disk_store_buf_put_uint32(buf, n);

    for (i = 0; i < n; i++) {
        disk_store_serialize_atom(buf, &datum->keys[i],
                                  type->key.type);
    }

    if (type->value.type != OVSDB_TYPE_VOID) {
        for (i = 0; i < n; i++) {
            disk_store_serialize_atom(buf, &datum->values[i],
                                      type->value.type);
        }
    }
}

/* Deserialize a datum.  The caller must eventually destroy it with
 * ovsdb_datum_destroy().  Returns true on success. */
static bool
disk_store_deserialize_datum(struct disk_store_reader *r,
                             struct ovsdb_datum *datum,
                             const struct ovsdb_type *type)
{
    uint32_t n;
    unsigned int i;

    if (!disk_store_reader_get_uint32(r, &n)) {
        return false;
    }

    datum->n = n;
    datum->refcnt = NULL;
    datum->keys = n ? xmalloc(n * sizeof *datum->keys) : NULL;
    datum->values = NULL;

    for (i = 0; i < n; i++) {
        if (!disk_store_deserialize_atom(r, &datum->keys[i],
                                         type->key.type)) {
            goto error_keys;
        }
    }

    if (type->value.type != OVSDB_TYPE_VOID) {
        datum->values = n ? xmalloc(n * sizeof *datum->values)
                          : NULL;
        for (i = 0; i < n; i++) {
            if (!disk_store_deserialize_atom(
                    r, &datum->values[i], type->value.type)) {
                goto error_values;
            }
        }
    }

    return true;

error_values:
    if (ovsdb_atom_needs_destruction(type->value.type)) {
        while (i-- > 0) {
            ovsdb_atom_destroy(&datum->values[i],
                               type->value.type);
        }
    }
    free(datum->values);
    datum->values = NULL;
    i = n; /* Fall through to free all keys. */

error_keys:
    if (ovsdb_atom_needs_destruction(type->key.type)) {
        while (i-- > 0) {
            ovsdb_atom_destroy(&datum->keys[i],
                               type->key.type);
        }
    }
    free(datum->keys);
    datum->keys = NULL;
    datum->n = 0;
    return false;
}

/* ------------------------------------------------------------------ */
/* Index helpers.                                                       */
/* ------------------------------------------------------------------ */

static uint32_t
disk_store_uuid_hash(const struct uuid *uuid)
{
    return uuid_hash(uuid);
}

static struct disk_store_index_entry *
disk_store_find_entry(const struct ovsdb_disk_store *store,
                      const struct uuid *uuid)
{
    struct disk_store_index_entry *e;
    uint32_t h = disk_store_uuid_hash(uuid);

    HMAP_FOR_EACH_WITH_HASH (e, hmap_node, h, &store->index) {
        if (uuid_equals(&e->uuid, uuid)) {
            return e;
        }
    }
    return NULL;
}

static void
disk_store_index_entry_destroy(struct disk_store_index_entry *e)
{
    free(e->table_name);
    free(e->name_value);
    free(e);
}

/* ------------------------------------------------------------------ */
/* Schema hashing.                                                     */
/* ------------------------------------------------------------------ */

static void
disk_store_compute_schema_hash(const struct ovsdb_schema *schema,
                               uint8_t digest[SHA1_DIGEST_SIZE])
{
    struct json *json = ovsdb_schema_to_json(schema);
    char *s = json_to_string(json, 0);
    sha1_bytes(s, (uint32_t) strlen(s), digest);
    free(s);
    json_destroy(json);
}

/* Serializes 'schema' to a JSON string.  Returns the string (caller
 * must free) and stores its length in '*lenp'. */
static char *
disk_store_schema_to_string(const struct ovsdb_schema *schema, uint32_t *lenp)
{
    struct json *sj = ovsdb_schema_to_json(schema);
    char *s = json_to_string(sj, 0);
    *lenp = strlen(s);
    json_destroy(sj);
    return s;
}

/* ------------------------------------------------------------------ */
/* File header I/O.                                                    */
/* ------------------------------------------------------------------ */

static struct ovsdb_error *
disk_store_write_header(int fd,
                        const uint8_t schema_hash[SHA1_DIGEST_SIZE],
                        const char *schema_json_str,
                        uint32_t schema_json_len)
{
    uint8_t header[DISK_STORE_HEADER_SIZE];
    uint32_t version = DISK_STORE_VERSION;
    ssize_t n;

    memset(header, 0, sizeof header);
    memcpy(header, DISK_STORE_MAGIC, DISK_STORE_MAGIC_LEN);
    memcpy(header + 8, &version, sizeof version);
    memcpy(header + 12, schema_hash, SHA1_DIGEST_SIZE);
    memcpy(header + 32, &schema_json_len, sizeof schema_json_len);

    n = write(fd, header, sizeof header);
    if (n != (ssize_t) sizeof header) {
        return ovsdb_io_error(errno, "failed to write disk store "
                              "header");
    }

    /* Write embedded schema JSON after the fixed header. */
    if (schema_json_len > 0 && schema_json_str) {
        n = write(fd, schema_json_str, schema_json_len);
        if (n != (ssize_t) schema_json_len) {
            return ovsdb_io_error(errno, "failed to write embedded "
                                  "schema JSON");
        }
    }
    return NULL;
}

static struct ovsdb_error *
disk_store_read_header(int fd,
                       uint8_t schema_hash[SHA1_DIGEST_SIZE],
                       uint32_t *schema_json_lenp)
{
    uint8_t header[DISK_STORE_HEADER_SIZE];
    ssize_t n;
    uint32_t version;

    n = pread(fd, header, sizeof header, 0);
    if (n != (ssize_t) sizeof header) {
        return ovsdb_io_error(errno, "failed to read disk store "
                              "header");
    }

    if (memcmp(header, DISK_STORE_MAGIC, DISK_STORE_MAGIC_LEN)) {
        return ovsdb_error(NULL, "bad disk store magic");
    }

    memcpy(&version, header + 8, sizeof version);
    if (version != DISK_STORE_VERSION) {
        return ovsdb_error(NULL, "unsupported disk store version "
                           "%"PRIu32, version);
    }

    memcpy(schema_hash, header + 12, SHA1_DIGEST_SIZE);
    memcpy(schema_json_lenp, header + 32, sizeof *schema_json_lenp);
    return NULL;
}

/* ------------------------------------------------------------------ */
/* Index rebuild: scan existing file to reconstruct in-memory index.    */
/* ------------------------------------------------------------------ */

/* Returns the byte size of a serialized atom of the given type,
 * reading from 'data'.  For fixed-size types this is constant;
 * for strings it reads the 4-byte length prefix. */
static size_t
disk_store_atom_size(const uint8_t *data, uint8_t type_tag)
{
    switch (type_tag) {
    case OVSDB_TYPE_INTEGER: return 8;
    case OVSDB_TYPE_REAL:    return 8;
    case OVSDB_TYPE_BOOLEAN: return 1;
    case OVSDB_TYPE_UUID:    return 16;
    case OVSDB_TYPE_STRING: {
        uint32_t len;
        memcpy(&len, data, sizeof len);
        return sizeof len + len;
    }
    default: return 0;
    }
}

/* Skip past a serialized datum (uint32_t n + n keys + n values).
 * Returns the number of bytes consumed. */
static size_t
disk_store_skip_datum(const uint8_t *data, uint8_t key_type,
                      uint8_t val_type)
{
    uint32_t n;
    memcpy(&n, data, sizeof n);
    size_t offset = sizeof n;

    for (uint32_t i = 0; i < n; i++) {
        offset += disk_store_atom_size(data + offset, key_type);
    }
    if (val_type != OVSDB_TYPE_VOID) {
        for (uint32_t i = 0; i < n; i++) {
            offset += disk_store_atom_size(data + offset, val_type);
        }
    }
    return offset;
}

/* Scan a serialized row record for a specific column by name.
 * Returns the string value if found (caller must free), NULL otherwise.
 * Only works for scalar string columns. */
static char *
disk_store_extract_column_string(const uint8_t *record,
                                 uint32_t total_len,
                                 uint16_t n_columns,
                                 uint16_t table_name_len,
                                 const char *target_column)
{
    size_t target_len = strlen(target_column);
    size_t pos = DISK_STORE_ROW_HEADER_SIZE
                 + sizeof(uint16_t) + table_name_len;

    for (uint16_t c = 0; c < n_columns && pos < total_len; c++) {
        if (pos + sizeof(uint16_t) > total_len) {
            break;
        }
        uint16_t col_name_len;
        memcpy(&col_name_len, record + pos, sizeof col_name_len);
        pos += sizeof col_name_len;

        if (pos + col_name_len + 2 > total_len) {
            break;
        }
        size_t col_name_pos = pos;
        pos += col_name_len;

        uint8_t key_type = record[pos++];
        uint8_t val_type = record[pos++];

        if (col_name_len == target_len
            && !memcmp((const char *)(record + col_name_pos),
                       target_column, target_len)
            && key_type == OVSDB_TYPE_STRING) {
            /* Found target column.  Extract the first string value. */
            if (pos + sizeof(uint32_t) > total_len) {
                break;
            }
            uint32_t datum_n;
            memcpy(&datum_n, record + pos, sizeof datum_n);
            pos += sizeof datum_n;
            if (datum_n >= 1 && pos + sizeof(uint32_t) <= total_len) {
                uint32_t str_len;
                memcpy(&str_len, record + pos, sizeof str_len);
                pos += sizeof str_len;
                if (pos + str_len <= total_len) {
                    return xmemdup0((const char *)(record + pos),
                                   str_len);
                }
            }
            return NULL;
        }

        /* Skip this column's datum. */
        if (pos + sizeof(uint32_t) > total_len) {
            break;
        }
        pos += disk_store_skip_datum(record + pos, key_type, val_type);
    }
    return NULL;
}

static struct ovsdb_error *
disk_store_rebuild_index(struct ovsdb_disk_store *store)
{
    off_t pos = DISK_STORE_HEADER_SIZE + store->schema_json_len;
    struct stat st;

    if (fstat(store->fd, &st) < 0) {
        return ovsdb_io_error(errno, "fstat on disk store");
    }

    while (pos < st.st_size) {
        uint8_t row_header[DISK_STORE_ROW_HEADER_SIZE];
        ssize_t n;
        struct uuid uuid;
        uint32_t total_len;
        uint16_t n_columns;
        uint16_t flags;
        struct disk_store_index_entry *entry;

        n = pread(store->fd, row_header, sizeof row_header, pos);
        if (n != (ssize_t) sizeof row_header) {
            VLOG_WARN("truncated row header at offset %lld",
                      (long long) pos);
            break;
        }

        memcpy(&uuid, row_header, sizeof uuid);
        memcpy(&total_len, row_header + 16, sizeof total_len);
        memcpy(&n_columns, row_header + DISK_STORE_N_COLUMNS_OFFSET,
               sizeof n_columns);
        memcpy(&flags, row_header + 22, sizeof flags);

        if (total_len < DISK_STORE_ROW_HEADER_SIZE) {
            VLOG_WARN("invalid record length %"PRIu32" at offset "
                      "%lld", total_len, (long long) pos);
            break;
        }

        /* Read the full record so we can extract both the table
         * name and the indexed column value in a single pass. */
        {
            uint8_t *record = xmalloc(total_len);
            n = pread(store->fd, record, total_len, pos);
            if (n != (ssize_t) total_len) {
                free(record);
                VLOG_WARN("short read at offset %lld",
                          (long long) pos);
                break;
            }

            uint16_t name_len;
            char name_buf[DISK_STORE_MAX_TABLE_NAME];

            memcpy(&name_len,
                   record + DISK_STORE_ROW_HEADER_SIZE,
                   sizeof name_len);
            if (name_len >= DISK_STORE_MAX_TABLE_NAME) {
                free(record);
                VLOG_WARN("bad table name at offset %lld",
                          (long long) pos);
                break;
            }
            memcpy(name_buf,
                   record + DISK_STORE_ROW_HEADER_SIZE
                   + sizeof name_len,
                   name_len);
            name_buf[name_len] = '\0';

            /* Extract indexed column value from the record. */
            char *name_value = NULL;
            if (store->indexed_column_name
                && !(flags & DISK_STORE_FLAG_DELETED)) {
                name_value = disk_store_extract_column_string(
                    record, total_len, n_columns,
                    name_len, store->indexed_column_name);
            }
            free(record);

            /* If there's an older entry for this UUID, replace. */
            entry = disk_store_find_entry(store, &uuid);
            if (entry) {
                hmap_remove(&store->index, &entry->hmap_node);
                disk_store_index_entry_destroy(entry);
            }

            entry = xzalloc(sizeof *entry);
            entry->uuid = uuid;
            entry->offset = pos;
            entry->length = total_len;
            entry->table_name = xstrdup(name_buf);
            entry->deleted = (flags & DISK_STORE_FLAG_DELETED) != 0;
            entry->name_value = name_value;
            entry->in_name_index = false;
            hmap_insert(&store->index, &entry->hmap_node,
                        disk_store_uuid_hash(&uuid));
        }

        pos += total_len;
    }

    return NULL;
}

/* ------------------------------------------------------------------ */
/* Public API: lifecycle.                                               */
/* ------------------------------------------------------------------ */

struct ovsdb_disk_store *
ovsdb_disk_store_open(const char *filename,
                      const struct ovsdb_schema *schema)
{
    struct ovsdb_disk_store *store;
    uint8_t schema_hash[SHA1_DIGEST_SIZE];
    struct stat st;
    int fd;

    if (schema) {
        disk_store_compute_schema_hash(schema, schema_hash);
    } else {
        memset(schema_hash, 0, SHA1_DIGEST_SIZE);
    }

    fd = open(filename, O_RDWR | O_CREAT, 0666);
    if (fd < 0) {
        VLOG_ERR("failed to open disk store '%s': %s",
                 filename, ovs_strerror(errno));
        return NULL;
    }

    store = xzalloc(sizeof *store);
    store->filename = xstrdup(filename);
    store->fd = fd;
    hmap_init(&store->index);
    ovs_rwlock_init(&store->index_rwlock);
    memcpy(store->schema_hash, schema_hash, SHA1_DIGEST_SIZE);
    store->schema = schema ? ovsdb_schema_clone(schema) : NULL;

    if (fstat(fd, &st) < 0) {
        VLOG_ERR("fstat failed for '%s': %s",
                 filename, ovs_strerror(errno));
        goto error;
    }

    if (st.st_size == 0) {
        /* New file: write header with embedded schema JSON. */
        struct ovsdb_error *err;
        char *schema_str = NULL;
        uint32_t schema_len = 0;

        if (schema) {
            schema_str = disk_store_schema_to_string(schema, &schema_len);
        }

        err = disk_store_write_header(fd, schema_hash,
                                      schema_str, schema_len);
        free(schema_str);
        if (err) {
            char *msg = ovsdb_error_to_string_free(err);
            VLOG_ERR("%s", msg);
            free(msg);
            goto error;
        }
        store->schema_json_len = schema_len;
    } else {
        /* Existing file: validate header and rebuild index. */
        uint8_t existing_hash[SHA1_DIGEST_SIZE] = {0};
        uint32_t schema_json_len = 0;
        struct ovsdb_error *err;

        err = disk_store_read_header(fd, existing_hash,
                                     &schema_json_len);
        if (err) {
            char *msg = ovsdb_error_to_string_free(err);
            VLOG_ERR("%s", msg);
            free(msg);
            goto error;
        }

        if (schema
            && memcmp(existing_hash, schema_hash, SHA1_DIGEST_SIZE)) {
            VLOG_ERR("disk store schema hash mismatch for '%s'",
                     filename);
            goto error;
        }
        /* Store the on-disk hash for later comparison. */
        memcpy(store->schema_hash, existing_hash,
               SHA1_DIGEST_SIZE);
        store->schema_json_len = schema_json_len;

        /* If no schema was provided but the file has an embedded
         * schema, parse it so get_schema() works. */
        if (!store->schema && schema_json_len > 0) {
            char *buf = xmalloc(schema_json_len + 1);
            ssize_t n = pread(fd, buf, schema_json_len,
                              DISK_STORE_HEADER_SIZE);
            if (n == (ssize_t) schema_json_len) {
                buf[schema_json_len] = '\0';
                struct json *sj = json_from_string(buf);
                if (sj->type == JSON_STRING) {
                    VLOG_WARN("%s: embedded schema JSON is malformed: "
                              "%s", filename, json_string(sj));
                } else {
                    struct ovsdb_schema *s;
                    err = ovsdb_schema_from_json(sj, &s);
                    if (!err) {
                        store->schema = s;
                    } else {
                        char *msg = ovsdb_error_to_string_free(err);
                        VLOG_WARN("%s: embedded schema parse error: "
                                  "%s", filename, msg);
                        free(msg);
                    }
                }
                json_destroy(sj);
            } else {
                VLOG_WARN("%s: short read of embedded schema "
                          "(expected %"PRIu32" bytes)",
                          filename, schema_json_len);
            }
            free(buf);
        }

        err = disk_store_rebuild_index(store);
        if (err) {
            char *msg = ovsdb_error_to_string_free(err);
            VLOG_ERR("%s", msg);
            free(msg);
            goto error;
        }
    }

    VLOG_INFO("opened disk store '%s' with %"PRIuSIZE" entries",
              filename, hmap_count(&store->index));
    return store;

error:
    close(fd);
    hmap_destroy(&store->index);
    ovs_rwlock_destroy(&store->index_rwlock);
    ovsdb_schema_destroy(store->schema);
    free(store->filename);
    free(store);
    return NULL;
}

void
ovsdb_disk_store_close(struct ovsdb_disk_store *store)
{
    struct disk_store_index_entry *e;

    if (!store) {
        return;
    }

    ovs_rwlock_wrlock(&store->index_rwlock);
    HMAP_FOR_EACH_SAFE (e, hmap_node, &store->index) {
        hmap_remove(&store->index, &e->hmap_node);
        disk_store_index_entry_destroy(e);
    }
    hmap_destroy(&store->index);
    ovs_rwlock_unlock(&store->index_rwlock);
    ovs_rwlock_destroy(&store->index_rwlock);

    if (store->fd >= 0) {
        close(store->fd);
    }
    ovsdb_schema_destroy(store->schema);
    free(store->filename);
    free(store);
}

/* ------------------------------------------------------------------ */
/* Public API: single-row operations.                                  */
/* ------------------------------------------------------------------ */

/* Serialize a full row (header + table name + column data) into 'buf'.
 * The UUID and n_columns are taken from 'row'. */
static void
disk_store_serialize_row(struct disk_store_buf *buf,
                         const struct ovsdb_row *row,
                         const char *table_name,
                         uint16_t flags)
{
    const struct uuid *uuid = ovsdb_row_get_uuid(row);
    const struct ovsdb_table_schema *ts = row->table->schema;
    struct shash_node *node;
    uint16_t n_columns;
    size_t total_len_offset;
    uint32_t total_len;
    uint16_t name_len;

    /* Count user columns (skip the two standard ones). */
    n_columns = (uint16_t)(shash_count(&ts->columns) - OVSDB_N_STD_COLUMNS);

    /* Row header. */
    disk_store_buf_put_uuid(buf, uuid);

    /* Reserve space for total_len -- we patch it later. */
    total_len_offset = buf->size;
    disk_store_buf_put_uint32(buf, 0);

    disk_store_buf_put_uint16(buf, n_columns);
    disk_store_buf_put_uint16(buf, flags);

    /* Table name (length-prefixed). */
    name_len = (uint16_t) strlen(table_name);
    disk_store_buf_put_uint16(buf, name_len);
    disk_store_buf_put(buf, table_name, name_len);

    /* Per-column layout (name-based, order-independent):
     *   2 bytes  column name length (uint16_t)
     *   N bytes  column name
     *   1 byte   key type tag (ovsdb_atomic_type)
     *   1 byte   value type tag (VOID for sets/scalars)
     *   datum    serialized datum
     */
    SHASH_FOR_EACH (node, &ts->columns) {
        const struct ovsdb_column *col = node->data;
        uint16_t col_name_len;

        if (col->index < OVSDB_N_STD_COLUMNS) {
            continue;
        }

        col_name_len = (uint16_t) strlen(col->name);
        disk_store_buf_put_uint16(buf, col_name_len);
        disk_store_buf_put(buf, col->name, col_name_len);
        disk_store_buf_put_uint8(
            buf, (uint8_t) col->type.key.type);
        disk_store_buf_put_uint8(
            buf, (uint8_t) col->type.value.type);
        disk_store_serialize_datum(
            buf, &row->fields[col->index], &col->type);
    }

    /* Patch total_len. */
    total_len = (uint32_t) buf->size;
    memcpy(buf->data + total_len_offset, &total_len,
           sizeof total_len);
}

struct ovsdb_error *
ovsdb_disk_store_write_row(struct ovsdb_disk_store *store,
                           const struct ovsdb_row *row)
{
    const struct uuid *uuid = ovsdb_row_get_uuid(row);
    const char *table_name = row->table->schema->name;
    struct disk_store_buf buf;
    struct disk_store_index_entry *old_entry;
    struct disk_store_index_entry *entry;
    off_t offset;
    ssize_t n;

    disk_store_buf_init(&buf);
    disk_store_serialize_row(&buf, row, table_name, 0);

    /* Acquire write lock BEFORE disk write so concurrent readers
     * never see a stale index entry pointing to an old offset
     * (prevents data corruption on UUID reuse). */
    ovs_rwlock_wrlock(&store->index_rwlock);

    /* Append to file. */
    offset = lseek(store->fd, 0, SEEK_END);
    if (offset < 0) {
        ovs_rwlock_unlock(&store->index_rwlock);
        disk_store_buf_destroy(&buf);
        return ovsdb_io_error(errno,
                              "lseek failed on disk store");
    }

    n = write(store->fd, buf.data, buf.size);
    if (n != (ssize_t) buf.size) {
        ovs_rwlock_unlock(&store->index_rwlock);
        disk_store_buf_destroy(&buf);
        return ovsdb_io_error(errno,
                              "write failed on disk store");
    }

    /* Update in-memory index (already under write lock). */
    old_entry = disk_store_find_entry(store, uuid);
    if (old_entry) {
        hmap_remove(&store->index, &old_entry->hmap_node);
        disk_store_index_entry_destroy(old_entry);
    }

    entry = xzalloc(sizeof *entry);
    entry->uuid = *uuid;
    entry->offset = offset;
    entry->length = (uint32_t) buf.size;
    entry->table_name = xstrdup(table_name);
    entry->deleted = false;
    hmap_insert(&store->index, &entry->hmap_node,
                disk_store_uuid_hash(uuid));
    ovs_rwlock_unlock(&store->index_rwlock);

    disk_store_buf_destroy(&buf);
    return NULL;
}

struct ovsdb_error *
ovsdb_disk_store_delete_row(struct ovsdb_disk_store *store,
                            const struct uuid *uuid)
{
    struct disk_store_index_entry *entry;

    ovs_rwlock_wrlock(&store->index_rwlock);
    entry = disk_store_find_entry(store, uuid);
    if (!entry) {
        ovs_rwlock_unlock(&store->index_rwlock);
        return ovsdb_error(NULL,
                           "row "UUID_FMT" not found in disk store",
                           UUID_ARGS(uuid));
    }

    entry->deleted = true;
    ovs_rwlock_unlock(&store->index_rwlock);
    return NULL;
}

/* Deserialize a row from a raw buffer that starts immediately after
 * the row header (i.e. at the table-name field).
 * 'data' has length 'len' (= total_len - ROW_HEADER_SIZE).
 * Returns the new row, or NULL on error. */
static struct ovsdb_row *
disk_store_deserialize_row(const uint8_t *data, size_t len,
                           struct ovsdb_table *table,
                           const struct uuid *uuid,
                           uint16_t n_columns)
{
    static struct vlog_rate_limit rl = VLOG_RATE_LIMIT_INIT(5, 20);
    struct disk_store_reader r;
    uint16_t tbl_name_len;
    struct ovsdb_row *row;
    const struct ovsdb_table_schema *ts = table->schema;
    uint16_t i;

    r.data = data;
    r.size = len;
    r.pos = 0;

    /* 'data' starts AFTER the 24-byte fixed row header.
     * First field is the table name (2-byte length + name bytes). */
    if (!disk_store_reader_get_uint16(&r, &tbl_name_len)) {
        return NULL;
    }
    if (!disk_store_reader_remaining(&r, tbl_name_len)) {
        return NULL;
    }
    r.pos += tbl_name_len;

    row = ovsdb_row_create(table);
    *ovsdb_row_get_uuid_rw(row) = *uuid;

    /* Deserialize each column by name lookup.  Each on-disk column
     * is: 2-byte name length, name, key type tag, value type tag,
     * then the serialized datum. */
    for (i = 0; i < n_columns; i++) {
        uint16_t col_name_len;
        char col_name[DISK_STORE_MAX_COLUMN_NAME];
        uint8_t key_tag, val_tag;
        const struct ovsdb_column *col;

        if (!disk_store_reader_get_uint16(&r, &col_name_len)) {
            VLOG_WARN_RL(&rl, "truncated column name length for "
                         "row "UUID_FMT, UUID_ARGS(uuid));
            goto error;
        }
        if (col_name_len >= DISK_STORE_MAX_COLUMN_NAME
            || !disk_store_reader_remaining(&r, col_name_len)) {
            VLOG_WARN_RL(&rl, "bad column name length %"PRIu16
                         " for row "UUID_FMT
                         " (database may need reconversion"
                         " with ovsdb-tool convert-format)",
                         col_name_len, UUID_ARGS(uuid));
            goto error;
        }
        memcpy(col_name, r.data + r.pos, col_name_len);
        col_name[col_name_len] = '\0';
        r.pos += col_name_len;

        if (!disk_store_reader_get_uint8(&r, &key_tag)
            || !disk_store_reader_get_uint8(&r, &val_tag)) {
            VLOG_WARN_RL(&rl, "truncated type tags for column "
                         "'%s' in row "UUID_FMT,
                         col_name, UUID_ARGS(uuid));
            goto error;
        }

        col = shash_find_data(&ts->columns, col_name);
        if (!col) {
            /* Unknown column -- skip its datum.  We can't know
             * the exact size, so we must abort. */
            VLOG_WARN_RL(&rl, "unknown column '%s' in row "
                         UUID_FMT", skipping rest of row",
                         col_name, UUID_ARGS(uuid));
            goto error;
        }

        /* Validate type tags. */
        if (key_tag != (uint8_t) col->type.key.type
            || val_tag != (uint8_t) col->type.value.type) {
            VLOG_WARN_RL(&rl, "type mismatch for column '%s' "
                         "in row "UUID_FMT": expected "
                         "(%d,%d) got (%d,%d)",
                         col->name, UUID_ARGS(uuid),
                         (int) col->type.key.type,
                         (int) col->type.value.type,
                         (int) key_tag, (int) val_tag);
            goto error;
        }

        /* Destroy the default datum that ovsdb_row_create
         * initialized, then deserialize from disk. */
        ovsdb_datum_destroy(&row->fields[col->index],
                            &col->type);

        if (!disk_store_deserialize_datum(
                &r, &row->fields[col->index], &col->type)) {
            VLOG_WARN_RL(&rl, "failed to deserialize column "
                         "'%s' for row "UUID_FMT
                         " at pos %"PRIuSIZE" of %"PRIuSIZE,
                         col->name, UUID_ARGS(uuid),
                         r.pos, r.size);
            /* Re-init to default so destroy is safe. */
            ovsdb_datum_init_default(
                &row->fields[col->index], &col->type);
            goto error;
        }
    }

    return row;

error:
    ovsdb_row_destroy(row);
    return NULL;
}

struct ovsdb_row *
ovsdb_disk_store_read_row(struct ovsdb_disk_store *store,
                          struct ovsdb_table *table,
                          const struct uuid *uuid)
{
    uint8_t *record;
    ssize_t n;
    struct uuid stored_uuid;
    uint32_t total_len;
    uint16_t flags;
    off_t offset;
    uint32_t length;

    /* Copy offset/length under read lock so a concurrent write_row
     * on the main thread cannot free the index entry while we use
     * it.  The pread itself is outside the lock. */
    ovs_rwlock_rdlock(&store->index_rwlock);
    {
        struct disk_store_index_entry *entry;
        entry = disk_store_find_entry(store, uuid);
        if (!entry || entry->deleted) {
            ovs_rwlock_unlock(&store->index_rwlock);
            return NULL;
        }
        offset = entry->offset;
        length = entry->length;
    }
    ovs_rwlock_unlock(&store->index_rwlock);

    /* Read the full record (outside lock — pread is thread-safe). */
    record = xmalloc(length);
    n = pread(store->fd, record, length, offset);
    if (n != (ssize_t) length) {
        VLOG_WARN("short read for row "UUID_FMT" at offset %lld",
                  UUID_ARGS(uuid), (long long) offset);
        free(record);
        return NULL;
    }

    /* Parse the fixed header. */
    memcpy(&stored_uuid, record, sizeof stored_uuid);
    memcpy(&total_len, record + 16, sizeof total_len);
    memcpy(&flags, record + 22, sizeof flags);

    if (flags & DISK_STORE_FLAG_DELETED) {
        free(record);
        return NULL;
    }

    /* Body starts after the 24-byte header. */
    {
        struct ovsdb_row *row;
        uint16_t n_columns;
        size_t body_len = length - DISK_STORE_ROW_HEADER_SIZE;

        memcpy(&n_columns, record + DISK_STORE_N_COLUMNS_OFFSET,
               sizeof n_columns);
        row = disk_store_deserialize_row(
            record + DISK_STORE_ROW_HEADER_SIZE, body_len,
            table, uuid, n_columns);
        free(record);
        return row;
    }
}

/* ------------------------------------------------------------------ */
/* Public API: iteration.                                              */
/* ------------------------------------------------------------------ */

struct ovsdb_disk_store_cursor *
ovsdb_disk_store_cursor_open(struct ovsdb_disk_store *store,
                             const char *table_name)
{
    struct ovsdb_disk_store_cursor *cursor;
    struct disk_store_index_entry *e;
    size_t count = 0;
    size_t idx = 0;

    /* Hold read lock during snapshot to prevent compaction from
     * swapping the index underneath us. */
    ovs_rwlock_rdlock(&store->index_rwlock);

    /* Count matching, non-deleted entries. */
    HMAP_FOR_EACH (e, hmap_node, &store->index) {
        if (!e->deleted && !strcmp(e->table_name, table_name)) {
            count++;
        }
    }

    cursor = xzalloc(sizeof *cursor);
    cursor->store = store;
    cursor->table_name = xstrdup(table_name);
    cursor->n_entries = count;
    cursor->next_idx = 0;
    cursor->entries = count
        ? xmalloc(count * sizeof *cursor->entries) : NULL;

    HMAP_FOR_EACH (e, hmap_node, &store->index) {
        if (!e->deleted && !strcmp(e->table_name, table_name)) {
            cursor->entries[idx++] = e;
        }
    }

    ovs_rwlock_unlock(&store->index_rwlock);

    return cursor;
}

struct ovsdb_row *
ovsdb_disk_store_cursor_next(struct ovsdb_disk_store_cursor *cursor,
                             struct ovsdb_table *table)
{
    while (cursor->next_idx < cursor->n_entries) {
        struct disk_store_index_entry *e;

        e = cursor->entries[cursor->next_idx++];
        if (e->deleted) {
            /* Entry was deleted between cursor_open and now. */
            continue;
        }

        return ovsdb_disk_store_read_row(
            cursor->store, table, &e->uuid);
    }
    return NULL;
}

void
ovsdb_disk_store_cursor_close(struct ovsdb_disk_store_cursor *cursor)
{
    if (!cursor) {
        return;
    }
    free(cursor->entries);
    free(cursor->table_name);
    free(cursor);
}

/* ------------------------------------------------------------------ */
/* Public API: index queries.                                          */
/* ------------------------------------------------------------------ */

size_t
ovsdb_disk_store_count(const struct ovsdb_disk_store *store,
                       const char *table_name)
{
    const struct disk_store_index_entry *e;
    size_t count = 0;

    HMAP_FOR_EACH (e, hmap_node, &store->index) {
        if (!e->deleted && !strcmp(e->table_name, table_name)) {
            count++;
        }
    }
    return count;
}

bool
ovsdb_disk_store_contains(const struct ovsdb_disk_store *store,
                          const struct uuid *uuid)
{
    const struct disk_store_index_entry *e;

    /* Cast away const for hmap lookup (hmap API takes non-const,
     * but we only read). */
    e = disk_store_find_entry(
        CONST_CAST(struct ovsdb_disk_store *, store), uuid);
    return e && !e->deleted;
}

/* ------------------------------------------------------------------ */
/* Public API: compaction.                                             */
/* ------------------------------------------------------------------ */

/* Helper for ordered compaction: stores a deep copy of entry
 * fields needed for the I/O phase (after the lock is released). */
struct compact_entry {
    struct uuid uuid;
    off_t offset;
    uint32_t length;
    char *table_name;
};

struct compact_group {
    struct compact_entry *entries;
    size_t n;
    size_t allocated;
};

static int
compact_compare_by_uuid(const void *a_, const void *b_)
{
    const struct compact_entry *a = a_;
    const struct compact_entry *b = b_;
    return memcmp(&a->uuid, &b->uuid, sizeof(struct uuid));
}

struct ovsdb_error *
ovsdb_disk_store_compact(struct ovsdb_disk_store *store)
{
    char *tmp_filename;
    int tmp_fd;
    struct disk_store_index_entry *e;
    struct hmap new_index;
    struct ovsdb_error *error;

    tmp_filename = xasprintf("%s.compact.tmp", store->filename);

    tmp_fd = open(tmp_filename, O_RDWR | O_CREAT | O_TRUNC, 0666);
    if (tmp_fd < 0) {
        error = ovsdb_io_error(errno,
                               "failed to create compact temp file "
                               "'%s'", tmp_filename);
        free(tmp_filename);
        return error;
    }

    /* Write header with embedded schema.  Prefer re-reading the raw
     * schema bytes from the original file to avoid schema→JSON→schema
     * round-trip fidelity issues (e.g. key ordering differences). */
    {
        char *schema_str = NULL;
        uint32_t schema_len = 0;

        if (store->schema_json_len > 0) {
            /* Re-read embedded schema bytes from original file. */
            schema_str = xmalloc(store->schema_json_len);
            ssize_t rn = pread(store->fd, schema_str,
                               store->schema_json_len,
                               DISK_STORE_HEADER_SIZE);
            if (rn == (ssize_t) store->schema_json_len) {
                schema_len = store->schema_json_len;
            } else {
                free(schema_str);
                schema_str = NULL;
            }
        }

        if (!schema_str && store->schema) {
            /* Fallback: serialize from the parsed schema. */
            schema_str = disk_store_schema_to_string(store->schema,
                                                     &schema_len);
        }

        error = disk_store_write_header(tmp_fd, store->schema_hash,
                                        schema_str, schema_len);
        free(schema_str);
        if (error) {
            close(tmp_fd);
            unlink(tmp_filename);
            free(tmp_filename);
            return error;
        }
    }

    /* Collect non-deleted entries, grouped by table and sorted
     * by UUID within each group.  This produces a compacted file
     * where each table's rows are contiguous and UUID-sorted,
     * improving sequential scan performance (OS readahead). */
    hmap_init(&new_index);

    /* Phase 1: Deep-copy entry fields under read lock and group by
     * table.  We copy rather than snapshot raw pointers so that
     * Phase 3 I/O can proceed safely after releasing the lock. */
    struct shash table_groups;
    shash_init(&table_groups);

    ovs_rwlock_rdlock(&store->index_rwlock);
    HMAP_FOR_EACH (e, hmap_node, &store->index) {
        if (e->deleted) {
            continue;
        }

        struct compact_group *group =
            shash_find_data(&table_groups, e->table_name);
        if (!group) {
            group = xzalloc(sizeof *group);
            shash_add(&table_groups, e->table_name, group);
        }
        if (group->n >= group->allocated) {
            group->allocated = group->allocated ? group->allocated * 2 : 16;
            group->entries = xrealloc(group->entries,
                                      group->allocated
                                      * sizeof *group->entries);
        }
        struct compact_entry *ce = &group->entries[group->n++];
        ce->uuid = e->uuid;
        ce->offset = e->offset;
        ce->length = e->length;
        ce->table_name = xstrdup(e->table_name);
    }
    ovs_rwlock_unlock(&store->index_rwlock);

    /* Phase 2: Sort each group by UUID. */
    struct shash_node *sn;
    SHASH_FOR_EACH (sn, &table_groups) {
        struct compact_group *group = sn->data;
        qsort(group->entries, group->n, sizeof *group->entries,
              compact_compare_by_uuid);
    }

    /* Phase 3: Write groups sequentially to temp file.
     * Uses deep-copied fields from Phase 1 — no lock needed. */
    SHASH_FOR_EACH (sn, &table_groups) {
        struct compact_group *group = sn->data;
        for (size_t i = 0; i < group->n; i++) {
            struct compact_entry *ce = &group->entries[i];

            uint8_t *record = xmalloc(ce->length);
            ssize_t n = pread(store->fd, record, ce->length, ce->offset);
            if (n != (ssize_t) ce->length) {
                free(record);
                VLOG_WARN("short read during compact for "UUID_FMT,
                          UUID_ARGS(&ce->uuid));
                continue;
            }

            off_t new_offset = lseek(tmp_fd, 0, SEEK_END);
            if (new_offset < 0) {
                free(record);
                error = ovsdb_io_error(errno,
                                       "lseek failed during compact");
                goto compact_error;
            }

            n = write(tmp_fd, record, ce->length);
            free(record);
            if (n != (ssize_t) ce->length) {
                error = ovsdb_io_error(errno,
                                       "write failed during compact");
                goto compact_error;
            }

            struct disk_store_index_entry *new_entry;
            new_entry = xzalloc(sizeof *new_entry);
            new_entry->uuid = ce->uuid;
            new_entry->offset = new_offset;
            new_entry->length = ce->length;
            new_entry->table_name = xstrdup(ce->table_name);
            new_entry->deleted = false;
            hmap_insert(&new_index, &new_entry->hmap_node,
                        disk_store_uuid_hash(&new_entry->uuid));
        }
    }

    /* Phase 4: Atomic rename (POSIX guarantees atomicity).
     * Readers with the old fd can still read the old (unlinked) file
     * until we close it. */
    if (rename(tmp_filename, store->filename) < 0) {
        error = ovsdb_io_error(errno,
                               "rename '%s' -> '%s' failed",
                               tmp_filename, store->filename);
        goto compact_error;
    }

    /* Phase 5: Swap fd and index under write lock.
     * This ensures no concurrent reader sees a new index entry
     * pointing to the old fd or vice versa. */
    ovs_rwlock_wrlock(&store->index_rwlock);
    {
        int old_fd = store->fd;
        struct hmap old_index = store->index;

        store->fd = tmp_fd;
        store->index = new_index;

        ovs_rwlock_unlock(&store->index_rwlock);

        /* Cleanup outside lock. */
        close(old_fd);
        HMAP_FOR_EACH_SAFE (e, hmap_node, &old_index) {
            hmap_remove(&old_index, &e->hmap_node);
            disk_store_index_entry_destroy(e);
        }
        hmap_destroy(&old_index);
    }

    /* Cleanup table groups. */
    SHASH_FOR_EACH_SAFE (sn, &table_groups) {
        struct compact_group *group = sn->data;
        for (size_t i = 0; i < group->n; i++) {
            free(group->entries[i].table_name);
        }
        free(group->entries);
        free(group);
    }
    shash_destroy(&table_groups);

    VLOG_INFO("compacted disk store '%s': %"PRIuSIZE" entries",
              store->filename, hmap_count(&store->index));
    free(tmp_filename);
    return NULL;

compact_error:
    /* Clean up new_index. */
    HMAP_FOR_EACH_SAFE (e, hmap_node, &new_index) {
        hmap_remove(&new_index, &e->hmap_node);
        disk_store_index_entry_destroy(e);
    }
    hmap_destroy(&new_index);

    /* Clean up table groups. */
    SHASH_FOR_EACH_SAFE (sn, &table_groups) {
        struct compact_group *group = sn->data;
        for (size_t i = 0; i < group->n; i++) {
            free(group->entries[i].table_name);
        }
        free(group->entries);
        free(group);
    }
    shash_destroy(&table_groups);

    close(tmp_fd);
    unlink(tmp_filename);
    free(tmp_filename);
    return error;
}

/* Calls 'cb' once for each non-deleted UUID in 'table_name'.
 * Walks the in-memory index only — no disk I/O. */
void
ovsdb_disk_store_for_each_uuid(struct ovsdb_disk_store *store,
                               const char *table_name,
                               void (*cb)(const struct uuid *,
                                          void *aux),
                               void *aux)
{
    struct disk_store_index_entry *e;

    HMAP_FOR_EACH (e, hmap_node, &store->index) {
        if (!e->deleted
            && !strcmp(e->table_name, table_name)) {
            cb(&e->uuid, aux);
        }
    }
}

static void
add_bloom_rebuild_cb(const struct uuid *uuid, void *aux)
{
    ovsdb_bloom_filter_add(aux, uuid);
}

/* Rebuilds 'bloom_p' from the current non-deleted UUIDs in 'store'
 * for 'table_name'.  Destroys the old bloom filter and replaces it
 * with a fresh one.  Should be called after compaction to eliminate
 * false positives from deleted UUIDs. */
void
ovsdb_disk_store_rebuild_bloom(struct ovsdb_disk_store *store,
                               struct ovsdb_bloom_filter **bloom_p,
                               const char *table_name)
{
    size_t count = ovsdb_disk_store_count(store, table_name);
    struct ovsdb_bloom_filter *new_bloom =
        ovsdb_bloom_filter_create(count > 0 ? count : 1);

    ovsdb_disk_store_for_each_uuid(store, table_name,
                                   add_bloom_rebuild_cb, new_bloom);

    struct ovsdb_bloom_filter *old = *bloom_p;
    *bloom_p = new_bloom;
    ovsdb_bloom_filter_destroy(old);
}

/* ------------------------------------------------------------------ */
/* Public API: secondary name index.                                   */
/* ------------------------------------------------------------------ */

void
ovsdb_disk_store_set_indexed_column(struct ovsdb_disk_store *store,
                                    const char *column_name)
{
    free(store->indexed_column_name);
    store->indexed_column_name = column_name ? xstrdup(column_name) : NULL;
}

void
ovsdb_disk_store_build_name_index(struct ovsdb_disk_store *store,
                                  struct ovsdb_name_index *ni)
{
    struct disk_store_index_entry *e;

    /* If name values were extracted during rebuild_index (single pass),
     * just insert them into the name hmap.  Otherwise, do a second
     * pass to read and extract them now. */
    bool need_extract = false;
    HMAP_FOR_EACH (e, hmap_node, &store->index) {
        if (!e->deleted && !e->name_value) {
            need_extract = true;
            break;
        }
    }

    if (need_extract) {
        /* Second pass: read each row from disk, extract column. */
        HMAP_FOR_EACH (e, hmap_node, &store->index) {
            if (e->deleted) {
                continue;
            }
            uint8_t *record = xmalloc(e->length);
            ssize_t n = pread(store->fd, record, e->length, e->offset);
            if (n != (ssize_t) e->length) {
                free(record);
                continue;
            }
            uint16_t name_len;
            memcpy(&name_len,
                   record + DISK_STORE_ROW_HEADER_SIZE,
                   sizeof name_len);
            uint16_t n_columns;
            memcpy(&n_columns,
                   record + DISK_STORE_N_COLUMNS_OFFSET,
                   sizeof n_columns);
            e->name_value = disk_store_extract_column_string(
                record, e->length, n_columns,
                name_len, ni->column_name);
            free(record);
        }
    }

    /* Insert entries with name values into the name hmap. */
    HMAP_FOR_EACH (e, hmap_node, &store->index) {
        if (e->deleted || !e->name_value) {
            continue;
        }
        hmap_insert(&ni->entries, &e->name_node,
                    hash_string(e->name_value, 0));
        e->in_name_index = true;
    }
}

const struct uuid *
ovsdb_name_index_find(const struct ovsdb_name_index *ni,
                      const char *name)
{
    if (!ni) {
        return NULL;
    }
    uint32_t h = hash_string(name, 0);
    const struct disk_store_index_entry *e;

    HMAP_FOR_EACH_WITH_HASH (e, name_node, h, &ni->entries) {
        if (!strcmp(e->name_value, name)) {
            return &e->uuid;
        }
    }
    return NULL;
}

void
ovsdb_disk_store_name_index_add(struct ovsdb_disk_store *store,
                                struct ovsdb_name_index *ni,
                                const struct uuid *uuid,
                                const char *name)
{
    struct disk_store_index_entry *e = disk_store_find_entry(store, uuid);
    if (!e) {
        return;
    }

    if (e->in_name_index) {
        hmap_remove(&ni->entries, &e->name_node);
        e->in_name_index = false;
    }
    free(e->name_value);
    e->name_value = xstrdup(name);
    hmap_insert(&ni->entries, &e->name_node,
                hash_string(name, 0));
    e->in_name_index = true;
}

void
ovsdb_disk_store_name_index_remove(struct ovsdb_disk_store *store,
                                   struct ovsdb_name_index *ni,
                                   const struct uuid *uuid)
{
    struct disk_store_index_entry *e = disk_store_find_entry(store, uuid);
    if (!e) {
        return;
    }

    if (e->in_name_index) {
        hmap_remove(&ni->entries, &e->name_node);
        e->in_name_index = false;
    }
    free(e->name_value);
    e->name_value = NULL;
}

struct ovsdb_name_index *
ovsdb_name_index_create(const char *column_name,
                        unsigned int column_index)
{
    struct ovsdb_name_index *ni = xzalloc(sizeof *ni);
    hmap_init(&ni->entries);
    ni->column_name = xstrdup(column_name);
    ni->column_index = column_index;
    return ni;
}

void
ovsdb_name_index_destroy(struct ovsdb_name_index *ni)
{
    if (ni) {
        hmap_destroy(&ni->entries);
        free(ni->column_name);
        free(ni);
    }
}

/* Returns the schema associated with 'store', or NULL. */
struct ovsdb_schema *
ovsdb_disk_store_get_schema(const struct ovsdb_disk_store *store)
{
    return store ? store->schema : NULL;
}

/* Returns the filename of 'store'. */
const char *
ovsdb_disk_store_get_filename(const struct ovsdb_disk_store *store)
{
    return store ? store->filename : NULL;
}

/* Returns true if 'filename' begins with the BINARYV1 magic. */
bool
ovsdb_disk_store_is_binary(const char *filename)
{
    char buf[DISK_STORE_MAGIC_LEN];
    int fd;

    fd = open(filename, O_RDONLY);
    if (fd < 0) {
        return false;
    }

    ssize_t n = read(fd, buf, DISK_STORE_MAGIC_LEN);
    close(fd);

    return (n == DISK_STORE_MAGIC_LEN
            && !memcmp(buf, DISK_STORE_MAGIC, DISK_STORE_MAGIC_LEN));
}

/* Reads the schema from a BINARYV1 file.
 *
 * The file header contains a 'schema_json_len' field.  If nonzero,
 * the full schema JSON is embedded in the file immediately after
 * the 36-byte fixed header.  Legacy files with schema_json_len == 0
 * do not have an embedded schema and this function returns NULL.
 *
 * Returns NULL if the schema cannot be read. */
struct ovsdb_schema *
ovsdb_disk_store_read_schema(const char *filename)
{
    struct ovsdb_disk_store *ds;
    struct ovsdb_schema *schema;

    ds = ovsdb_disk_store_open(filename, NULL);
    if (!ds) {
        return NULL;
    }

    schema = ds->schema ? ovsdb_schema_clone(ds->schema) : NULL;
    ovsdb_disk_store_close(ds);
    return schema;
}
