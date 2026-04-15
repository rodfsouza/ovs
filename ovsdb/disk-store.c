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

#include "column.h"
#include "openvswitch/dynamic-string.h"
#include "ovsdb-data.h"
#include "ovsdb-error.h"
#include "ovsdb.h"
#include "openvswitch/hmap.h"
#include "openvswitch/json.h"
#include "openvswitch/shash.h"
#include "openvswitch/uuid.h"
#include "openvswitch/vlog.h"
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

/* Maximum table name length stored on disk. */
#define DISK_STORE_MAX_TABLE_NAME  256

/* File header layout (on disk):
 *   8 bytes   magic ("BINARYV1")
 *   4 bytes   format_version (uint32_t, network order)
 *  20 bytes   schema_hash (SHA-1 digest)
 *   4 bytes   schema_json_len (uint32_t): length of embedded schema JSON
 *             that follows the fixed header.  Zero in legacy files.
 *  ----
 *  36 bytes fixed header
 *
 * If schema_json_len > 0, 'schema_json_len' bytes of UTF-8 schema
 * JSON follow the fixed header.  Row data begins at offset
 * 36 + schema_json_len.
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

/* In-memory index entry mapping UUID -> file location. */
struct disk_store_index_entry {
    struct hmap_node hmap_node;   /* In ovsdb_disk_store.index. */
    struct uuid uuid;             /* Row UUID. */
    off_t offset;                 /* Byte offset in file. */
    uint32_t length;              /* Total record length on disk. */
    char *table_name;             /* Owning table name. */
    bool deleted;                 /* Marked for lazy deletion. */
};

/* The disk store handle. */
struct ovsdb_disk_store {
    char *filename;               /* Path to the data file. */
    int fd;                       /* File descriptor (-1 if closed). */
    struct hmap index;            /* UUID -> disk_store_index_entry. */
    uint8_t schema_hash[SHA1_DIGEST_SIZE]; /* SHA-1 of schema JSON. */
    struct ovsdb_schema *schema;  /* Embedded schema (owned). */
    uint32_t schema_json_len;     /* Length of embedded schema JSON.
                                   * Zero in legacy files. */
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
        memcpy(&n_columns, row_header + 20, sizeof n_columns);
        memcpy(&flags, row_header + 22, sizeof flags);

        if (total_len < DISK_STORE_ROW_HEADER_SIZE) {
            VLOG_WARN("invalid record length %"PRIu32" at offset "
                      "%lld", total_len, (long long) pos);
            break;
        }

        /* Read the table name that follows the row header.
         * We store a 2-byte name length + name bytes right
         * after the fixed row header. */
        {
            uint16_t name_len;
            char name_buf[DISK_STORE_MAX_TABLE_NAME];

            n = pread(store->fd, &name_len, sizeof name_len,
                      pos + DISK_STORE_ROW_HEADER_SIZE);
            if (n != (ssize_t) sizeof name_len
                || name_len >= DISK_STORE_MAX_TABLE_NAME) {
                VLOG_WARN("bad table name at offset %lld",
                          (long long) pos);
                break;
            }

            n = pread(store->fd, name_buf, name_len,
                      pos + DISK_STORE_ROW_HEADER_SIZE
                      + sizeof name_len);
            if (n != (ssize_t) name_len) {
                VLOG_WARN("truncated table name at offset %lld",
                          (long long) pos);
                break;
            }
            name_buf[name_len] = '\0';

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
            struct json *sj = ovsdb_schema_to_json(schema);
            schema_str = json_to_string(sj, 0);
            schema_len = strlen(schema_str);
            json_destroy(sj);
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
        uint8_t existing_hash[SHA1_DIGEST_SIZE];
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
                if (sj->type != JSON_STRING) {
                    struct ovsdb_schema *s;
                    err = ovsdb_schema_from_json(sj, &s);
                    if (!err) {
                        store->schema = s;
                    } else {
                        ovsdb_error_destroy(err);
                    }
                }
                json_destroy(sj);
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

    HMAP_FOR_EACH_SAFE (e, hmap_node, &store->index) {
        hmap_remove(&store->index, &e->hmap_node);
        disk_store_index_entry_destroy(e);
    }
    hmap_destroy(&store->index);

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

    /* Serialize each non-standard column.
     * For each column we write:
     *   1 byte   column type tag (key atomic type)
     *   datum    serialized datum
     */
    SHASH_FOR_EACH (node, &ts->columns) {
        const struct ovsdb_column *col = node->data;

        if (col->index < OVSDB_N_STD_COLUMNS) {
            continue;
        }

        disk_store_buf_put_uint8(
            buf, (uint8_t) col->type.key.type);
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

    /* Append to file. */
    offset = lseek(store->fd, 0, SEEK_END);
    if (offset < 0) {
        disk_store_buf_destroy(&buf);
        return ovsdb_io_error(errno,
                              "lseek failed on disk store");
    }

    n = write(store->fd, buf.data, buf.size);
    if (n != (ssize_t) buf.size) {
        disk_store_buf_destroy(&buf);
        return ovsdb_io_error(errno,
                              "write failed on disk store");
    }

    /* Update in-memory index. */
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

    disk_store_buf_destroy(&buf);
    return NULL;
}

struct ovsdb_error *
ovsdb_disk_store_delete_row(struct ovsdb_disk_store *store,
                            const struct uuid *uuid)
{
    struct disk_store_index_entry *entry;

    entry = disk_store_find_entry(store, uuid);
    if (!entry) {
        return ovsdb_error(NULL,
                           "row "UUID_FMT" not found in disk store",
                           UUID_ARGS(uuid));
    }

    entry->deleted = true;
    return NULL;
}

/* Deserialize a row from a raw buffer that starts immediately after
 * the row header (i.e. at the table-name field).
 * 'data' has length 'len' (= total_len - ROW_HEADER_SIZE).
 * Returns the new row, or NULL on error. */
static struct ovsdb_row *
disk_store_deserialize_row(const uint8_t *data, size_t len,
                           struct ovsdb_table *table,
                           const struct uuid *uuid)
{
    struct disk_store_reader r;
    uint16_t name_len;
    struct ovsdb_row *row;
    const struct ovsdb_table_schema *ts = table->schema;
    uint16_t n_columns;
    uint16_t i;

    r.data = data;
    r.size = len;
    r.pos = 0;

    /* We already parsed uuid, total_len, n_columns, flags from the
     * outer caller.  But the body starts with table name that we
     * need to skip here. */

    /* Read n_columns and flags that the caller already consumed
     * from the header -- wait, the caller gives us data AFTER
     * the 24-byte header.  But the 24-byte header already had
     * n_columns and flags, so we need them passed in.  Let's
     * re-read them from the original buffer.
     *
     * Actually, let's restructure: the caller reads the full
     * record and passes us the full record bytes, plus the
     * pre-parsed uuid.  We re-parse n_columns/flags from offsets
     * 20 and 22 relative to the record start, but the caller
     * passed us the body starting at offset 0 = record start.
     */

    /* Actually the caller gives us data starting AFTER the 24-byte
     * fixed header.  So first thing in 'data' is the table name. */

    /* Skip table name. */
    if (!disk_store_reader_get_uint16(&r, &name_len)) {
        return NULL;
    }
    if (!disk_store_reader_remaining(&r, name_len)) {
        return NULL;
    }
    r.pos += name_len;

    /* We need n_columns.  The caller should pass it.  For now,
     * derive from table schema. */
    n_columns = (uint16_t)(shash_count(&ts->columns)
                           - OVSDB_N_STD_COLUMNS);

    row = ovsdb_row_create(table);
    *ovsdb_row_get_uuid_rw(row) = *uuid;

    /* Deserialize each column in schema order (same order we
     * serialized: SHASH_FOR_EACH iterates in the same order
     * for the same shash). */
    i = 0;
    {
        struct shash_node *node;

        SHASH_FOR_EACH (node, &ts->columns) {
            const struct ovsdb_column *col = node->data;
            uint8_t type_tag;

            if (col->index < OVSDB_N_STD_COLUMNS) {
                continue;
            }

            if (i >= n_columns) {
                break;
            }

            if (!disk_store_reader_get_uint8(&r, &type_tag)) {
                VLOG_WARN("truncated column type tag for "
                          "row "UUID_FMT, UUID_ARGS(uuid));
                ovsdb_row_destroy(row);
                return NULL;
            }

            /* Destroy the default datum that ovsdb_row_create
             * initialized, then deserialize from disk. */
            ovsdb_datum_destroy(&row->fields[col->index],
                                &col->type);

            if (!disk_store_deserialize_datum(
                    &r, &row->fields[col->index], &col->type)) {
                VLOG_WARN("failed to deserialize column '%s' "
                          "for row "UUID_FMT,
                          col->name, UUID_ARGS(uuid));
                /* Re-init to default so destroy is safe. */
                ovsdb_datum_init_default(
                    &row->fields[col->index], &col->type);
                ovsdb_row_destroy(row);
                return NULL;
            }

            i++;
        }
    }

    return row;
}

struct ovsdb_row *
ovsdb_disk_store_read_row(struct ovsdb_disk_store *store,
                          struct ovsdb_table *table,
                          const struct uuid *uuid)
{
    struct disk_store_index_entry *entry;
    uint8_t *record;
    ssize_t n;
    struct uuid stored_uuid;
    uint32_t total_len;
    uint16_t flags;

    entry = disk_store_find_entry(store, uuid);
    if (!entry || entry->deleted) {
        return NULL;
    }

    /* Read the full record. */
    record = xmalloc(entry->length);
    n = pread(store->fd, record, entry->length, entry->offset);
    if (n != (ssize_t) entry->length) {
        VLOG_WARN("short read for row "UUID_FMT" at offset %lld",
                  UUID_ARGS(uuid), (long long) entry->offset);
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
        size_t body_len = entry->length - DISK_STORE_ROW_HEADER_SIZE;

        row = disk_store_deserialize_row(
            record + DISK_STORE_ROW_HEADER_SIZE, body_len,
            table, uuid);
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

    /* Write header with embedded schema. */
    {
        char *schema_str = NULL;
        uint32_t schema_len = 0;

        if (store->schema) {
            struct json *sj = ovsdb_schema_to_json(store->schema);
            schema_str = json_to_string(sj, 0);
            schema_len = strlen(schema_str);
            json_destroy(sj);
        } else if (store->schema_json_len > 0) {
            /* Re-read embedded schema from original file. */
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

    /* Copy all non-deleted records. */
    hmap_init(&new_index);

    HMAP_FOR_EACH (e, hmap_node, &store->index) {
        uint8_t *record;
        ssize_t n;
        off_t new_offset;
        struct disk_store_index_entry *new_entry;

        if (e->deleted) {
            continue;
        }

        record = xmalloc(e->length);
        n = pread(store->fd, record, e->length, e->offset);
        if (n != (ssize_t) e->length) {
            free(record);
            VLOG_WARN("short read during compact for "UUID_FMT,
                      UUID_ARGS(&e->uuid));
            continue;
        }

        new_offset = lseek(tmp_fd, 0, SEEK_END);
        if (new_offset < 0) {
            free(record);
            error = ovsdb_io_error(errno,
                                   "lseek failed during compact");
            goto compact_error;
        }

        n = write(tmp_fd, record, e->length);
        free(record);
        if (n != (ssize_t) e->length) {
            error = ovsdb_io_error(errno,
                                   "write failed during compact");
            goto compact_error;
        }

        new_entry = xzalloc(sizeof *new_entry);
        new_entry->uuid = e->uuid;
        new_entry->offset = new_offset;
        new_entry->length = e->length;
        new_entry->table_name = xstrdup(e->table_name);
        new_entry->deleted = false;
        hmap_insert(&new_index, &new_entry->hmap_node,
                    disk_store_uuid_hash(&new_entry->uuid));
    }

    /* Atomic rename. */
    if (rename(tmp_filename, store->filename) < 0) {
        error = ovsdb_io_error(errno,
                               "rename '%s' -> '%s' failed",
                               tmp_filename, store->filename);
        goto compact_error;
    }

    /* Swap file descriptor and index. */
    close(store->fd);
    store->fd = tmp_fd;

    HMAP_FOR_EACH_SAFE (e, hmap_node, &store->index) {
        hmap_remove(&store->index, &e->hmap_node);
        disk_store_index_entry_destroy(e);
    }
    hmap_destroy(&store->index);
    store->index = new_index;

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
