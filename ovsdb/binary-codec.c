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

#include "binary-codec.h"

#include <arpa/inet.h>
#include <string.h>

#include "openvswitch/json.h"
#include "ovsdb-data.h"
#include "openvswitch/uuid.h"
#include "util.h"

/* Sanity caps for untrusted network data.  These do not apply to
 * disk-store callers (nbo=false), only to network callers (nbo=true). */
#define BINARY_CODEC_MAX_STRING  (16 * 1024 * 1024)  /* 16 MB. */
#define BINARY_CODEC_MAX_DATUM_N (1024 * 1024)        /* 1M elements. */

/* ------------------------------------------------------------------ */
/* Byte-order helpers.                                                 */
/* ------------------------------------------------------------------ */

/* Portable 64-bit host-to-big-endian and reverse.
 * Uses two 32-bit swaps for maximum portability. */
static uint64_t
htobe64__(uint64_t v)
{
#if __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
    return v;
#else
    return ((uint64_t) htonl((uint32_t) v) << 32)
           | htonl((uint32_t) (v >> 32));
#endif
}

static uint64_t
be64toh__(uint64_t v)
{
    return htobe64__(v);  /* Self-inverse. */
}

/* ------------------------------------------------------------------ */
/* Serialization buffer.                                               */
/* ------------------------------------------------------------------ */

void
ovsdb_binary_buf_init(struct ovsdb_binary_buf *buf)
{
    buf->data = NULL;
    buf->size = 0;
    buf->allocated = 0;
}

void
ovsdb_binary_buf_destroy(struct ovsdb_binary_buf *buf)
{
    free(buf->data);
    buf->data = NULL;
    buf->size = 0;
    buf->allocated = 0;
}

void
ovsdb_binary_buf_clear(struct ovsdb_binary_buf *buf)
{
    buf->size = 0;
}

static void
ovsdb_binary_buf_ensure(struct ovsdb_binary_buf *buf, size_t extra)
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

void
ovsdb_binary_buf_put(struct ovsdb_binary_buf *buf,
                     const void *p, size_t n)
{
    ovsdb_binary_buf_ensure(buf, n);
    memcpy(buf->data + buf->size, p, n);
    buf->size += n;
}

void
ovsdb_binary_buf_put_uint8(struct ovsdb_binary_buf *buf, uint8_t v)
{
    ovsdb_binary_buf_put(buf, &v, sizeof v);
}

void
ovsdb_binary_buf_put_uint16(struct ovsdb_binary_buf *buf,
                            uint16_t v, bool nbo)
{
    if (nbo) {
        v = htons(v);
    }
    ovsdb_binary_buf_put(buf, &v, sizeof v);
}

void
ovsdb_binary_buf_put_uint32(struct ovsdb_binary_buf *buf,
                            uint32_t v, bool nbo)
{
    if (nbo) {
        v = htonl(v);
    }
    ovsdb_binary_buf_put(buf, &v, sizeof v);
}

void
ovsdb_binary_buf_put_int64(struct ovsdb_binary_buf *buf,
                           int64_t v, bool nbo)
{
    if (nbo) {
        uint64_t u;
        memcpy(&u, &v, sizeof u);
        u = htobe64__(u);
        ovsdb_binary_buf_put(buf, &u, sizeof u);
    } else {
        ovsdb_binary_buf_put(buf, &v, sizeof v);
    }
}

void
ovsdb_binary_buf_put_double(struct ovsdb_binary_buf *buf,
                            double v, bool nbo)
{
    if (nbo) {
        uint64_t u;
        memcpy(&u, &v, sizeof u);
        u = htobe64__(u);
        ovsdb_binary_buf_put(buf, &u, sizeof u);
    } else {
        ovsdb_binary_buf_put(buf, &v, sizeof v);
    }
}

void
ovsdb_binary_buf_put_uuid(struct ovsdb_binary_buf *buf,
                          const struct uuid *uuid)
{
    ovsdb_binary_buf_put(buf, uuid, sizeof *uuid);
}

void
ovsdb_binary_buf_put_string(struct ovsdb_binary_buf *buf,
                            const char *s, bool nbo)
{
    size_t slen = strlen(s);

    ovs_assert(slen <= UINT32_MAX);
    ovsdb_binary_buf_put_uint32(buf, (uint32_t) slen, nbo);
    ovsdb_binary_buf_put(buf, s, slen);
}

/* ------------------------------------------------------------------ */
/* Deserialization reader.                                             */
/* ------------------------------------------------------------------ */

void
ovsdb_binary_reader_init(struct ovsdb_binary_reader *r,
                         const uint8_t *data, size_t size)
{
    r->data = data;
    r->size = size;
    r->pos = 0;
}

bool
ovsdb_binary_reader_remaining(const struct ovsdb_binary_reader *r,
                              size_t n)
{
    return r->pos + n <= r->size;
}

bool
ovsdb_binary_reader_get(struct ovsdb_binary_reader *r,
                        void *out, size_t n)
{
    if (!ovsdb_binary_reader_remaining(r, n)) {
        return false;
    }
    memcpy(out, r->data + r->pos, n);
    r->pos += n;
    return true;
}

bool
ovsdb_binary_reader_get_uint8(struct ovsdb_binary_reader *r,
                              uint8_t *out)
{
    return ovsdb_binary_reader_get(r, out, sizeof *out);
}

bool
ovsdb_binary_reader_get_uint16(struct ovsdb_binary_reader *r,
                               uint16_t *out, bool nbo)
{
    if (!ovsdb_binary_reader_get(r, out, sizeof *out)) {
        return false;
    }
    if (nbo) {
        *out = ntohs(*out);
    }
    return true;
}

bool
ovsdb_binary_reader_get_uint32(struct ovsdb_binary_reader *r,
                               uint32_t *out, bool nbo)
{
    if (!ovsdb_binary_reader_get(r, out, sizeof *out)) {
        return false;
    }
    if (nbo) {
        *out = ntohl(*out);
    }
    return true;
}

bool
ovsdb_binary_reader_get_int64(struct ovsdb_binary_reader *r,
                              int64_t *out, bool nbo)
{
    uint64_t u;
    if (!ovsdb_binary_reader_get(r, &u, sizeof u)) {
        return false;
    }
    if (nbo) {
        u = be64toh__(u);
    }
    memcpy(out, &u, sizeof *out);
    return true;
}

bool
ovsdb_binary_reader_get_double(struct ovsdb_binary_reader *r,
                               double *out, bool nbo)
{
    uint64_t u;
    if (!ovsdb_binary_reader_get(r, &u, sizeof u)) {
        return false;
    }
    if (nbo) {
        u = be64toh__(u);
    }
    memcpy(out, &u, sizeof *out);
    return true;
}

bool
ovsdb_binary_reader_get_uuid(struct ovsdb_binary_reader *r,
                             struct uuid *out)
{
    return ovsdb_binary_reader_get(r, out, sizeof *out);
}

bool
ovsdb_binary_reader_get_string(struct ovsdb_binary_reader *r,
                               char **out, bool nbo)
{
    uint32_t len;
    if (!ovsdb_binary_reader_get_uint32(r, &len, nbo)) {
        return false;
    }
    if (nbo && len > BINARY_CODEC_MAX_STRING) {
        return false;
    }
    if (!ovsdb_binary_reader_remaining(r, len)) {
        return false;
    }
    *out = xmalloc(len + 1);
    memcpy(*out, r->data + r->pos, len);
    (*out)[len] = '\0';
    r->pos += len;
    return true;
}

/* ------------------------------------------------------------------ */
/* Atom serialization / deserialization.                                */
/* ------------------------------------------------------------------ */

void
ovsdb_binary_serialize_atom(struct ovsdb_binary_buf *buf,
                            const union ovsdb_atom *atom,
                            enum ovsdb_atomic_type type,
                            bool nbo)
{
    switch (type) {
    case OVSDB_TYPE_INTEGER:
        ovsdb_binary_buf_put_int64(buf, atom->integer, nbo);
        break;

    case OVSDB_TYPE_REAL:
        ovsdb_binary_buf_put_double(buf, atom->real, nbo);
        break;

    case OVSDB_TYPE_BOOLEAN:
        ovsdb_binary_buf_put_uint8(buf, atom->boolean ? 1 : 0);
        break;

    case OVSDB_TYPE_STRING: {
        const char *s = json_string(atom->s);
        ovsdb_binary_buf_put_string(buf, s, nbo);
        break;
    }

    case OVSDB_TYPE_UUID:
        ovsdb_binary_buf_put_uuid(buf, &atom->uuid);
        break;

    case OVSDB_TYPE_VOID:
    case OVSDB_N_TYPES:
    default:
        OVS_NOT_REACHED();
    }
}

bool
ovsdb_binary_deserialize_atom(struct ovsdb_binary_reader *r,
                              union ovsdb_atom *atom,
                              enum ovsdb_atomic_type type,
                              bool nbo)
{
    switch (type) {
    case OVSDB_TYPE_INTEGER:
        return ovsdb_binary_reader_get_int64(r, &atom->integer, nbo);

    case OVSDB_TYPE_REAL:
        return ovsdb_binary_reader_get_double(r, &atom->real, nbo);

    case OVSDB_TYPE_BOOLEAN: {
        uint8_t v;
        if (!ovsdb_binary_reader_get_uint8(r, &v)) {
            return false;
        }
        atom->boolean = (v != 0);
        return true;
    }

    case OVSDB_TYPE_STRING: {
        uint32_t len;
        if (!ovsdb_binary_reader_get_uint32(r, &len, nbo)) {
            return false;
        }
        if (!ovsdb_binary_reader_remaining(r, len)) {
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
        return ovsdb_binary_reader_get_uuid(r, &atom->uuid);

    case OVSDB_TYPE_VOID:
    case OVSDB_N_TYPES:
    default:
        return false;
    }
}

/* ------------------------------------------------------------------ */
/* Datum serialization / deserialization.                               */
/* ------------------------------------------------------------------ */

void
ovsdb_binary_serialize_datum(struct ovsdb_binary_buf *buf,
                             const struct ovsdb_datum *datum,
                             const struct ovsdb_type *type,
                             bool nbo)
{
    uint32_t n = datum->n;
    unsigned int i;

    ovsdb_binary_buf_put_uint32(buf, n, nbo);

    for (i = 0; i < n; i++) {
        ovsdb_binary_serialize_atom(buf, &datum->keys[i],
                                    type->key.type, nbo);
    }

    if (type->value.type != OVSDB_TYPE_VOID) {
        for (i = 0; i < n; i++) {
            ovsdb_binary_serialize_atom(buf, &datum->values[i],
                                        type->value.type, nbo);
        }
    }
}

bool
ovsdb_binary_deserialize_datum(struct ovsdb_binary_reader *r,
                               struct ovsdb_datum *datum,
                               const struct ovsdb_type *type,
                               bool nbo)
{
    uint32_t n;
    unsigned int i;

    if (!ovsdb_binary_reader_get_uint32(r, &n, nbo)) {
        return false;
    }
    if (nbo && n > BINARY_CODEC_MAX_DATUM_N) {
        return false;
    }

    datum->n = n;
    datum->refcnt = NULL;
    datum->keys = n ? xmalloc(n * sizeof *datum->keys) : NULL;
    datum->values = NULL;

    for (i = 0; i < n; i++) {
        if (!ovsdb_binary_deserialize_atom(r, &datum->keys[i],
                                           type->key.type, nbo)) {
            goto error_keys;
        }
    }

    if (type->value.type != OVSDB_TYPE_VOID) {
        datum->values = n ? xmalloc(n * sizeof *datum->values)
                          : NULL;
        for (i = 0; i < n; i++) {
            if (!ovsdb_binary_deserialize_atom(
                    r, &datum->values[i], type->value.type, nbo)) {
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
    i = n;  /* Fall through to free all keys. */

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
