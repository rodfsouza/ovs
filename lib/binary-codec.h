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

#ifndef OVSDB_BINARY_CODEC_H
#define OVSDB_BINARY_CODEC_H 1

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "ovsdb-types.h"
#include "openvswitch/uuid.h"

union ovsdb_atom;
struct ovsdb_datum;
struct ovsdb_type;
struct ovsdb_column_set;
struct ovsdb_table_schema;

/* ------------------------------------------------------------------ */
/* Serialization buffer.                                               */
/* ------------------------------------------------------------------ */

struct ovsdb_binary_buf {
    uint8_t *data;
    size_t size;       /* Bytes written. */
    size_t allocated;
};

void ovsdb_binary_buf_init(struct ovsdb_binary_buf *);
void ovsdb_binary_buf_destroy(struct ovsdb_binary_buf *);
void ovsdb_binary_buf_clear(struct ovsdb_binary_buf *);
void ovsdb_binary_buf_put(struct ovsdb_binary_buf *, const void *, size_t);
void ovsdb_binary_buf_put_uint8(struct ovsdb_binary_buf *, uint8_t);
void ovsdb_binary_buf_put_uint16(struct ovsdb_binary_buf *, uint16_t,
                                  bool nbo);
void ovsdb_binary_buf_put_uint32(struct ovsdb_binary_buf *, uint32_t,
                                  bool nbo);
void ovsdb_binary_buf_put_int64(struct ovsdb_binary_buf *, int64_t,
                                 bool nbo);
void ovsdb_binary_buf_put_double(struct ovsdb_binary_buf *, double,
                                  bool nbo);
void ovsdb_binary_buf_put_uuid(struct ovsdb_binary_buf *,
                                const struct uuid *);
void ovsdb_binary_buf_put_string(struct ovsdb_binary_buf *,
                                  const char *, bool nbo);

/* ------------------------------------------------------------------ */
/* Deserialization reader.                                             */
/* ------------------------------------------------------------------ */

struct ovsdb_binary_reader {
    const uint8_t *data;
    size_t size;
    size_t pos;
};

void ovsdb_binary_reader_init(struct ovsdb_binary_reader *,
                               const uint8_t *, size_t);
bool ovsdb_binary_reader_remaining(const struct ovsdb_binary_reader *,
                                    size_t);
bool ovsdb_binary_reader_get(struct ovsdb_binary_reader *, void *, size_t);
bool ovsdb_binary_reader_get_uint8(struct ovsdb_binary_reader *, uint8_t *);
bool ovsdb_binary_reader_get_uint16(struct ovsdb_binary_reader *,
                                     uint16_t *, bool nbo);
bool ovsdb_binary_reader_get_uint32(struct ovsdb_binary_reader *,
                                     uint32_t *, bool nbo);
bool ovsdb_binary_reader_get_int64(struct ovsdb_binary_reader *,
                                    int64_t *, bool nbo);
bool ovsdb_binary_reader_get_double(struct ovsdb_binary_reader *,
                                     double *, bool nbo);
bool ovsdb_binary_reader_get_uuid(struct ovsdb_binary_reader *,
                                   struct uuid *);
bool ovsdb_binary_reader_get_string(struct ovsdb_binary_reader *,
                                     char **, bool nbo);

/* ------------------------------------------------------------------ */
/* Atom codec.                                                         */
/*                                                                     */
/* When 'nbo' (network byte order) is true, multi-byte integers and    */
/* floats are stored in big-endian format suitable for network          */
/* transport.  When false, native byte order is used (disk format).    */
/* ------------------------------------------------------------------ */

void ovsdb_binary_serialize_atom(struct ovsdb_binary_buf *,
                                  const union ovsdb_atom *,
                                  enum ovsdb_atomic_type,
                                  bool nbo);
bool ovsdb_binary_deserialize_atom(struct ovsdb_binary_reader *,
                                    union ovsdb_atom *,
                                    enum ovsdb_atomic_type,
                                    bool nbo);

/* ------------------------------------------------------------------ */
/* Datum codec.                                                        */
/*                                                                     */
/* Format:                                                             */
/*   4 bytes  n       (number of key/value pairs)                      */
/*   n atoms  keys    (each serialized per atom type)                  */
/*   n atoms  values  (only if value_type != VOID)                     */
/* ------------------------------------------------------------------ */

void ovsdb_binary_serialize_datum(struct ovsdb_binary_buf *,
                                   const struct ovsdb_datum *,
                                   const struct ovsdb_type *,
                                   bool nbo);
bool ovsdb_binary_deserialize_datum(struct ovsdb_binary_reader *,
                                     struct ovsdb_datum *,
                                     const struct ovsdb_type *,
                                     bool nbo);

/* ------------------------------------------------------------------ */
/* Row-level codec for network transport.                              */
/*                                                                     */
/* Wire format per row:                                                */
/*   uuid:        16 bytes                                             */
/*   n_columns:   uint16                                               */
/*   Per column:  uint16 name_len + name + uint8 key_type              */
/*                + uint8 val_type + datum                              */
/* ------------------------------------------------------------------ */

void ovsdb_binary_serialize_row(struct ovsdb_binary_buf *,
                                const struct uuid *,
                                const struct ovsdb_datum *datums,
                                const struct ovsdb_column_set *columns,
                                bool nbo);
bool ovsdb_binary_deserialize_row(struct ovsdb_binary_reader *,
                                  struct uuid *,
                                  struct ovsdb_datum *datums,
                                  size_t n_datums,
                                  const struct ovsdb_table_schema *,
                                  bool nbo);

#endif /* ovsdb/binary-codec.h */
