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

#ifndef BINARY_PROTOCOL_H
#define BINARY_PROTOCOL_H 1

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/* Binary frame magic byte.  Chosen to never conflict with the first
 * byte of a valid JSON value (which starts with '{', '[', '"', digit,
 * 't', 'f', or 'n' — all < 0x80). */
#define OVSDB_BINARY_MAGIC  0xDB
#define OVSDB_BINARY_VERSION 0x01

/* Binary frame header:
 *   [0]    magic   (0xDB)
 *   [1]    version (0x01)
 *   [2]    msg_type
 *   [3]    flags   (reserved, 0x00)
 *   [4..7] payload_len (uint32_t, network byte order)
 *
 * Total: 8 bytes, followed by payload_len bytes of payload. */
#define OVSDB_BINARY_FRAME_HDR_SIZE 8
#define OVSDB_BINARY_MAX_PAYLOAD (64 * 1024 * 1024)  /* 64 MB. */

/* Message types. */
enum ovsdb_binary_msg_type {
    /* Initial snapshot streaming (server → client). */
    OVSDB_BIN_INITIAL_BEGIN  = 0x01,
    OVSDB_BIN_ROW_BATCH      = 0x02,
    OVSDB_BIN_INITIAL_END    = 0x03,

    /* Incremental updates (server → client). */
    OVSDB_BIN_UPDATE         = 0x04,
    OVSDB_BIN_UPDATE_BATCH   = 0x05,
};

/* Update sub-types within OVSDB_BIN_UPDATE payloads. */
enum ovsdb_binary_update_type {
    OVSDB_BIN_ROW_INSERT = 0,
    OVSDB_BIN_ROW_MODIFY = 1,
    OVSDB_BIN_ROW_DELETE = 2,
};

/* Returns true if 'byte' could be the start of a binary frame. */
static inline bool
ovsdb_binary_is_magic(uint8_t byte)
{
    return byte == OVSDB_BINARY_MAGIC;
}

/* Build a binary frame header into 'hdr' (must be >= 8 bytes).
 * Returns OVSDB_BINARY_FRAME_HDR_SIZE. */
size_t ovsdb_binary_frame_encode(uint8_t *hdr, uint8_t msg_type,
                                 uint32_t payload_len);

/* Parse a binary frame header.  Returns true on success.
 * On success, sets *msg_type and *payload_len. */
bool ovsdb_binary_frame_decode(const uint8_t *hdr,
                               uint8_t *msg_type,
                               uint32_t *payload_len);

#endif /* lib/binary-protocol.h */
