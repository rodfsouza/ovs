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

#include "binary-protocol.h"

#include <arpa/inet.h>
#include <string.h>

size_t
ovsdb_binary_frame_encode(uint8_t *hdr, uint8_t msg_type,
                          uint32_t payload_len)
{
    uint32_t len_be;

    hdr[0] = OVSDB_BINARY_MAGIC;
    hdr[1] = OVSDB_BINARY_VERSION;
    hdr[2] = msg_type;
    hdr[3] = 0x00;  /* flags: reserved */

    len_be = htonl(payload_len);
    memcpy(&hdr[4], &len_be, 4);

    return OVSDB_BINARY_FRAME_HDR_SIZE;
}

bool
ovsdb_binary_frame_decode(const uint8_t *hdr,
                          uint8_t *msg_type,
                          uint32_t *payload_len)
{
    uint32_t len_be;

    if (hdr[0] != OVSDB_BINARY_MAGIC) {
        return false;
    }
    if (hdr[1] != OVSDB_BINARY_VERSION) {
        return false;
    }

    *msg_type = hdr[2];

    memcpy(&len_be, &hdr[4], 4);
    *payload_len = ntohl(len_be);

    return true;
}
