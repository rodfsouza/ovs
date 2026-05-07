# Copyright (c) 2026 Magalu Cloud.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

"""Binary frame protocol for OVSDB monitor transport.

Coexists with JSON-RPC on the same TCP stream.  The magic byte 0xDB
can never appear as the first byte of valid JSON (all JSON values
start with characters < 0x80).

Frame header (8 bytes):
  [0]    magic       0xDB
  [1]    version     0x01
  [2]    msg_type    (see constants below)
  [3]    flags       0x00 (reserved)
  [4..7] payload_len uint32 network byte order

Ported from lib/binary-protocol.h.
"""

import struct

BINARY_MAGIC = 0xDB
BINARY_VERSION = 0x01
FRAME_HDR_SIZE = 8
MAX_PAYLOAD = 64 * 1024 * 1024  # 64 MB

# Message types (server -> client).
INITIAL_BEGIN = 0x01
ROW_BATCH = 0x02
INITIAL_END = 0x03
UPDATE = 0x04
UPDATE_BATCH = 0x05

# Update sub-types within UPDATE payloads.
ROW_INSERT = 0
ROW_MODIFY = 1
ROW_DELETE = 2

_HDR_STRUCT = struct.Struct('!BBBBI')


def is_binary_magic(byte):
    """Return True if 'byte' could be the start of a binary frame."""
    return byte == BINARY_MAGIC


def encode_frame_header(msg_type, payload_len):
    """Encode an 8-byte binary frame header.

    Returns bytes of length FRAME_HDR_SIZE.
    """
    return _HDR_STRUCT.pack(BINARY_MAGIC, BINARY_VERSION,
                            msg_type, 0x00, payload_len)


def decode_frame_header(data):
    """Decode an 8-byte binary frame header.

    Returns (msg_type, payload_len).
    Raises ValueError on bad magic, unsupported version, or oversize.
    """
    if len(data) < FRAME_HDR_SIZE:
        raise ValueError("incomplete frame header: need %d bytes, got %d"
                         % (FRAME_HDR_SIZE, len(data)))
    magic, version, msg_type, _flags, payload_len = _HDR_STRUCT.unpack_from(
        data, 0)
    if magic != BINARY_MAGIC:
        raise ValueError("bad magic: 0x%02x" % magic)
    if version != BINARY_VERSION:
        raise ValueError("unsupported version: %d" % version)
    if payload_len > MAX_PAYLOAD:
        raise ValueError("payload too large: %d bytes (max %d)"
                         % (payload_len, MAX_PAYLOAD))
    return msg_type, payload_len
