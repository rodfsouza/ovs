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

"""Binary codec for OVSDB atoms, datums, and rows.

Ported from lib/binary-codec.c.  All multi-byte integers use network
byte order (big-endian) for wire transport.

Atom wire format:
  integer  : 8 bytes  int64  NBO
  real     : 8 bytes  float64 NBO
  boolean  : 1 byte   uint8 (0 or 1)
  string   : 4 bytes  uint32 NBO length + raw UTF-8 bytes
  uuid     : 16 bytes raw

Datum wire format:
  4 bytes   n (uint32 NBO, number of key/value pairs)
  n atoms   keys   (each serialized per atom type)
  n atoms   values (only if value_type is not VOID)

Row wire format (within ROW_BATCH):
  16 bytes  uuid
  2 bytes   n_columns (uint16 NBO)
  Per column:
    4+N bytes  col_name   (uint32 NBO len + UTF-8)
    1 byte     key_type   (C enum: VOID=0 INT=1 REAL=2 BOOL=3 STR=4 UUID=5)
    1 byte     val_type   (VOID for sets/scalars)
    variable   datum
"""

import struct
import uuid as _uuid_mod

import ovs.db.types as ovs_types

# Map C enum values (ovsdb-types.h) to Python AtomicType objects.
VOID_CODE = 0
INTEGER_CODE = 1
REAL_CODE = 2
BOOLEAN_CODE = 3
STRING_CODE = 4
UUID_CODE = 5

_TYPE_BY_CODE = {
    VOID_CODE: ovs_types.VoidType,
    INTEGER_CODE: ovs_types.IntegerType,
    REAL_CODE: ovs_types.RealType,
    BOOLEAN_CODE: ovs_types.BooleanType,
    STRING_CODE: ovs_types.StringType,
    UUID_CODE: ovs_types.UuidType,
}

# Sanity caps for untrusted network data.
MAX_STRING_LEN = 16 * 1024 * 1024   # 16 MB
MAX_DATUM_N = 1 * 1024 * 1024       # 1 M elements

# Pre-compiled struct formats for hot-path deserialization.
_FMT_U8 = struct.Struct('!B')
_FMT_U16 = struct.Struct('!H')
_FMT_U32 = struct.Struct('!I')
_FMT_I64 = struct.Struct('!q')
_FMT_F64 = struct.Struct('!d')


class BinaryReader:
    """Read buffer for deserializing binary-encoded OVSDB data."""

    __slots__ = ('data', 'pos', 'end')

    def __init__(self, data):
        if isinstance(data, memoryview):
            self.data = bytes(data)
        elif isinstance(data, (bytes, bytearray)):
            self.data = data
        else:
            self.data = bytes(data)
        self.pos = 0
        self.end = len(self.data)

    def remaining(self):
        return self.end - self.pos

    def _check(self, n):
        if self.pos + n > self.end:
            raise ValueError(
                "truncated binary data: need %d bytes, have %d"
                % (n, self.remaining()))

    def get_bytes(self, n):
        self._check(n)
        v = self.data[self.pos:self.pos + n]
        self.pos += n
        return v

    def get_uint8(self):
        self._check(1)
        v = self.data[self.pos]
        self.pos += 1
        return v

    def get_uint16(self):
        self._check(2)
        v, = _FMT_U16.unpack_from(self.data, self.pos)
        self.pos += 2
        return v

    def get_uint32(self):
        self._check(4)
        v, = _FMT_U32.unpack_from(self.data, self.pos)
        self.pos += 4
        return v

    def get_int64(self):
        self._check(8)
        v, = _FMT_I64.unpack_from(self.data, self.pos)
        self.pos += 8
        return v

    def get_double(self):
        self._check(8)
        v, = _FMT_F64.unpack_from(self.data, self.pos)
        self.pos += 8
        return v

    def get_uuid(self):
        self._check(16)
        v = _uuid_mod.UUID(bytes=self.data[self.pos:self.pos + 16])
        self.pos += 16
        return v

    def get_string(self):
        """Read a string with uint32 length prefix."""
        slen = self.get_uint32()
        if slen > MAX_STRING_LEN:
            raise ValueError("string too long: %d bytes" % slen)
        self._check(slen)
        s = self.data[self.pos:self.pos + slen].decode('utf-8')
        self.pos += slen
        return s


def deserialize_atom(reader, atom_type):
    """Deserialize a single atom value from binary.

    Returns a native Python value matching what ovs.db.data.Atom expects:
      IntegerType  -> int
      RealType     -> float
      BooleanType  -> bool
      StringType   -> str
      UuidType     -> uuid.UUID
    """
    if atom_type == ovs_types.IntegerType:
        return reader.get_int64()
    elif atom_type == ovs_types.RealType:
        return reader.get_double()
    elif atom_type == ovs_types.BooleanType:
        return bool(reader.get_uint8())
    elif atom_type == ovs_types.StringType:
        return reader.get_string()
    elif atom_type == ovs_types.UuidType:
        return reader.get_uuid()
    else:
        raise ValueError("unsupported atom type: %s" % atom_type.name)


def _atom_to_json(value, atom_type):
    """Convert a deserialized atom value to JSON representation."""
    if atom_type == ovs_types.UuidType:
        return ["uuid", str(value)]
    return value


def deserialize_datum_to_json(reader, key_type, val_type):
    """Deserialize a datum from binary into JSON-equivalent form.

    Returns a value compatible with ovs.db.data.Datum.from_json():
      - scalar:  raw value (int, str, bool, float, ["uuid", "..."])
      - set:     ["set", [v1, v2, ...]]
      - map:     ["map", [[k1, v1], [k2, v2], ...]]
    """
    n = reader.get_uint32()
    if n > MAX_DATUM_N:
        raise ValueError("datum too large: %d elements" % n)

    keys = [deserialize_atom(reader, key_type) for _ in range(n)]

    is_map = (val_type != ovs_types.VoidType)
    values = None
    if is_map:
        values = [deserialize_atom(reader, val_type) for _ in range(n)]

    # Convert to JSON form that Datum.from_json() expects.
    if is_map:
        pairs = [[_atom_to_json(keys[i], key_type),
                   _atom_to_json(values[i], val_type)] for i in range(n)]
        return ["map", pairs]
    elif n == 1:
        return _atom_to_json(keys[0], key_type)
    else:
        return ["set", [_atom_to_json(k, key_type) for k in keys]]


def deserialize_row_batch(reader, table_schemas):
    """Deserialize a ROW_BATCH payload into a table-update dict.

    Args:
        reader: BinaryReader positioned at start of batch payload.
        table_schemas: dict of {table_name: IdlTable} from Idl.tables.

    Returns:
        dict of {table_name: {uuid_str: {"insert": {col: datum_json}}}}
        compatible with Idl.__do_parse_update() for OVSDB_UPDATE2.
    """
    table_name = reader.get_string()
    n_rows = reader.get_uint16()

    table = table_schemas.get(table_name)

    row_updates = {}
    for _ in range(n_rows):
        row_uuid = reader.get_uuid()
        n_columns = reader.get_uint16()

        row_data = {}
        for _ in range(n_columns):
            col_name = reader.get_string()
            key_type_code = reader.get_uint8()
            val_type_code = reader.get_uint8()

            key_type = _TYPE_BY_CODE.get(key_type_code)
            val_type = _TYPE_BY_CODE.get(val_type_code)
            if key_type is None:
                raise ValueError("unknown key type code: %d" % key_type_code)
            if val_type is None:
                raise ValueError("unknown val type code: %d" % val_type_code)

            datum_json = deserialize_datum_to_json(reader, key_type, val_type)

            # Only include columns the IDL is monitoring.
            if table is not None and col_name in table.columns:
                row_data[col_name] = datum_json

        row_updates[str(row_uuid)] = {"insert": row_data}

    return {table_name: row_updates}
