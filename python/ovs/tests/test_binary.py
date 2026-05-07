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

"""Tests for ovs.db.binary_protocol and ovs.db.binary_codec."""

import struct
import unittest
import uuid

from ovs.db import binary_codec
from ovs.db import binary_protocol
from ovs.db import types as ovs_types


class TestBinaryProtocol(unittest.TestCase):

    def test_is_binary_magic(self):
        self.assertTrue(binary_protocol.is_binary_magic(0xDB))
        self.assertFalse(binary_protocol.is_binary_magic(0x7B))  # '{'
        self.assertFalse(binary_protocol.is_binary_magic(0x5B))  # '['
        self.assertFalse(binary_protocol.is_binary_magic(0x22))  # '"'
        self.assertFalse(binary_protocol.is_binary_magic(0x00))

    def test_frame_header_roundtrip(self):
        for msg_type in (binary_protocol.INITIAL_BEGIN,
                         binary_protocol.ROW_BATCH,
                         binary_protocol.INITIAL_END,
                         binary_protocol.UPDATE,
                         binary_protocol.UPDATE_BATCH):
            for payload_len in (0, 1, 256, 65535, 1024 * 1024):
                hdr = binary_protocol.encode_frame_header(
                    msg_type, payload_len)
                self.assertEqual(len(hdr), binary_protocol.FRAME_HDR_SIZE)
                got_type, got_len = binary_protocol.decode_frame_header(hdr)
                self.assertEqual(got_type, msg_type)
                self.assertEqual(got_len, payload_len)

    def test_decode_bad_magic(self):
        hdr = struct.pack('!BBBBI', 0x00, 0x01, 0x01, 0x00, 0)
        with self.assertRaises(ValueError):
            binary_protocol.decode_frame_header(hdr)

    def test_decode_bad_version(self):
        hdr = struct.pack('!BBBBI', 0xDB, 0xFF, 0x01, 0x00, 0)
        with self.assertRaises(ValueError):
            binary_protocol.decode_frame_header(hdr)

    def test_decode_payload_too_large(self):
        too_big = binary_protocol.MAX_PAYLOAD + 1
        hdr = struct.pack('!BBBBI', 0xDB, 0x01, 0x02, 0x00, too_big)
        with self.assertRaises(ValueError):
            binary_protocol.decode_frame_header(hdr)

    def test_decode_incomplete_header(self):
        with self.assertRaises(ValueError):
            binary_protocol.decode_frame_header(b"\xDB\x01")

    def test_constants(self):
        self.assertEqual(binary_protocol.BINARY_MAGIC, 0xDB)
        self.assertEqual(binary_protocol.BINARY_VERSION, 0x01)
        self.assertEqual(binary_protocol.FRAME_HDR_SIZE, 8)


class TestBinaryReader(unittest.TestCase):

    def test_uint8(self):
        r = binary_codec.BinaryReader(bytes([0, 42, 255]))
        self.assertEqual(r.get_uint8(), 0)
        self.assertEqual(r.get_uint8(), 42)
        self.assertEqual(r.get_uint8(), 255)

    def test_uint16(self):
        data = struct.pack('!HH', 0, 12345)
        r = binary_codec.BinaryReader(data)
        self.assertEqual(r.get_uint16(), 0)
        self.assertEqual(r.get_uint16(), 12345)

    def test_uint32(self):
        data = struct.pack('!I', 0xDEADBEEF)
        r = binary_codec.BinaryReader(data)
        self.assertEqual(r.get_uint32(), 0xDEADBEEF)

    def test_int64_positive(self):
        r = binary_codec.BinaryReader(struct.pack('!q', 2**62))
        self.assertEqual(r.get_int64(), 2**62)

    def test_int64_negative(self):
        r = binary_codec.BinaryReader(struct.pack('!q', -42))
        self.assertEqual(r.get_int64(), -42)

    def test_double(self):
        r = binary_codec.BinaryReader(struct.pack('!d', 3.14159))
        self.assertAlmostEqual(r.get_double(), 3.14159, places=5)

    def test_uuid(self):
        u = uuid.uuid4()
        r = binary_codec.BinaryReader(u.bytes)
        self.assertEqual(r.get_uuid(), u)

    def test_string(self):
        s = "hello world"
        data = struct.pack('!I', len(s)) + s.encode('utf-8')
        r = binary_codec.BinaryReader(data)
        self.assertEqual(r.get_string(), s)

    def test_empty_string(self):
        data = struct.pack('!I', 0)
        r = binary_codec.BinaryReader(data)
        self.assertEqual(r.get_string(), "")

    def test_unicode_string(self):
        s = "café ☕"
        encoded = s.encode('utf-8')
        data = struct.pack('!I', len(encoded)) + encoded
        r = binary_codec.BinaryReader(data)
        self.assertEqual(r.get_string(), s)

    def test_remaining(self):
        r = binary_codec.BinaryReader(b"\x00\x01\x02")
        self.assertEqual(r.remaining(), 3)
        r.get_uint8()
        self.assertEqual(r.remaining(), 2)

    def test_truncated_raises(self):
        r = binary_codec.BinaryReader(b"\x00")
        with self.assertRaises(ValueError):
            r.get_uint16()

    def test_empty_reader_raises(self):
        r = binary_codec.BinaryReader(b"")
        with self.assertRaises(ValueError):
            r.get_uint8()


class TestDeserializeAtom(unittest.TestCase):

    def test_integer(self):
        r = binary_codec.BinaryReader(struct.pack('!q', 999))
        self.assertEqual(
            binary_codec.deserialize_atom(r, ovs_types.IntegerType), 999)

    def test_integer_zero(self):
        r = binary_codec.BinaryReader(struct.pack('!q', 0))
        self.assertEqual(
            binary_codec.deserialize_atom(r, ovs_types.IntegerType), 0)

    def test_integer_negative(self):
        r = binary_codec.BinaryReader(struct.pack('!q', -1))
        self.assertEqual(
            binary_codec.deserialize_atom(r, ovs_types.IntegerType), -1)

    def test_real(self):
        r = binary_codec.BinaryReader(struct.pack('!d', 2.718))
        self.assertAlmostEqual(
            binary_codec.deserialize_atom(r, ovs_types.RealType), 2.718)

    def test_boolean_true(self):
        r = binary_codec.BinaryReader(bytes([1]))
        self.assertTrue(
            binary_codec.deserialize_atom(r, ovs_types.BooleanType))

    def test_boolean_false(self):
        r = binary_codec.BinaryReader(bytes([0]))
        self.assertFalse(
            binary_codec.deserialize_atom(r, ovs_types.BooleanType))

    def test_string(self):
        s = "test_string"
        data = struct.pack('!I', len(s)) + s.encode()
        r = binary_codec.BinaryReader(data)
        self.assertEqual(
            binary_codec.deserialize_atom(r, ovs_types.StringType), s)

    def test_uuid(self):
        u = uuid.uuid4()
        r = binary_codec.BinaryReader(u.bytes)
        self.assertEqual(
            binary_codec.deserialize_atom(r, ovs_types.UuidType), u)


class TestDeserializeDatumToJson(unittest.TestCase):

    def test_scalar_integer(self):
        data = struct.pack('!Iq', 1, 42)
        r = binary_codec.BinaryReader(data)
        result = binary_codec.deserialize_datum_to_json(
            r, ovs_types.IntegerType, ovs_types.VoidType)
        self.assertEqual(result, 42)

    def test_scalar_string(self):
        s = "hello"
        data = struct.pack('!I', 1) + struct.pack('!I', len(s)) + s.encode()
        r = binary_codec.BinaryReader(data)
        result = binary_codec.deserialize_datum_to_json(
            r, ovs_types.StringType, ovs_types.VoidType)
        self.assertEqual(result, "hello")

    def test_scalar_boolean(self):
        data = struct.pack('!I', 1) + bytes([1])
        r = binary_codec.BinaryReader(data)
        result = binary_codec.deserialize_datum_to_json(
            r, ovs_types.BooleanType, ovs_types.VoidType)
        self.assertTrue(result)

    def test_scalar_uuid(self):
        u = uuid.uuid4()
        data = struct.pack('!I', 1) + u.bytes
        r = binary_codec.BinaryReader(data)
        result = binary_codec.deserialize_datum_to_json(
            r, ovs_types.UuidType, ovs_types.VoidType)
        self.assertEqual(result, ["uuid", str(u)])

    def test_set_of_strings(self):
        items = ["a", "bb", "ccc"]
        buf = struct.pack('!I', 3)
        for s in items:
            buf += struct.pack('!I', len(s)) + s.encode()
        r = binary_codec.BinaryReader(buf)
        result = binary_codec.deserialize_datum_to_json(
            r, ovs_types.StringType, ovs_types.VoidType)
        self.assertEqual(result, ["set", ["a", "bb", "ccc"]])

    def test_set_of_integers(self):
        buf = struct.pack('!I', 2)
        buf += struct.pack('!q', 10) + struct.pack('!q', 20)
        r = binary_codec.BinaryReader(buf)
        result = binary_codec.deserialize_datum_to_json(
            r, ovs_types.IntegerType, ovs_types.VoidType)
        self.assertEqual(result, ["set", [10, 20]])

    def test_empty_set(self):
        r = binary_codec.BinaryReader(struct.pack('!I', 0))
        result = binary_codec.deserialize_datum_to_json(
            r, ovs_types.StringType, ovs_types.VoidType)
        self.assertEqual(result, ["set", []])

    def test_map_string_to_string(self):
        buf = struct.pack('!I', 2)
        for s in ["k1", "k2"]:
            buf += struct.pack('!I', len(s)) + s.encode()
        for s in ["v1", "v2"]:
            buf += struct.pack('!I', len(s)) + s.encode()
        r = binary_codec.BinaryReader(buf)
        result = binary_codec.deserialize_datum_to_json(
            r, ovs_types.StringType, ovs_types.StringType)
        self.assertEqual(result, ["map", [["k1", "v1"], ["k2", "v2"]]])

    def test_map_string_to_integer(self):
        buf = struct.pack('!I', 1)
        buf += struct.pack('!I', 3) + b"key"
        buf += struct.pack('!q', 99)
        r = binary_codec.BinaryReader(buf)
        result = binary_codec.deserialize_datum_to_json(
            r, ovs_types.StringType, ovs_types.IntegerType)
        self.assertEqual(result, ["map", [["key", 99]]])

    def test_empty_map(self):
        r = binary_codec.BinaryReader(struct.pack('!I', 0))
        result = binary_codec.deserialize_datum_to_json(
            r, ovs_types.StringType, ovs_types.StringType)
        self.assertEqual(result, ["map", []])

    def test_map_uuid_to_integer(self):
        u = uuid.uuid4()
        buf = struct.pack('!I', 1) + u.bytes + struct.pack('!q', 7)
        r = binary_codec.BinaryReader(buf)
        result = binary_codec.deserialize_datum_to_json(
            r, ovs_types.UuidType, ovs_types.IntegerType)
        self.assertEqual(result, ["map", [[["uuid", str(u)], 7]]])


class TestConnectionBinaryDemux(unittest.TestCase):
    """Test the _demux_binary method on Connection."""

    def _make_conn(self):
        """Create a Connection with a mock stream."""
        from unittest import mock
        stream = mock.Mock()
        stream.name = "test"
        from ovs.jsonrpc import Connection
        return Connection(stream)

    def test_pure_json_passthrough(self):
        conn = self._make_conn()
        data = b'{"method":"echo","params":[]}'
        result = conn._demux_binary(data)
        self.assertEqual(result, data)
        self.assertEqual(len(conn._binary_queue), 0)

    def test_single_binary_frame(self):
        conn = self._make_conn()
        payload = b"test_payload"
        hdr = binary_protocol.encode_frame_header(
            binary_protocol.INITIAL_BEGIN, len(payload))
        data = hdr + payload
        result = conn._demux_binary(data)
        self.assertEqual(result, b"")
        self.assertEqual(len(conn._binary_queue), 1)
        msg_type, got_payload = conn._binary_queue[0]
        self.assertEqual(msg_type, binary_protocol.INITIAL_BEGIN)
        self.assertEqual(got_payload, payload)

    def test_empty_payload_frame(self):
        conn = self._make_conn()
        hdr = binary_protocol.encode_frame_header(
            binary_protocol.INITIAL_END, 0)
        result = conn._demux_binary(hdr)
        self.assertEqual(result, b"")
        self.assertEqual(len(conn._binary_queue), 1)
        msg_type, payload = conn._binary_queue[0]
        self.assertEqual(msg_type, binary_protocol.INITIAL_END)
        self.assertEqual(payload, b"")

    def test_partial_header(self):
        conn = self._make_conn()
        hdr = binary_protocol.encode_frame_header(
            binary_protocol.ROW_BATCH, 4)
        # Send only first 3 bytes of header.
        result = conn._demux_binary(hdr[:3])
        self.assertEqual(result, b"")
        self.assertEqual(len(conn._binary_queue), 0)
        self.assertEqual(conn._binary_state, 1)  # HEADER
        # Send rest of header + payload.
        result = conn._demux_binary(hdr[3:] + b"\x00\x01\x02\x03")
        self.assertEqual(result, b"")
        self.assertEqual(len(conn._binary_queue), 1)
        self.assertEqual(conn._binary_queue[0][1], b"\x00\x01\x02\x03")

    def test_binary_then_json(self):
        conn = self._make_conn()
        hdr = binary_protocol.encode_frame_header(
            binary_protocol.INITIAL_END, 0)
        json_data = b'{"id":1}'
        data = hdr + json_data
        result = conn._demux_binary(data)
        self.assertEqual(result, json_data)
        self.assertEqual(len(conn._binary_queue), 1)

    def test_json_then_binary(self):
        conn = self._make_conn()
        json_data = b'{"id":1}'
        payload = b"data"
        hdr = binary_protocol.encode_frame_header(
            binary_protocol.ROW_BATCH, len(payload))
        data = json_data + hdr + payload
        result = conn._demux_binary(data)
        self.assertEqual(result, json_data)
        self.assertEqual(len(conn._binary_queue), 1)
        self.assertEqual(list(conn._binary_queue)[0][1], payload)


class TestDeserializeRowBatch(unittest.TestCase):
    """Tests for deserialize_row_batch with mock table schemas."""

    def _make_table_schemas(self):
        """Create a mock table_schemas dict mimicking IdlTable structure."""
        from unittest import mock

        # Simulate a table with columns "name" (string) and "tag" (integer)
        columns = {
            "name": mock.Mock(),
            "tag": mock.Mock(),
        }
        table = mock.Mock()
        table.columns = columns
        return {"Logical_Switch": table}

    def _build_row_batch_payload(self, table_name, rows):
        """Build a ROW_BATCH payload from (uuid, columns) pairs.

        Each column is (col_name, key_type_code, val_type_code, datum_bytes).
        """
        buf = b""
        # table_name as string (uint32 len + utf8)
        encoded_name = table_name.encode('utf-8')
        buf += struct.pack('!I', len(encoded_name)) + encoded_name
        # n_rows as uint16
        buf += struct.pack('!H', len(rows))

        for row_uuid, columns in rows:
            # uuid: 16 bytes
            buf += row_uuid.bytes
            # n_columns: uint16
            buf += struct.pack('!H', len(columns))
            for col_name, key_code, val_code, datum_bytes in columns:
                encoded_col = col_name.encode('utf-8')
                buf += struct.pack('!I', len(encoded_col)) + encoded_col
                buf += struct.pack('!BB', key_code, val_code)
                buf += datum_bytes
        return buf

    def test_single_row_two_columns(self):
        schemas = self._make_table_schemas()
        row_uuid = uuid.uuid4()

        # "name" column: scalar string "sw0"
        name_datum = struct.pack('!I', 1)  # n=1
        name_datum += struct.pack('!I', 3) + b"sw0"

        # "tag" column: scalar integer 42
        tag_datum = struct.pack('!Iq', 1, 42)

        payload = self._build_row_batch_payload("Logical_Switch", [
            (row_uuid, [
                ("name", binary_codec.STRING_CODE, binary_codec.VOID_CODE,
                 name_datum),
                ("tag", binary_codec.INTEGER_CODE, binary_codec.VOID_CODE,
                 tag_datum),
            ])
        ])

        reader = binary_codec.BinaryReader(payload)
        result = binary_codec.deserialize_row_batch(reader, schemas)

        self.assertIn("Logical_Switch", result)
        row_updates = result["Logical_Switch"]
        self.assertIn(str(row_uuid), row_updates)
        row = row_updates[str(row_uuid)]
        self.assertIn("insert", row)
        self.assertEqual(row["insert"]["name"], "sw0")
        self.assertEqual(row["insert"]["tag"], 42)

    def test_unknown_column_filtered_out(self):
        schemas = self._make_table_schemas()
        row_uuid = uuid.uuid4()

        # "name" column (known) + "unknown_col" (not in schema)
        name_datum = struct.pack('!I', 1) + struct.pack('!I', 2) + b"s1"
        unknown_datum = struct.pack('!I', 1) + struct.pack('!I', 1) + b"x"

        payload = self._build_row_batch_payload("Logical_Switch", [
            (row_uuid, [
                ("name", binary_codec.STRING_CODE, binary_codec.VOID_CODE,
                 name_datum),
                ("unknown_col", binary_codec.STRING_CODE,
                 binary_codec.VOID_CODE, unknown_datum),
            ])
        ])

        reader = binary_codec.BinaryReader(payload)
        result = binary_codec.deserialize_row_batch(reader, schemas)

        row = result["Logical_Switch"][str(row_uuid)]["insert"]
        self.assertIn("name", row)
        self.assertNotIn("unknown_col", row)

    def test_multiple_rows(self):
        schemas = self._make_table_schemas()
        u1, u2 = uuid.uuid4(), uuid.uuid4()

        def make_name_datum(s):
            return struct.pack('!I', 1) + struct.pack('!I', len(s)) + s.encode()

        payload = self._build_row_batch_payload("Logical_Switch", [
            (u1, [("name", binary_codec.STRING_CODE,
                   binary_codec.VOID_CODE, make_name_datum("sw1"))]),
            (u2, [("name", binary_codec.STRING_CODE,
                   binary_codec.VOID_CODE, make_name_datum("sw2"))]),
        ])

        reader = binary_codec.BinaryReader(payload)
        result = binary_codec.deserialize_row_batch(reader, schemas)

        self.assertEqual(len(result["Logical_Switch"]), 2)
        self.assertEqual(
            result["Logical_Switch"][str(u1)]["insert"]["name"], "sw1")
        self.assertEqual(
            result["Logical_Switch"][str(u2)]["insert"]["name"], "sw2")

    def test_unknown_table_still_parses(self):
        # Empty schema — table not monitored
        schemas = {}
        row_uuid = uuid.uuid4()
        name_datum = struct.pack('!I', 1) + struct.pack('!I', 2) + b"sw"

        payload = self._build_row_batch_payload("Unknown_Table", [
            (row_uuid, [
                ("name", binary_codec.STRING_CODE, binary_codec.VOID_CODE,
                 name_datum),
            ])
        ])

        reader = binary_codec.BinaryReader(payload)
        result = binary_codec.deserialize_row_batch(reader, schemas)

        # Row is present but all columns filtered (table not in schemas)
        self.assertIn("Unknown_Table", result)
        row = result["Unknown_Table"][str(row_uuid)]["insert"]
        self.assertEqual(row, {})

    def test_map_column(self):
        from unittest import mock
        table = mock.Mock()
        table.columns = {"external_ids": mock.Mock()}
        schemas = {"Logical_Switch": table}

        row_uuid = uuid.uuid4()
        # map<string, string> with 2 entries: k1→v1, k2→v2
        datum = struct.pack('!I', 2)
        for s in ["k1", "k2"]:
            datum += struct.pack('!I', len(s)) + s.encode()
        for s in ["v1", "v2"]:
            datum += struct.pack('!I', len(s)) + s.encode()

        payload = self._build_row_batch_payload("Logical_Switch", [
            (row_uuid, [
                ("external_ids", binary_codec.STRING_CODE,
                 binary_codec.STRING_CODE, datum),
            ])
        ])

        reader = binary_codec.BinaryReader(payload)
        result = binary_codec.deserialize_row_batch(reader, schemas)

        row = result["Logical_Switch"][str(row_uuid)]["insert"]
        self.assertEqual(
            row["external_ids"],
            ["map", [["k1", "v1"], ["k2", "v2"]]])


if __name__ == '__main__':
    unittest.main()
