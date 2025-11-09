# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
# cython: infer_types=True
# cython: nonecheck=False

import os
import struct
from utils.logger import AppLogger
from utils.enums import MessageType

# Cython constants
cdef public dict TYPE_MAP = {
    'a': '32h', 'b': 'b', 'B': 'B', 'h': 'h', 'H': 'H', 'i': 'i', 'I': 'I',
    'f': 'f', 'd': 'd', 'n': '4s', 'N': '16s', 'Z': '64s', 'c': 'h', 'C': 'H',
    'e': 'i', 'E': 'I', 'L': 'i', 'M': 'B', 'q': 'q', 'Q': 'Q',
}

cdef public frozenset SCALE_100 = frozenset({'c', 'C', 'e', 'E'})
cdef public frozenset STRING = frozenset({'n', 'N', 'Z'})
cdef public frozenset ROUND = frozenset({
    'Lat', 'Lng', 'TLat', 'TLng', 'Pitch', 'IPE', 'Yaw', 'IPN', 'IYAW',
    'DesPitch', 'NavPitch', 'Temp', 'AltE', 'VDop', 'VAcc', 'Roll',
    'HAGL', 'SM', 'VWN', 'VWE', 'IVT', 'SAcc', 'TAW', 'IPD', 'ErrRP',
    'SVT', 'SP', 'TAT', 'GZ', 'HDop', 'NavRoll', 'NavBrg', 'TAsp',
    'HAcc', 'DesRoll', 'SH', 'TBrg', 'AX'
})

cdef class Reader:
    """Binary message reader for MAVLink format files."""

    cdef public object logger
    cdef public dict fmt_messages
    cdef public dict _structs
    cdef public dict TYPE_MAP
    cdef public frozenset SCALE_100
    cdef public frozenset STRING
    cdef public frozenset ROUND

    cdef public bytes HEADER
    cdef public int FMT_MSG_LENGTH

    def __init__(self) -> None:
        cdef dict fmt_messages
        cdef dict _structs
        self.fmt_messages = {}
        self._structs = {}
        self.SCALE_100 = SCALE_100
        self.STRING = STRING
        self.ROUND = ROUND

        self.HEADER = b'\xA3\x95'
        self.FMT_MSG_LENGTH = 89

    cdef str decode_msg(self, bytes data):
        """Decode null-terminated ASCII string."""
        return data.partition(b'\x00')[0].decode('ascii', errors='ignore')

    cdef dict read_fmt_massage(self, bytes data, int num):
        """Read and parse FMT message."""
        cdef int fmt_type = data[num + 3]
        cdef int fmt_length = data[num + 4]


        cdef str fmt_name = self.decode_msg(data[num + 5:num + 9])
        cdef str fmt_format = self.decode_msg(data[num + 9:num + 25])
        cdef str fmt_cols = self.decode_msg(data[num + 25:num + 89])

        cdef dict msg_config = {
            "mavpackettype": "FMT",
            "Name": fmt_name,
            "Length": fmt_length,
            "Format": fmt_format,
            "Columns": fmt_cols,
            "Type": fmt_type,
            "cols": fmt_cols.split(",")
        }

        self.fmt_messages[fmt_type] = msg_config

        # Pre-compile struct once
        fmt_str = '<' + ''.join(TYPE_MAP[t] for t in fmt_format)
        self._structs[fmt_type] = struct.Struct(fmt_str)

        return msg_config

    cdef compile_all_structs(self):
        """Compile struct formats for all known message types."""
        cdef int type_msg
        cdef dict msg_config
        for type_msg, msg_config in self.fmt_messages.items():
            if type_msg not in self._structs:
                fmt_str = '<' + ''.join(TYPE_MAP[t] for t in msg_config["Format"])
                self._structs[type_msg] = struct.Struct(fmt_str)

    cdef bint is_new_message(self, data, int pos):
        """Check if valid message header exists at position."""
        if data[pos] != 0xA3 or data[pos + 1] != 0x95:
            return False
        cdef int type_msg = data[pos + 2]
        return type_msg == 0x80 or type_msg in self.fmt_messages

    def read_messages(self, bytes data, bint to_round,
                      message_type_to_read: MessageType = MessageType.ALL_MESSAGES,
                      dict fmt_messages=None, str wanted_type = ""):
        messages = self.process_messages(data, to_round, message_type_to_read, fmt_messages, wanted_type)
        for msg in messages:
            yield msg

    cdef list process_messages(self, bytes data, bint to_round,
                      message_type_to_read: MessageType = MessageType.ALL_MESSAGES,
                      dict fmt_messages=None, str wanted_type = ""):
        """Yield messages from binary data."""
        cdef int pos = 0
        cdef int data_len = len(data)
        cdef int type_msg
        cdef int next_head
        cdef dict msg_config


        if fmt_messages is not None:
            self.fmt_messages = fmt_messages

        if not self._structs:
            self.compile_all_structs()

        cdef bint read_fmt = message_type_to_read in {MessageType.ALL_MESSAGES, MessageType.FMT_MESSAGE}
        cdef bint read_data = message_type_to_read in {MessageType.ALL_MESSAGES, MessageType.DATA_MESSAGE}
        cdef bint filter_type = bool(wanted_type)

        cdef list messages = []

        while pos < data_len:
            if not self.is_new_message(data, pos):
                next_head = bytes(data[pos:]).find(self.HEADER)
                if next_head == -1:
                    break
                pos += next_head
                continue

            type_msg = data[pos + 2]

            if type_msg == 0x80:  # FMT message
                if read_fmt and (not filter_type or wanted_type == "FMT"):
                    msg = self.read_fmt_massage(data, pos)
                    messages.append(msg)

                pos += self.FMT_MSG_LENGTH
            else:  # Data message
                msg_config = self.fmt_messages[type_msg]
                if read_data and (not filter_type or wanted_type == msg_config["Name"]):
                    msg = self._parse_data_msg(data, msg_config, pos + 3, to_round)
                    messages.append(msg)
                pos += msg_config["Length"]
        return messages

    cdef dict _parse_data_msg(self, bytes payload, dict msg_config, int offset, bint to_round):
        """Parse data message efficiently with Cython optimization."""
        cdef str format_msg = msg_config["Format"]
        cdef list cols = msg_config["cols"]
        cdef int type_msg = msg_config["Type"]
        cdef str name = msg_config["Name"]

        # Unpack values
        cdef tuple values = self._structs[type_msg].unpack_from(payload, offset)
        cdef dict result = {"mavpackettype": name}

        # Local references for speed
        cdef int i
        cdef int format_len = len(format_msg)

        cdef char t
        cdef str col
        cdef object val

        cdef frozenset scale_100 = self.SCALE_100
        cdef frozenset string_cols = self.STRING
        cdef frozenset round_cols = self.ROUND

        for i in range(format_len):
            t = format_msg[i]
            val = values[i]
            col = cols[i]

            if t in SCALE_100:
                val *= 0.01
                if to_round and col in ROUND:
                    val = round(val, 7)
            elif t in STRING and col != "Data":
                val = val.partition(b'\x00')[0].decode('ascii', errors='ignore')
            elif t == 'L':
                val *= 1e-7
                if to_round and col in ROUND:
                    val = round(val, 7)

            result[col] = val

        return result
