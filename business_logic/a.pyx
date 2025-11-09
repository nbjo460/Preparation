# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
# cython: infer_types=True

import os
import struct
import sys
from typing import Any, Generator

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.logger import AppLogger
from utils.enums import MessageType

# Cython type declarations
cdef dict STRUCT_CACHE = {}
cdef dict TYPE_MAP = {
    'a': ('32h', 2 * 32),
    'b': ('b', 1),
    'B': ('B', 1),
    'h': ('h', 2),
    'H': ('H', 2),
    'i': ('i', 4),
    'I': ('I', 4),
    'f': ('f', 4),
    'd': ('d', 8),
    'n': ('4s', 4),
    'N': ('16s', 16),
    'Z': ('64s', 64),
    'c': ('h', 2),
    'C': ('H', 2),
    'e': ('i', 4),
    'E': ('I', 4),
    'L': ('i', 4),
    'M': ('B', 1),
    'q': ('q', 8),
    'Q': ('Q', 8),
}

cdef set SCALE_100 = {'c', 'C', 'e', 'E'}
cdef set STRING = {'n', 'N', 'Z'}
cdef set ROUND = {
    'Lat', 'Lng', 'TLat', 'TLng', 'Pitch', 'IPE', 'Yaw', 'IPN', 'IYAW',
    'DesPitch', 'NavPitch', 'Temp', 'AltE', 'VDop', 'VAcc', 'Roll',
    'HAGL', 'SM', 'VWN', 'VWE', 'IVT', 'SAcc', 'TAW', 'IPD', 'ErrRP',
    'SVT', 'SP', 'TAT', 'GZ', 'HDop', 'NavRoll', 'NavBrg', 'TAsp',
    'HAcc', 'DesRoll', 'SH', 'TBrg', 'AX'
}


class Reader:
    """Binary message reader for MAVLink format files."""

    HEADER = b'\xA3\x95'
    FMT_MSG_LENGTH = 89

    def __init__(self) -> None:
        self.logger = AppLogger(self.__class__.__name__)
        self.fmt_messages = {}

    @staticmethod
    def decode_msg(data: memoryview) -> str:
        """Decode null-terminated ASCII string."""
        return data.tobytes().partition(b'\x00')[0].decode('ascii', errors='ignore')

    def read_fmt_massage(self, data: memoryview, num: int) -> dict:
        """Read and parse FMT message."""
        cdef int fmt_type = data[num + 3]
        cdef int fmt_length = data[num + 4]

        fmt_name = self.decode_msg(data[num + 5:num + 9])
        fmt_format = self.decode_msg(data[num + 9:num + 25])
        fmt_cols = self.decode_msg(data[num + 25:num + 89]).split(",")

        msg_config = {
            "mavpackettype": "FMT",
            "Name": fmt_name,
            "Length": fmt_length,
            "Format": fmt_format,
            "Columns": ",".join(fmt_cols),
            "Type": fmt_type,
            "cols": fmt_cols
        }

        self.fmt_messages[fmt_type] = msg_config

        # Pre-compile struct once
        if fmt_type not in STRUCT_CACHE:
            fmt_string = ''.join([TYPE_MAP[t][0] for t in fmt_format])
            STRUCT_CACHE[fmt_type] = struct.Struct(f"<{fmt_string}")

        return msg_config

    def is_new_message(self, data: memoryview, int pos) -> bint:
        """Check if valid message header exists at position."""
        if data[pos] != 0xA3 or data[pos + 1] != 0x95:
            return False
        cdef int type_msg = data[pos + 2]
        return type_msg == 0x80 or type_msg in self.fmt_messages

    def read_messages(self, data: memoryview, bint to_round,
                     message_type_to_read: MessageType = MessageType.ALL_MESSAGES,
                     fmt_messages=None, str wanted_type = "") -> Generator[dict, None, None]:
        """Yield messages from binary data."""
        data = memoryview(data)
        cdef int pos = 0
        cdef int data_len = len(data)
        cdef int type_msg
        cdef int next_head

        if fmt_messages is not None:
            self.fmt_messages = fmt_messages

        cdef bint read_fmt = message_type_to_read in {MessageType.ALL_MESSAGES, MessageType.FMT_MESSAGE}
        cdef bint read_data = message_type_to_read in {MessageType.ALL_MESSAGES, MessageType.DATA_MESSAGE}
        cdef bint filter_type = bool(wanted_type)

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
                    yield self.read_fmt_massage(data, pos)
                pos += self.FMT_MSG_LENGTH
            else:  # Data message
                msg_config = self.fmt_messages[type_msg]
                if read_data and (not filter_type or wanted_type == msg_config["Name"]):
                    yield get_value_by_format_fast(
                        bytes(data[pos + 3:pos + msg_config["Length"]]),
                        msg_config["Format"],
                        msg_config["Columns"],
                        msg_config["Type"],
                        msg_config["Name"],
                        to_round
                    )
                pos += msg_config["Length"]


# Cython optimized function - cpdef makes it accessible from Python too
cpdef dict get_value_by_format_fast(bytes payload, str types, str cols, int type_msg, str name, bint to_round):
    """Parse data message with Cython optimization."""
    # Get or create struct
    if type_msg not in STRUCT_CACHE:
        fmt_string = ''.join([TYPE_MAP[t][0] for t in types])
        STRUCT_CACHE[type_msg] = struct.Struct(f"<{fmt_string}")

    cdef object struct_fmt = STRUCT_CACHE[type_msg]
    cdef tuple values = struct_fmt.unpack_from(payload)
    cdef list cols_list = cols.split(",")

    cdef dict result = {"mavpackettype": name}
    cdef int i
    cdef object val
    cdef str t, col

    for i in range(len(cols_list)):
        t = types[i]
        val = values[i]
        col = cols_list[i]

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