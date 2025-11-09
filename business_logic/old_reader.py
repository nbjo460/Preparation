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


class Reader:
    """Binary message reader for MAVLink format files."""

    HEADER = b'\xA3\x95'
    FMT_MSG_LENGTH = 89

    TYPE_MAP = {
        'a': '32h', 'b': 'b', 'B': 'B', 'h': 'h', 'H': 'H', 'i': 'i', 'I': 'I',
        'f': 'f', 'd': 'd', 'n': '4s', 'N': '16s', 'Z': '64s', 'c': 'h', 'C': 'H',
        'e': 'i', 'E': 'I', 'L': 'i', 'M': 'B', 'q': 'q', 'Q': 'Q',
    }

    SCALE_100 = frozenset({'c', 'C', 'e', 'E'})
    STRING = frozenset({'n', 'N', 'Z'})
    ROUND = frozenset({
        'Lat', 'Lng', 'TLat', 'TLng', 'Pitch', 'IPE', 'Yaw', 'IPN', 'IYAW',
        'DesPitch', 'NavPitch', 'Temp', 'AltE', 'VDop', 'VAcc', 'Roll',
        'HAGL', 'SM', 'VWN', 'VWE', 'IVT', 'SAcc', 'TAW', 'IPD', 'ErrRP',
        'SVT', 'SP', 'TAT', 'GZ', 'HDop', 'NavRoll', 'NavBrg', 'TAsp',
        'HAcc', 'DesRoll', 'SH', 'TBrg', 'AX'
    })

    __slots__ = ('logger', 'fmt_messages', '_structs')

    def __init__(self) -> None:
        self.logger = AppLogger(self.__class__.__name__)
        self.fmt_messages = {}
        self._structs = {}

    @staticmethod
    def decode_msg(data: memoryview) -> str:
        """Decode null-terminated ASCII string."""
        return data.tobytes().partition(b'\x00')[0].decode('ascii', errors='ignore')

    def read_fmt_massage(self, data: memoryview, num: int) -> dict:
        """Read and parse FMT message."""
        fmt_type = data[num + 3]
        fmt_length = data[num + 4]
        fmt_name = self.decode_msg(data[num + 5:num + 9])
        fmt_format = self.decode_msg(data[num + 9:num + 25])
        fmt_cols = self.decode_msg(data[num + 25:num + 89])

        msg_config = {
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
        fmt_str = '<' + ''.join(self.TYPE_MAP[t] for t in fmt_format)
        self._structs[fmt_type] = struct.Struct(fmt_str)

        return msg_config
    def compile_all_structs(self) -> None:
        """Compile struct formats for all known message types."""
        for type_msg, msg_config in self.fmt_messages.items():
            fmt_str = '<' + ''.join(self.TYPE_MAP[t] for t in msg_config["Format"])
            self._structs[type_msg] = struct.Struct(fmt_str)

    def is_new_message(self, data: memoryview, pos: int) -> bool:
        """Check if valid message header exists at position."""
        if data[pos] != 0xA3 or data[pos + 1] != 0x95:
            return False
        type_msg = data[pos + 2]
        return type_msg == 0x80 or type_msg in self.fmt_messages

    def read_messages(self, data: bytes | memoryview, to_round: bool,
                      message_type_to_read: MessageType = MessageType.ALL_MESSAGES,
                      fmt_messages=None, wanted_type: str = "") -> Generator[dict, None, None]:
        """Yield messages from binary data."""
        pos = 0
        data_len = len(data)
        if isinstance(data, bytes):
            data = memoryview(data)
        if fmt_messages is not None:
            self.fmt_messages = fmt_messages
        if not self._structs:
            self.compile_all_structs()

        read_fmt = message_type_to_read in {MessageType.ALL_MESSAGES, MessageType.FMT_MESSAGE}
        read_data = message_type_to_read in {MessageType.ALL_MESSAGES, MessageType.DATA_MESSAGE}
        filter_type = bool(wanted_type)

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
                    yield self._parse_data_msg(data, msg_config, pos + 3, to_round)
                pos += msg_config["Length"]

    def _parse_data_msg(self, payload: memoryview, msg_config: dict,
                        offset: int, to_round: bool) -> dict:
        """Parse data message efficiently."""
        format_msg = msg_config["Format"]
        cols = msg_config["cols"]
        type_msg = msg_config["Type"]

        values = self._structs[type_msg].unpack_from(payload, offset)
        result = {"mavpackettype": msg_config["Name"]}

        scale_100 = self.SCALE_100
        string_set = self.STRING
        round_set = self.ROUND

        for i, t in enumerate(format_msg):
            val = values[i]
            col = cols[i]

            if t in scale_100:
                val *= 0.01  # Faster than division
                if to_round and col in round_set:
                    val = round(val, 7)
            elif t in string_set and col != "Data":
                val = val.partition(b'\x00')[0].decode('ascii', errors='ignore')
            elif t == 'L':
                val *= 1e-7
                if to_round and col in round_set:
                    val = round(val, 7)

            result[col] = val

        return result