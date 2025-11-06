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
    FMT_HEADER = b'\xA3\x95\x80'
    FMT_MSG_LENGTH = 89

    TYPE_MAP = {
        'a': ('32h', 2 * 32),  # int16_t[32]
        'b': ('b', 1),          # int8_t
        'B': ('B', 1),          # uint8_t
        'h': ('h', 2),          # int16_t
        'H': ('H', 2),          # uint16_t
        'i': ('i', 4),          # int32_t
        'I': ('I', 4),          # uint32_t
        'f': ('f', 4),          # float
        'd': ('d', 8),          # double
        'n': ('4s', 4),         # char[4]
        'N': ('16s', 16),       # char[16]
        'Z': ('64s', 64),       # char[64]
        'c': ('h', 2),          # int16_t (scaled by 100)
        'C': ('H', 2),          # uint16_t * 100
        'e': ('i', 4),          # int32_t (scaled by 100)
        'E': ('I', 4),          # uint32_t * 100
        'L': ('i', 4),          # int32_t latitude/longitude in 1e-7 degrees
        'M': ('B', 1),          # uint8_t flight mode
        'q': ('q', 8),          # int64_t
        'Q': ('Q', 8),          # uint64_t
    }

    SCALE_100_SET = frozenset(["c", "C", "e", "E"])
    STRING_SET = frozenset(["n", "N", "Z"])

    ROUND_SET = frozenset([
        "Lat", "Lng", "TLat", "TLng", "Pitch", "IPE", "Yaw", "IPN", "IYAW",
        "DesPitch", "NavPitch", "Temp", "AltE", "VDop", "VAcc", "Roll",
        "HAGL", "SM", "VWN", "VWE", "IVT", "SAcc", "TAW", "IPD", "ErrRP",
        "SVT", "SP", "TAT", "GZ", "HDop", "NavRoll", "NavBrg", "TAsp",
        "HAcc", "DesRoll", "SH", "TBrg", "AX"
    ])

    NOT_ROUND_SET = frozenset([
        "Dist", "XT", "XTi", "AsE", "Alt", "RelHomeAlt", "RelOriginAlt",
        "Q1", "Q2", "Q3", "Q4", "Spd", "VZ", "GCrs"
    ])

    def __init__(self) -> None:
        self.logger = AppLogger(self.__class__.__name__)
        self.fmt_messages = {}
        self.STRUCT_CACHE = {}
        self.PROCESSING_CACHE = {}

    @staticmethod
    def decode_msg(data: memoryview) -> str:
        """Decode null-terminated ASCII string from memoryview."""
        return data.tobytes().partition(b'\x00')[0].decode('ascii', errors='ignore')

    def read_fmt_massage(self, data: memoryview, start_offset: int) -> dict:
        """Read and parse FMT message.

        Args:
            data: Binary data buffer
            start_offset: Position of FMT message in buffer

        Returns:
            Dictionary containing FMT message configuration
        """
        num = start_offset
        is_fmt_msg = data[num + 2] == 0x80

        if is_fmt_msg:
            fmt_type = data[num + 3]
            fmt_length = data[num + 4]
            fmt_name = self.decode_msg(data[num + 5:num + 9])
            fmt_format = self.decode_msg(data[num + 9:num + 25])
            fmt_cols = self.decode_msg(data[num + 25:num + 89])
            fmt_split_cols = fmt_cols.split(",")

            msg_config = {
                "mavpackettype": "FMT",
                "Name": fmt_name,
                "Length": fmt_length,
                "Format": fmt_format,
                "Columns": fmt_cols,
                "Type": fmt_type,
            }

            # Store FMT message configuration
            self.fmt_messages[fmt_type] = {**msg_config, "cols": fmt_split_cols}

            # Compile processing instructions for this message type
            self._compile_processing(fmt_type, fmt_format, fmt_split_cols)
            return msg_config

    def _compile_processing(self, type_msg: int, types: str, cols: list[str]) -> None:
        """Compile processing instructions for a message type.

        Args:
            type_msg: Message type ID
            types: Format string defining field types
            cols: Column names for fields
        """
        processing = []
        for t, col in zip(types, cols):
            op = None
            if t in self.SCALE_100_SET:
                op = ('scale100', col in self.ROUND_SET)
            elif t in self.STRING_SET and col != "Data":
                op = ('string',)
            elif t == "L":
                op = ('latlon', col in self.ROUND_SET)
            processing.append((col, op))
        self.PROCESSING_CACHE[type_msg] = processing

    def get_struct_for_type(self, type_msg: int, types: str) -> struct.Struct:
        """Get or create cached struct for unpacking message type.

        Args:
            type_msg: Message type ID
            types: Format string defining field types

        Returns:
            Compiled struct object
        """
        if type_msg not in self.STRUCT_CACHE:
            fmt_string = ''.join(self.TYPE_MAP[t][0] for t in types)
            self.STRUCT_CACHE[type_msg] = struct.Struct(f"<{fmt_string}")
        return self.STRUCT_CACHE[type_msg]

    def is_new_message(self, data: memoryview, start_pos: int) -> bool:
        """Check if position contains a valid message header.

        Args:
            data: Binary data buffer
            start_pos: Position to check

        Returns:
            True if valid message header found
        """
        if data[start_pos] != 0xA3 or data[start_pos + 1] != 0x95:
            return False
        type_msg = data[start_pos + 2]
        return type_msg == 0x80 or type_msg in self.fmt_messages

    def read_messages(self, data: memoryview, to_round: bool,
                     message_type_to_read: MessageType = MessageType.ALL_MESSAGES,
                     fmt_messages=None, wanted_type: str = "") -> Generator[dict[Any, Any], Any, None]:
        """Generator that yields messages from binary data.

        Args:
            data: Binary data buffer
            to_round: Whether to round numeric values
            message_type_to_read: Filter for message types (FMT, DATA, or ALL)
            fmt_messages: Pre-loaded format messages (optional)
            wanted_type: Specific message type name to filter, or empty for all

        Yields:
            Dictionary containing message data
        """
        data = memoryview(data)
        start_pos: int = 0
        length_data = len(data)

        if fmt_messages is not None:
            self.fmt_messages = fmt_messages
        else:
            fmt_messages = self.fmt_messages

        while start_pos < length_data:
            is_new_message = self.is_new_message(data, start_pos)
            type_msg = data[start_pos + 2]

            if is_new_message:
                # Determine message name
                msg_name = "FMT" if type_msg == 0x80 else fmt_messages[type_msg]["Name"]

                # Decide whether to return this message based on wanted_type
                return_this_message = wanted_type == "" or wanted_type == msg_name

                is_fmt = type_msg == 0x80

                if is_fmt:
                    if message_type_to_read in {MessageType.ALL_MESSAGES, MessageType.FMT_MESSAGE}:
                        msg = self.read_fmt_massage(data, start_pos)
                        if return_this_message:
                            yield msg
                    start_pos += self.FMT_MSG_LENGTH

                elif not is_fmt:  # Data message
                    msg_config = fmt_messages[type_msg]
                    if message_type_to_read in {MessageType.ALL_MESSAGES, MessageType.DATA_MESSAGE}:
                        if return_this_message:
                            value = self.get_value_by_format(data, msg_config, start_pos + 3, to_round)
                            yield value
                    start_pos += msg_config["Length"]

            elif not is_new_message:  # Invalid message, search for next header
                next_head = bytes(data[start_pos:]).find(self.HEADER)
                if next_head == -1:
                    break
                start_pos += next_head

    def get_value_by_format(self, payload: memoryview, msg_config: dict,
                           header_to_skip: int, to_round: bool) -> dict:
        """Extract and format message values from binary payload.

        Args:
            payload: Binary message data
            msg_config: Message format configuration
            header_to_skip: Offset to start of data fields
            to_round: Whether to round numeric values

        Returns:
            Dictionary containing formatted message fields
        """
        format_msg = msg_config["Format"]
        type_msg = msg_config["Type"]
        name = msg_config["Name"]

        struct_fmt = self.get_struct_for_type(type_msg, format_msg)
        values = struct_fmt.unpack_from(payload, offset=header_to_skip)

        result = {}
        processing = self.PROCESSING_CACHE[type_msg]

        for i, (col, op) in enumerate(processing):
            val = values[i]

            if op is None:
                result[col] = val
            elif op[0] == 'scale100':
                val /= 100
                result[col] = round(val, 7) if (to_round and op[1]) else val
            elif op[0] == 'string':
                result[col] = val.partition(b'\x00')[0].decode('ascii', errors='ignore')
            elif op[0] == 'latlon':
                val *= 1e-7
                result[col] = round(val, 7) if (to_round and op[1]) else val
            else:
                result[col] = val

        # Add message type name for reference
        result["mavpackettype"] = name
        return result
