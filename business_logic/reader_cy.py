import struct
from enum import Enum
from typing import Dict, Generator, Union, Optional
import array

# If Cython is available
try:
    from reader_fast import parse_data_message, parse_fmt_message
    print("using cython")
    USE_CYTHON = True
except ImportError:
    USE_CYTHON = False

class MessageType(Enum):
    ALL_MESSAGES = 0
    FMT_MESSAGE = 1
    DATA_MESSAGE = 2

class Reader:
    HEADER = b'\xA3\x95'
    HEADER_ARRAY = array.array('B', HEADER)
    FMT_MSG_TYPE = 0x80
    FMT_MSG_LENGTH = 89

    def __init__(self):
        self.fmt_messages: Dict[int, Dict] = {}
        # Pre-compile struct formats for common operations
        self._header_struct = struct.Struct('<BB')

    def _parse_fmt_message(self, data: memoryview, offset: int) -> dict:
        if USE_CYTHON:
            return parse_fmt_message(data, offset, self.fmt_messages)

        # Optimized Python fallback
        fmt_type = data[offset+3]
        fmt_length = data[offset+4]
        # Use more efficient slicing and decoding
        name_end = offset + 9
        format_end = offset + 25
        cols_end = offset + 89

        fmt_name = bytes(data[offset+5:name_end]).decode('ascii').rstrip('\x00')
        fmt_format = bytes(data[offset+9:format_end]).decode('ascii').rstrip('\x00')
        fmt_cols = bytes(data[offset+25:cols_end]).decode('ascii').rstrip('\x00')

        columns = fmt_cols.split(',')
        self.fmt_messages[fmt_type] = {
            'name': fmt_name,
            'length': fmt_length,
            'format': fmt_format,
            'columns': columns,
            'struct': struct.Struct('<' + ''.join(fmt_format))
        }

        return {
            'mavpackettype': 'FMT',
            'Name': fmt_name,
            'Length': fmt_length,
            'Format': fmt_format,
            'Columns': fmt_cols,
            'Type': fmt_type
        }

    def _parse_data_message(self, data: memoryview, msg_type: int, offset: int, to_round: bool) -> dict:
        cfg = self.fmt_messages[msg_type]
        if USE_CYTHON:
            return parse_data_message(data, offset,
                                   cfg['struct'], cfg['columns'], cfg['format'],
                                   cfg['name'], to_round)

        # Optimized Python fallback
        values = cfg['struct'].unpack_from(bytes(data[offset:offset+cfg['length']]))
        result = {'mavpackettype': cfg['name']}

        # Use zip for more efficient iteration
        for col, val in zip(cfg['columns'], values):
            result[col] = val

        return result

    def _is_valid_header(self, data: memoryview, pos: int) -> bool:
        if pos + 2 >= len(data):
            return False

        # Use direct comparison instead of multiple checks
        header_valid = (data[pos] == 0xA3 and data[pos+1] == 0x95)
        if not header_valid:
            return False

        msg_type = data[pos+2]
        return msg_type == self.FMT_MSG_TYPE or msg_type in self.fmt_messages

    def _find_next_header(self, data: memoryview, start_pos: int) -> int:
        # Optimize header search using memoryview and array
        view = data[start_pos:]
        try:
            return bytes(view).index(self.HEADER)
        except ValueError:
            return -1

    def read_messages(self, data: Union[bytes, bytearray, memoryview],
                     to_round: bool = True,
                     wanted_type: Union[str, MessageType, None] = "",
                     run_mode: Optional[str] = None,
                     num_workers: Optional[int] = None) -> Generator[dict, None, None]:

        if not isinstance(data, memoryview):
            data = memoryview(data)

        pos = 0
        data_len = len(data)

        while pos < data_len:
            if not self._is_valid_header(data, pos):
                next_header = self._find_next_header(data, pos)
                if next_header == -1:
                    break
                pos += next_header
                continue

            msg_type = data[pos+2]
            if msg_type == self.FMT_MSG_TYPE:
                if wanted_type in ("", None, MessageType.ALL_MESSAGES):
                    yield self._parse_fmt_message(data, pos)
                pos += self.FMT_MSG_LENGTH
            else:
                if msg_type not in self.fmt_messages:
                    pos += 1
                    continue
                if wanted_type in ("", None, MessageType.ALL_MESSAGES):
                    yield self._parse_data_message(data, msg_type, pos+3, to_round)
                pos += self.fmt_messages[msg_type]['length']
