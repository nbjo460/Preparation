import struct
import time
from typing import Any, Generator


import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.logger import AppLogger
from utils.exceptions import UnknownMessageFormatError

class CoordinateExtractor:

    HEADER = b'\xA3\x95'
    TYPE_MAP = {
        'a': ('32h', 2 * 32),  # int16_t[32]
        'b': ('b', 1),  # int8_t
        'B': ('B', 1),  # uint8_t
        'h': ('h', 2),  # int16_t
        'H': ('H', 2),  # uint16_t
        'i': ('i', 4),  # int32_t
        'I': ('I', 4),  # uint32_t
        'f': ('f', 4),  # float
        'd': ('d', 8),  # double
        'n': ('4s', 4),  # char[4]
        'N': ('16s', 16),  # char[16]
        'Z': ('64s', 64),  # char[64]
        'c': ('h', 2),  # int16_t (ערך בודד, לא מערך)
        'C': ('H', 2),  # uint16_t * 100
        'e': ('i', 4),  # int32_t (ערך בודד, לא מערך)
        'E': ('I', 4),  # uint32_t * 100
        'L': ('i', 4),  # int32_t latitude/longitude in 1e-7 degrees
        'M': ('B', 1),  # uint8_t flight mode
        'q': ('q', 8),  # int64_t
        'Q': ('Q', 8),  # uint64_t
    }
    SCALE_100_SET = frozenset(["c", "C", "e", "E"])
    STRING_SET = frozenset(["n", "N", "Z"])
    NOT_ROUND_SET = frozenset(["Dist", "XT", "XTi","AsE","Alt","RelHomeAlt","RelOriginAlt", "Q1", "Q2","Q3", "Q4", "Spd", "VZ", "GCrs"])
    FMT_MSG_LENGTH = 89

    def __init__(self) -> None:
        self.logger = AppLogger(self.__class__.__name__)
        self.path = r"C:\Users\Menachem\Desktop\9900\Hafifa\log_file_test_01.bin"
        self.fmt_massages = {}
        self.STRUCT_CACHE = {}
        self._PROCESSING_CACHE = {}
        self.known_types = set()

    @staticmethod
    def decode_msg(data : memoryview) -> str:
        return data.tobytes().partition(b'\x00')[0].decode('ascii', errors='ignore')

    def read_fmt_massage(self, data: memoryview, start_offset: int) -> None:
        num = start_offset
        is_fmt_msg = data[num + 2] == 0x80
        if is_fmt_msg:
            fmt_type = data[num + 3]
            fmt_name = self.decode_msg(data[num + 5: num + 9])
            fmt_length = data[num + 4]
            fmt_format = self.decode_msg(data[num + 9: num + 9 + 16])
            fmt_cols = self.decode_msg(data[num + 25:num + 25 + 64]).split(",")
            msg_config = {
                "name": fmt_name,
                "length": fmt_length,
                "format": fmt_format,
                "cols": fmt_cols,
                "num_of_cols": len(fmt_format),
                "type" : fmt_type
            }
            self.fmt_massages[fmt_type] = msg_config
            self.known_types.add(fmt_type)

            self._compile_processing(fmt_type, fmt_format, fmt_cols)
        elif not is_fmt_msg:
            raise "No fmt."

    def _compile_processing(self, type_msg : int, types : str, cols : list[str]) -> None:
        processing = []
        for i, (t, col) in enumerate(zip(types, cols)):
            op = None
            if t in self.SCALE_100_SET:
                op = ('scale100', col not in self.NOT_ROUND_SET)
            elif t in self.STRING_SET and col != "Data":
                op = ("string", )
            elif t == "L":
                op = ("latlon", col not in self.NOT_ROUND_SET)
            processing.append((col, op))
        self._PROCESSING_CACHE[type_msg] = processing

    def get_struct_for_type(self, type_msg: int, types: str):
        if type_msg not in self.STRUCT_CACHE:
            fmt_string = ''.join(self.TYPE_MAP[t][0] for t in types)
            self.STRUCT_CACHE[type_msg] = struct.Struct(f"<{fmt_string}")
        return self.STRUCT_CACHE[type_msg]

    def from_bin(self, path: str, to_round : bool= False):
        """
        :param path: Path of a bin file.
        :return: List of all messages who founds.
        """
        with open(path, "rb") as file:
            data = memoryview(file.read())
            yield from self.read_messages(data, to_round)

    def is_new_message(self, data : memoryview, start_pos : int) -> bool:
        header = data[start_pos: start_pos + 2]
        is_correct_header = header[0] == 0xA3 and header[1] == 0x95
        if not is_correct_header:
            return False
        type_message = data[start_pos + 2]
        type_is_fmt = type_message == 0x80
        type_is_known = type_message in self.known_types or type_is_fmt
        # if not type_is_known:
        #     raise UnknownMessageFormatError
        return type_is_known


    def read_messages(self, data: memoryview, to_round : bool) -> Generator[dict[Any, Any], Any, None]:
        start_pos : int = 0
        length_data = len(data)
        fmt_messages = self.fmt_massages
        while start_pos < length_data:

            is_new_message = self.is_new_message(data, start_pos)
            type_msg = data[start_pos + 2]

            if is_new_message:
                is_fmt = type_msg == 0x80
                if is_fmt:
                    self.read_fmt_massage(data, start_pos)
                    start_pos += 89

                elif not is_fmt:
                    msg_config =  fmt_messages[type_msg]
                    skip = 3 + start_pos
                    value = self.get_value_by_format(data, msg_config, skip, to_round)
                    yield value
                    length = msg_config["length"]
                    start_pos += length

            elif not is_new_message:
                next_head = bytes(data[start_pos:]).find(self.HEADER)
                if next_head == -1:
                    break
                start_pos += next_head
                # start_pos += 1

        return None

    # @profile
    def get_value_by_format(self, payload: memoryview, msg_config: dict, header_to_skip: int,
                             to_round : bool):
        format_msg = msg_config["format"]
        type_msg = msg_config["type"]

        struct_fmt = self.get_struct_for_type(type_msg, format_msg)
        values = struct_fmt.unpack_from(payload, offset=header_to_skip)  # פענוח מלא בבת אחת

        result = {}
        processing = self._PROCESSING_CACHE[type_msg]
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

        return result

if __name__ == "__main__":
    start = time.time()

    path = r"C:\Users\Menachem\Desktop\9900\Hafifa\log_file_test_01.bin"
    coordinate_ex = CoordinateExtractor()


    for  num , msg in enumerate(coordinate_ex.from_bin(path, True)):
        # if num % 1000000 == 0:
        #     # pass
        #     print(msg)
        #     print(num)
        pass

    end = time.time()

    print(f"Elapsed time: {end - start:.6f} seconds")
