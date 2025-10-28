# from coordinate_extractor_cy import  get_value_by_format
import struct
import time

from pymavlink import mavutil

from pymavlink.CSVReader import CSVMessage


from utils.logger import AppLogger
class CoordinateExtractor:

    def __init__(self) -> None:
        self.logger = AppLogger(self.__class__.__name__)
    def from_bin(self, path: str) -> list[tuple[float, float]]:
        """
        :param path: Path of a bin file.
        :return: List of all coordinates who founds.
        """
        mav: CSVMessage = mavutil.mavlink_connection(path)
        self.logger.info("Reading the file...")

        coordinates: list[tuple[float, float]] = []

        remain_info_in_file = True
        while remain_info_in_file:
            gps_massage: mav = mav.recv_match(type=["GPS"], blocking = False)
            if gps_massage is None:
                break

            if gps_massage.I == 1:
                lat: float = gps_massage.Lat
                lon: float = gps_massage.Lng
                coordinates.append((lat, lon))
        self.logger.debug(f"Found {len(coordinates)} Coordinates.")
        return coordinates


path = r"C:\Users\Menachem\Desktop\9900\Hafifa\log_file_test_01.bin"
msgs = []


fmt_massages = {}
def read_fmt_massage(data : bytes, start_offset : int) -> None:
    num = start_offset
    is_fmt_msg = data[num + 2] == 0x80
    if is_fmt_msg:
        fmt_type = data[num + 3]
        fmt_name = decode_msg(data[num + 5 : num + 9])
        fmt_length = data[num + 4]
        fmt_format = decode_msg(data[num + 9 : num + 9 + 16])
        fmt_cols = decode_msg(data[num + 25 :num + 25 +64])
        msg_config = {
            "name" : fmt_name,
            "length" : fmt_length,
            "format" : fmt_format,
            "cols" : fmt_cols,
            "values" : {}
        }
        fmt_massages[int(fmt_type)] = msg_config

    elif not is_fmt_msg:
        raise "No fmt."

TYPE_MAP = {
    'a': ('32h', 2 * 32),   # int16_t[32]
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
    'c': ('h', 2),          # int16_t (ערך בודד, לא מערך)
    'C': ('H', 2 ), # uint16_t * 100
    'e': ('i', 4),          # int32_t (ערך בודד, לא מערך)
    'E': ('I', 4), # uint32_t * 100
    'L': ('i', 4),          # int32_t latitude/longitude in 1e-7 degrees
    'M': ('B', 1),          # uint8_t flight mode
    'q': ('q', 8),          # int64_t
    'Q': ('Q', 8),          # uint64_t
}


STRUCT_CACHE = {}

def get_struct(fmt):
    if fmt not in STRUCT_CACHE:
        STRUCT_CACHE[fmt] = struct.Struct(f"<{fmt}")
    return STRUCT_CACHE[fmt]

# def get_value_by_format(payload: memoryview, types: str, cols: str):
#     result, offset = {}, 0
#     cols = cols.split(",")
#     for t, col in zip(types, cols):
#         fmt, size = TYPE_MAP[t]
#         struct_fmt = get_struct(fmt)
#         value = struct_fmt.unpack_from(payload, offset)
#         if t in ["c", "C", "e", "E"]:
#             value = (value[0] * 100,)
#         result[col] = (
#             # (value) if len(value) > 1 else
#             value.decode('ascii', errors='ignore').strip('\x00') if isinstance(value, bytes) else
#             value
#         )
#         offset += size
#     return result
def get_struct_for_type(type_msg: int, types: str):
    if type_msg not in STRUCT_CACHE:
        fmt_string = ''.join(TYPE_MAP[t][0] for t in types)
        STRUCT_CACHE[type_msg] = struct.Struct(f"<{fmt_string}")
    return STRUCT_CACHE[type_msg]



def get_value_by_format(payload: memoryview, types: str, cols: str, type_msg: int):
    struct_fmt = get_struct_for_type(type_msg, types)
    values = struct_fmt.unpack_from(payload)  # פענוח מלא בבת אחת
    cols_list = cols.split(",")

    result = {}
    for col, val, t in zip(cols_list, values, types):
        if t in ["c", "C", "e", "E"]:
            val *= 100
        elif t in ["n", "N", "Z"]:
            val = val.partition(b'\x00')[0].decode('ascii', errors='ignore')
        result[col] = val
    return result


def read_massages(data : bytes) -> None:

    num : int = 0
    length_data = len(data)
    # counter = 0
    while num < length_data:
        header = data[num : num + 2]
        is_new_massage = header[0] == 0xA3 and header[1] == 0x95

        if is_new_massage:
            # counter+=1
            type_msg = data[num + 2]
            is_fmt = type_msg == 0x80
            if is_fmt:
                read_fmt_massage(data, num)
                num += 89

            elif not is_fmt:
                exist_msg_config = fmt_massages[int(type_msg)]
                # name = exist_msg_config["name"]
                length = exist_msg_config["length"]
                format_msg = exist_msg_config["format"]
                cols = exist_msg_config["cols"]
                payload = data[num + 3: num + length]
                get_value_by_format(payload ,format_msg, cols, type_msg)
                num += length

        elif not is_new_massage:
            print(f"{header[0]:02x}")
            num+=1
            # raise "wrong offset"

def decode_msg(msg1 : bytes)-> str:
    # try:
    return msg1.partition(b'\x00')[0].decode('ascii', errors="ignore")
    # except Exception:
    #     return str(msg1)


with open(path, "rb") as f:
    data = f.read()

    start = time.time()
    read_massages(data)
    end = time.time()
    print(f"Elapsed time: {end - start:.6f} seconds")


