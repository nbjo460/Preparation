import struct
import time
import cProfile
from typing import Any, Generator
from pymavlink import mavutil

from pymavlink.CSVReader import CSVMessage
from utils.logger import AppLogger
mavl = []
CHECK = 7596916 - 180
pr = cProfile.Profile()



class CoordinateExtractor:

    def __init__(self) -> None:
        self.logger = AppLogger(self.__class__.__name__)
    def from_bin(self, path: str, type : list[str]= ["GPS"]) -> list[tuple[float, float]]:
        """
        :param path: Path of a bin file.
        :return: List of all coordinates who founds.
        """
        global mavl
        mav: CSVMessage = mavutil.mavlink_connection(path)
        self.logger.info("Reading the file...")

        coordinates: list[tuple[float, float]] = []

        remain_info_in_file = True
        counter = 0
        while remain_info_in_file:
            gps_massage: mav = mav.recv_match(blocking = False)
            yield gps_massage
            if gps_massage is None:
                break
            # gps_massage = gps_massage.to_dict()
            # # if gps_massage["mavpackettype"] == "FMT":
            # #     continue
            # counter += 1
            # mavl.append(gps_massage)
            # if counter == CHECK: break
            # continue
        #     if gps_massage.I == 1:
        #         lat: float = gps_massage.Lat
        #         lon: float = gps_massage.Lng
        #         coordinates.append((lat, lon))
        # self.logger.debug(f"Found {len(coordinates)} Coordinates.")
        # return coordinates


path = r"C:\Users\Menachem\Desktop\9900\Hafifa\log_file_test_01.bin"
msgs = []

HEADER = b'\xA3\x95'
CHUNK_SIZE = 50 * 1024 * 1024  # 20 MB
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


fmt_massages = {}
STRUCT_CACHE = {}

# def get_struct(fmt):
#     # if fmt not in STRUCT_CACHE:
#     return struct.Struct(f"<{fmt}")
#     # return STRUCT_CACHE[fmt]

def read_fmt_massage(data : memoryview, start_offset : int) -> None:
    num = start_offset
    is_fmt_msg = data[num + 2] == 0x80
    if is_fmt_msg:
        fmt_type = data[num + 3]
        fmt_name = decode_msg(data[num + 5 : num + 9])
        fmt_length = data[num + 4]
        fmt_format = decode_msg(data[num + 9 : num + 9 + 16])
        fmt_cols = decode_msg(data[num + 25 :num + 25 +64]).split(",")
        msg_config = {
            "name" : fmt_name,
            "length" : fmt_length,
            "format" : fmt_format,
            "cols" : fmt_cols,
            "num_of_cols" : None
        }
        fmt_massages[int(fmt_type)] = msg_config

    elif not is_fmt_msg:
        raise "No fmt."



def get_struct_for_type(type_msg: int, types: str):
    if type_msg not in STRUCT_CACHE:
        fmt_string = ''.join(TYPE_MAP[t][0] for t in types)
        STRUCT_CACHE[type_msg] = struct.Struct(f"<{fmt_string}")
    return STRUCT_CACHE[type_msg]
# data_z = set()
SCALE_100_SET = {"c", "C", "e", "E"}
STRING_SET = {"n", "N", "Z"}

result = {"col":None}
# @profile
def get_value_by_format(payload: memoryview, types: str, cols_list: list[str], type_msg: int, header_to_skip : int, num_of_cols : int):
    struct_fmt = get_struct_for_type(type_msg, types)
    values = struct_fmt.unpack_from(payload, offset=header_to_skip)  # פענוח מלא בבת אחת
    # cols_list = cols.split(",")


    # for col, val, t in zip(cols_list, values, types):
    for i in range(num_of_cols):
        col = cols_list[i]
        val = values[i]
        t = types[i]

        if t in SCALE_100_SET:
            val /= 100
        elif t in STRING_SET:
            if col != "Data":
                val = bytes(val).partition(b'\x00')[0].decode('ascii', errors='ignore')
        elif t == "L":
            val = val * 1e-7


        result[col] = val
    return result

def get_value_by_format_proccessor(payload: memoryview, types: str, cols_list: list[str], type_msg: int, header_to_skip : int):
    result_p = {}
    struct_fmt = get_struct_for_type(type_msg, types)
    values = struct_fmt.unpack_from(payload, offset=header_to_skip)  # פענוח מלא בבת אחת
    # cols_list = cols.split(",")


    # for col, val, t in zip(cols_list, values, types):
    for i in range(len(cols_list)):
        col = cols_list[i]
        val = values[i]
        t = types[i]

        if t in SCALE_100_SET:
            val /= 100
        elif t in STRING_SET:
            if col != "Data":
                val = bytes(val).partition(b'\x00')[0].decode('ascii', errors='ignore')
        elif t == "L":
            val = val * 1e-7


        result_p[col] = val
    return result_p
LENGTH_COLS = {}
def calculate_len_cols(format_cols : str):
    try:
        return LENGTH_COLS[format_cols]
    except:
        LENGTH_COLS[format_cols] = len(format_cols)
        return LENGTH_COLS[format_cols]

def read_massages(data : memoryview) -> Generator[dict[Any, Any], Any, None]:
    num : int = 0
    length_data = len(data)
    counter = 0
    while num < length_data:
        header = data[num : num + 2]
        is_new_massage = header[0] == 0xA3 and header[1] == 0x95

        if is_new_massage:
            counter+=1
            type_msg = data[num + 2]
            is_fmt = type_msg == 0x80
            if is_fmt:
                read_fmt_massage(data, num)
                num += 89

            elif not is_fmt:
                exist_msg_config = fmt_massages[int(type_msg)]
                name = exist_msg_config["name"]
                length = exist_msg_config["length"]
                format_msg = exist_msg_config["format"]
                cols = exist_msg_config["cols"]
                num_of_cols = calculate_len_cols(format_msg)
                skip = 3 + num
                value = get_value_by_format(data ,format_msg, cols, type_msg, skip, num_of_cols)
                yield value
                # value['mavpackettype'] = name
                # messages_readable.append(value)
                # if len(msgs) == CHECK:
                #     break
                num += length

    # print(countr)

        elif not is_new_massage:
            # print(f"{header[0]:02x}")
            next_head = data[num:].find(HEADER)
            num+=next_head
    return None

def read_only_fmt(data : memoryview) -> None:
    num : int = 0
    length_data = len(data)
    counter = 0
    while num < length_data:
        header = data[num : num + 2]
        is_new_massage = header[0] == 0xA3 and header[1] == 0x95

        if is_new_massage:
            counter+=1
            type_msg = data[num + 2]
            is_fmt = type_msg == 0x80
            if is_fmt:
                read_fmt_massage(data, num)
                num += 89
            elif not is_fmt:
                exist_msg_config = fmt_massages[int(type_msg)]
                length = exist_msg_config["length"]
                num += length
    # print(countr)

        elif not is_new_massage:
            # print(f"{header[0]:02x}")
            next_head = data[num:].find(HEADER)
            num+=next_head


def decode_msg(msg1 : memoryview)-> str:
    return bytes(msg1).partition(b'\x00')[0].decode('ascii', errors="ignore")


def read_massages_by_chuncks(data : memoryview) -> bytes:
    global GLOBAL_FMT
    fmt_massages = GLOBAL_FMT
    num : int = 0
    length_data = len(data)
    counter = 0
    while num < length_data:
        header = data[num : num + 2]
        is_new_massage = header[0] == 0xA3 and header[1] == 0x95

        if is_new_massage:
            counter+=1
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

                remaining_bytes = len(data) - num
                does_massage_length_not_correct = length > remaining_bytes
                if does_massage_length_not_correct:
                    return bytes(data)[num:]

                skip = 3 + num
                value = get_value_by_format(data ,format_msg, cols, type_msg, skip)
                # print(value)
                num += length

        elif not is_new_massage:
            # print(f"{header[0]:02x}")
            next_head = data[num:].find(HEADER)
            if next_head == -1:
                return bytes(data[num:])
            num+=next_head



def read_massages_by_chuncks_return_msg(chunk_number : int, data : bytes):
    global GLOBAL_FMT
    data = memoryview(data)
    messages = []
    fmt_massages = GLOBAL_FMT
    num : int = 0
    length_data = len(data)
    counter = 0
    while num < length_data:
        header = data[num : num + 2]
        is_new_massage = header[0] == 0xA3 and header[1] == 0x95
        # if counter == 2:
        #     return chunk_number, messages
        if is_new_massage:
            type_msg = data[num + 2]
            is_fmt = type_msg == 0x80
            if is_fmt:
                num += 89
            elif not is_fmt:
                counter+=1
                exist_msg_config = fmt_massages[int(type_msg)]
                name = exist_msg_config["name"]
                length = exist_msg_config["length"]
                format_msg = exist_msg_config["format"]
                cols = exist_msg_config["cols"]

                # remaining_bytes = len(data) - num
                # does_massage_length_not_correct = length > remaining_bytes
                # if does_massage_length_not_correct:
                #     return bytes(data)[num:]

                skip = 3 + num
                value = get_value_by_format_proccessor(data ,format_msg, cols, type_msg, skip)
                value['mavpackettype'] = name
                messages.append(value)
                if len(messages_readable) == CHECK:
                    break
                # print(value)
                num += length

        elif not is_new_massage:
            # print(f"{header[0]:02x}")
            next_head = data[num:].find(HEADER)
            if next_head == -1:
                break
            num += next_head
    return chunk_number,messages




def find_chunk_boundaries(filepath: str, num_chunks: int, header: bytes = b'\xA3\x95') -> list[int]:
    import os
    size = os.path.getsize(filepath)
    boundaries = [0]

    with open(filepath, 'rb') as f:
        for i in range(1, num_chunks):
            f.seek(size // num_chunks * i)
            buffer = f.read(255)

            found = False
            pos = 0
            while pos < len(buffer) - 2:
                header_pos = buffer.find(header, pos)
                if header_pos == -1:
                    break

                type_msg = buffer[header_pos + 2]
                if type_msg in fmt_massages:
                    expected_length = fmt_massages[type_msg]['length']
                    if header_pos + expected_length < len(buffer):
                        if buffer[header_pos + expected_length:header_pos + expected_length + 2] == header:
                            boundaries.append(size // num_chunks * i + header_pos)
                            found = True
                            break

                pos = header_pos + 1
            if not found:
                boundaries.append(size // num_chunks * i)

        boundaries.append(size)

    return boundaries

def read_by_cuncks(file, callback_func):
    leftover = b''
    tasks = []
    while chunk:=file.read(CHUNK_SIZE):
        data = leftover + chunk
        mv = memoryview(data)
        tasks.append(mv)
        remaining_bytes = callback_func(mv)
        leftover = remaining_bytes

# def read_massages_by_chuncks_with_processors(data : memoryview, start : int, end :int) -> None:
#     num : int = start
#     length_data = len(data)
#     counter = 0
#     not_last_massage = True
#
#     while num < length_data and not_last_massage:
#         header = data[num : num + 2]
#         is_new_massage = header[0] == 0xA3 and header[1] == 0x95
#
#         if is_new_massage:
#             counter+=1
#             type_msg = data[num + 2]
#             is_fmt = type_msg == 0x80
#             if is_fmt:
#                 read_fmt_massage(data, num)
#                 num += 89
#
#             elif not is_fmt:
#                 exist_msg_config = fmt_massages[int(type_msg)]
#                 # name = exist_msg_config["name"]
#                 length = exist_msg_config["length"]
#                 format_msg = exist_msg_config["format"]
#                 cols = exist_msg_config["cols"]
#
#                 last_massage_in_chunk = length + num >= end
#                 if last_massage_in_chunk:
#                     not_last_massage = False
#
#                 skip = 3 + num
#                 value = get_value_by_format(data ,format_msg, cols, type_msg, skip)
#                 # print(value)
#                 num += length
#
#         elif not is_new_massage:
#             # print(f"{header[0]:02x}")
#             next_head = data[num:].find(HEADER)
#             if next_head == -1:
#                 break
#                 # return bytes(data[num:])
#             num=next_head

def split_cuncks(file_path : str, num_chunk : int) -> dict:
    chunks = {}
    chunks_pos : list[int]= find_chunk_boundaries(path, num_chunk, HEADER)
    with open(file_path, "rb") as file:
        data = file.read()
        for pos in range(len(chunks_pos) - 1):

            chunks[pos] = data[chunks_pos[pos]: chunks_pos[pos + 1]]
        return chunks

GLOBAL_FMT = None

def init_worker(fmt):
    global GLOBAL_FMT
    GLOBAL_FMT = fmt


def process_in_parallel(file_path : str, num_workers : int, callback_func):
    chunks : dict = split_cuncks(file_path, num_workers)
    chunks_items = list(chunks.items())
    combine = []
    from multiprocessing import Pool
    with Pool(num_workers, initializer=init_worker, initargs=(fmt_massages,)) as pool:
        results = pool.starmap(callback_func, chunks_items)
        results.sort(key=lambda x: x[0])
        for result in results:
            list_msg = result[1]
            combine.extend(list_msg)
    return combine

messages_readable = []

# from multiprocessing import Pool, cpu_count
if __name__ == "__main__":
    start = time.time()

    from multiprocessing import freeze_support
    from functools import partial

    with open(path, "rb") as file:
        # c = CoordinateExtractor()
        # for num, msg in enumerate(c.from_bin(path, [])):
        #     t = msg
            # if num %3500000 == 0:
            #     print(num, msg)

        data = file.read()
        data = memoryview(data)
        #
        read_only_fmt(data)
        # print(f"found{len(fmt_massages)}")
        #
        # # freeze_support()
        # messages_readable = process_in_parallel(path, 8, read_massages_by_chuncks_return_msg)
        # leng = len(messages_readable)
        # print(leng)
        # c = 0
        for msg in read_massages(data):
            c = msg
        #     pass
        # print(c)
        end = time.time()

        print(f"Elapsed time: {end - start:.6f} seconds")

#  # יוצרים אובייקט סטטיסטיקה
# stats = pstats.Stats(pr)
# stats.strip_dirs()  # מסיר נתיבי קבצים ארוכים
# stats.sort_stats('cumtime')  # מיון לפי זמן כולל כולל קריאות פנימיות
#
# # מציגים את ה-20 הפונקציות הכי "כבדות"
# print("=== 20 הפונקציות הכי כבדות ===")
# stats.print_stats(20)


# if __name__  == "__main__":
#     c = CoordinateExtractor()
#     c.from_bin(path, [])
#     checking = {True : 0, False : 0}
#
#     def fix(dicti : dict) -> dict:
#         fixed = {}
#         for key, val in dicti.items():
#
#             if isinstance(val, float):
#                 if key not in ("Dist", "XT", "XTi","AsE","Alt","RelHomeAlt","RelOriginAlt", "Q1", "Q2","Q3", "Q4", "Spd", "VZ", "GCrs"):
#                     val = round(val ,7)
#             fixed[key] = val
#         return fixed
#
#     for i in range(CHECK):
#         is_match = messages_readable[i] == mavl[i]
#         if not is_match:
#             messages_readable[i] = fix(messages_readable[i])
#             try:
#                 if math.isnan(mavl[i].get("Default")):
#                     mavl[i]["Default"] = "NAN"
#                     messages_readable[i]["Default"] = "NAN"
#             except TypeError:
#                 pass
#
#             is_match = messages_readable[i] == mavl[i]
#
#         checking[is_match] += 1
#         if not is_match:
#             print(messages_readable[i], mavl[i])
#
#
#     print(checking)
