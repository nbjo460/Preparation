# cython: boundscheck=False, wraparound=False, initializedcheck=False, cdivision=True, language_level=3

from libc.stdint cimport uint8_t
import struct

# =====================================================
# הגדרות גלובליות
# =====================================================
HEADER = b'\xA3\x95'

SCALE_100_SET = set(["c", "C", "e", "E"])
STRING_SET = set(["n", "N", "Z"])

fmt_massages = {}
STRUCT_CACHE = {}

# מיפוי סוגים בסיסי (מותאם לפי הקוד שלך)
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
    'c': ('h', 2),          # scaled int16
    'C': ('H', 2),          # scaled uint16
    'L': ('i', 4),          # GPS lat/lon scaled
    'e': ('h', 2),
    'E': ('H', 2),
    'n': ('32s', 32),
    'N': ('4s', 4),
    'Z': ('64s', 64),
}

# =====================================================
# פונקציות עזר
# =====================================================

cpdef str decode_msg(const unsigned char[:] msg1):
    """מפענח מחרוזת ASCII עד null-byte"""
    cdef Py_ssize_t i = 0
    cdef Py_ssize_t n = msg1.shape[0]
    while i < n and msg1[i] != 0:
        i += 1
    return bytes(msg1[:i]).decode('ascii', 'ignore')


# =====================================================
# קריאת הודעת FMT
# =====================================================

cpdef void read_fmt_massage(const unsigned char[:] data, Py_ssize_t start_offset):
    """קורא הודעת FMT ושומר בתצורה הגלובלית"""
    cdef unsigned char fmt_type = data[start_offset + 3]
    cdef unsigned char fmt_length = data[start_offset + 4]
    fmt_name = decode_msg(data[start_offset + 5 : start_offset + 9])
    fmt_format = decode_msg(data[start_offset + 9 : start_offset + 25])
    fmt_cols = decode_msg(data[start_offset + 25 : start_offset + 89]).split(",")

    msg_config = {
        "name": fmt_name,
        "length": fmt_length,
        "format": fmt_format,
        "cols": fmt_cols,
        "values": {},
    }
    fmt_massages[int(fmt_type)] = msg_config


# =====================================================
# פענוח הודעה לפי פורמט
# =====================================================

cpdef dict get_value_by_format(
    const unsigned char[:] payload,
    str types,
    list cols_list,
    int type_msg,
    Py_ssize_t header_to_skip
):
    """פענוח הודעה מלאה לפי הפורמט שלה"""
    cdef str fmt_str

    if type_msg not in STRUCT_CACHE:
        fmt_str = ""
        for t in types:
            fmt_str += TYPE_MAP[t][0]
        STRUCT_CACHE[type_msg] = struct.Struct(f"<{fmt_str}")

    struct_fmt = STRUCT_CACHE[type_msg]
    values = struct_fmt.unpack_from(payload, offset=header_to_skip)
    cdef dict result = {}

    for i in range(len(cols_list)):
        col = cols_list[i]
        val = values[i]
        t = types[i]

        if t in SCALE_100_SET:
            val = val / 100
        elif t in STRING_SET:
            if col != "Data":
                val = bytes(val).partition(b'\x00')[0].decode('ascii', 'ignore')
        elif t == "L":
            val = val * 1e-7
        result[col] = val

    return result


# =====================================================
# קריאת כל ההודעות (כולל FMT)
# =====================================================

cpdef list read_massages(const unsigned char[:] data):
    """מפענח את כל ההודעות מתוך ה־data"""
    cdef Py_ssize_t num = 0
    cdef Py_ssize_t length_data = data.shape[0]
    cdef unsigned char type_msg
    cdef bint is_new_massage
    cdef list messages = []

    while num < length_data:
        if num + 2 > length_data:
            break

        is_new_massage = data[num] == 0xA3 and data[num + 1] == 0x95
        if is_new_massage:
            type_msg = data[num + 2]

            if type_msg == 0x80:
                read_fmt_massage(data, num)
                num += 89
                continue

            if type_msg not in fmt_massages:
                num += 1
                continue

            cfg = fmt_massages[type_msg]
            length = cfg["length"]
            fmt_str = cfg["format"]
            cols = cfg["cols"]
            if num + length > length_data:
                break

            val = get_value_by_format(data, fmt_str, cols, type_msg, num + 3)
            val["mavpackettype"] = cfg["name"]
            messages.append(val)
            num += length
        else:
            next_head = bytes(data[num:]).find(HEADER)
            if next_head == -1:
                break
            num += next_head

    return messages
