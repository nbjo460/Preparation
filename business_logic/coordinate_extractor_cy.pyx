# coordinate_extractor_cy.pyx
# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: initializedcheck=False
# cython: cdivision=True

import struct

cdef dict TYPE_MAP = {
    b'a': (b'32h', 64),
    b'b': (b'b', 1),
    b'B': (b'B', 1),
    b'h': (b'h', 2),
    b'H': (b'H', 2),
    b'i': (b'i', 4),
    b'I': (b'I', 4),
    b'f': (b'f', 4),
    b'd': (b'd', 8),
    b'n': (b'4s', 4),
    b'N': (b'16s', 16),
    b'Z': (b'64s', 64),
    b'c': (b'h', 2),
    b'C': (b'H', 2),
    b'e': (b'i', 4),
    b'E': (b'I', 4),
    b'L': (b'i', 4),
    b'M': (b'B', 1),
    b'q': (b'q', 8),
    b'Q': (b'Q', 8),
}

cdef dict STRUCT_CACHE = {}

def get_struct(int type_msg, str fmt_str):
    key = (type_msg, fmt_str)
    if key not in STRUCT_CACHE:
        fmt = b''.join(TYPE_MAP.get(t.encode('utf-8'), (b'x', 1))[0] for t in fmt_str)
        STRUCT_CACHE[key] = struct.Struct(b'<' + fmt)
    return STRUCT_CACHE[key]

def decode_msg(bytes msg):
    return msg.partition(b'\x00')[0].decode('ascii', 'ignore')

def get_value_by_format(bytes payload, str types, str cols, int type_msg, bint to_round=False):
    st = get_struct(type_msg, types)
    try:
        values = st.unpack_from(payload)
    except struct.error:
        return {}

    cols_list = cols.split(",")
    result = {}
    cdef int i
    cdef object val
    cdef str t

    ROUND_SET = frozenset([
        "Lat", "Lng", "TLat", "TLng", "Pitch", "IPE", "Yaw", "IPN", "IYAW",
        "DesPitch", "NavPitch", "Temp", "AltE", "VDop", "VAcc", "Roll",
        "HAGL", "SM", "VWN", "VWE", "IVT", "SAcc", "TAW", "IPD", "ErrRP",
        "SVT", "SP", "TAT", "GZ", "HDop", "NavRoll", "NavBrg", "TAsp",
        "HAcc", "DesRoll", "SH", "TBrg", "AX"
    ])

    for i in range(len(cols_list)):
        t = types[i]
        val = values[i]

        if t in ("c","C","e","E"):
            val /= 100
            if to_round and cols_list[i] in ROUND_SET:
                val = round(val, 7)
        elif t == "L":
            val *= 1e-7
            if to_round and cols_list[i] in ROUND_SET:
                val = round(val, 7)
        elif t in ("n","N","Z") and isinstance(val, bytes):
            val = val.partition(b'\x00')[0].decode('ascii', 'ignore')

        result[cols_list[i]] = val

    return result
