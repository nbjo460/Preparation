# coordinate_extractor_cy.pyx
# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: initializedcheck=False
# cython: cdivision=True

import struct

# -------------------------------
# TYPE MAP
# -------------------------------
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

# -------------------------------
# STRUCT CACHE
# -------------------------------
cdef dict STRUCT_CACHE = {}

def get_struct(int type_msg, str fmt_str):
    """
    מחזיר struct.Struct object מהמטמון או יוצר חדש.
    """
    key = (type_msg, fmt_str)
    if key not in STRUCT_CACHE:
        fmt = b''.join(TYPE_MAP.get(t.encode('utf-8'), (b'x', 1))[0] for t in fmt_str)
        STRUCT_CACHE[key] = struct.Struct(b'<' + fmt)
    return STRUCT_CACHE[key]

# -------------------------------
# decode_msg
# -------------------------------
def decode_msg(bytes msg):
    """
    decode עד ה־NULL byte
    """
    return msg.partition(b'\x00')[0].decode('ascii', 'ignore')

# -------------------------------
# get_value_by_format
# -------------------------------
def get_value_by_format(bytes payload, str types, str cols, int type_msg):
    """
    פענוח payload לפי fmt ודינמיקה של TYPE_MAP
    """
    st = get_struct(type_msg, types)
    try:
        values = st.unpack_from(payload)
    except struct.error:
        return {}

    cols_list = cols.split(",")
    result = {}
    cdef int i
    for i in range(len(cols_list)):
        t = types[i]
        val = values[i]

        if t in ("c","C","e","E"):
            val *= 100
        elif t in ("n","N","Z") and isinstance(val, bytes):
            val = val.partition(b'\x00')[0].decode('ascii', 'ignore')

        result[cols_list[i]] = val

    return result
