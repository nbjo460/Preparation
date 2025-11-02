# py_cy.pyx
# cython: language_level=3, boundscheck=False, wraparound=False

import struct
from cpython.memoryview cimport memoryview

# --- STRUCT_CACHE ו-TYPE_MAP גלובאליים ---
cdef dict STRUCT_CACHE = {}
cdef dict TYPE_MAP = {
    'a': ('32h', 64), 'b': ('b', 1), 'B': ('B', 1),
    'h': ('h', 2), 'H': ('H', 2), 'i': ('i', 4), 'I': ('I', 4),
    'f': ('f', 4), 'd': ('d', 8), 'n': ('4s', 4), 'N': ('16s', 16),
    'Z': ('64s', 64), 'c': ('h', 2), 'C': ('H', 2),
    'e': ('i', 4), 'E': ('I', 4), 'L': ('i', 4),
    'M': ('B', 1), 'q': ('q', 8), 'Q': ('Q', 8)
}

# --- הפונקציה המהירה ---
def get_value_by_format(
    memoryview payload,
    str types,
    list cols_list,
    int type_msg,
    int header_to_skip,
    int num_of_cols
):
    cdef object struct_fmt
    cdef tuple values
    cdef dict result = {}
    cdef int i
    cdef object val
    cdef str col
    cdef str t
    cdef str fmt_string
    cdef int l

    # 🔹 בונים struct פעם אחת בלבד
    if type_msg not in STRUCT_CACHE:
        fmt_string = ""
        for i in range(len(types)):
            fmt_string += TYPE_MAP[types[i]][0]
        STRUCT_CACHE[type_msg] = struct.Struct("<" + fmt_string)

    struct_fmt = STRUCT_CACHE[type_msg]
    values = struct_fmt.unpack_from(payload, offset=header_to_skip)

    for i in range(num_of_cols):
        col = cols_list[i]
        val = values[i]
        t = types[i]

        if t in {"c","C","e","E"}:
            val /= 100.0
        elif t in {"n","N","Z"}:
            if col != "Data":
                val = bytes(val).partition(b'\x00')[0].decode('ascii', errors='ignore')
        elif t == "L":
            val *= 1e-7

        result[col] = val

    return result
