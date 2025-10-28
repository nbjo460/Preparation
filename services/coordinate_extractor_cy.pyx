# coordinate_extractor_cy.pyx
import struct

# dict גלובלי כמו בקוד שלך
cdef dict STRUCT_CACHE = {}
cdef dict TYPE_MAP = {
    'a': ('32h', 2 * 32),
    'b': ('b', 1),
    'B': ('B', 1),
    'h': ('h', 2),
    'H': ('H', 2),
    'i': ('i', 4),
    'I': ('I', 4),
    'f': ('f', 4),
    'd': ('d', 8),
    'n': ('4s', 4),
    'N': ('16s', 16),
    'Z': ('64s', 64),
    'c': ('h', 2),
    'C': ('H', 2),
    'e': ('i', 4),
    'E': ('I', 4),
    'L': ('i', 4),
    'M': ('B', 1),
    'q': ('q', 8),
    'Q': ('Q', 8),
}

# פונקציה שמקבלת payload ובקשות unpack
def get_value_by_format(bytes payload, str types, str cols, int type_msg):
    # מבנה struct מוכן מראש
    if type_msg not in STRUCT_CACHE:
        fmt_string = ''.join([TYPE_MAP[t][0] for t in types])
        STRUCT_CACHE[type_msg] = struct.Struct(f"<{fmt_string}")
    struct_fmt = STRUCT_CACHE[type_msg]

    # unpack כל הנתונים
    values = struct_fmt.unpack_from(payload)
    cols_list = cols.split(",")
    cdef dict result = {}
    cdef int i
    cdef object val, t
    for i in range(len(cols_list)):
        t = types[i]
        val = values[i]
        if t in ["c","C","e","E"]:
            val *= 100
        elif t in ["n","N","Z"]:
            val = val.partition(b'\x00')[0].decode('ascii', errors='ignore')
        result[cols_list[i]] = val
    return result
