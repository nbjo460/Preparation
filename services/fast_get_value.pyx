# cython: language_level=3, boundscheck=False, wraparound=False, cdivision=True
import struct
cimport cython

cdef inline double decode_latlon(int val):
    return val * 1e-7

cdef inline double scale_100(int val):
    return val / 100.0

@cython.cfunc
def get_value_by_format_cy(payload, struct_fmt, processing, header_to_skip: int, to_round: bint):
    """
    payload: memoryview של הנתונים
    struct_fmt: struct.Struct מוכן מראש
    processing: רשימת (col, op)
    header_to_skip: offset להתחלת הפענוח
    to_round: האם לעגל
    """
    cdef list result = []
    cdef tuple values = struct_fmt.unpack_from(payload, offset=header_to_skip)
    cdef int i
    cdef col, op
    cdef val

    for i in range(len(processing)):
        col, op = processing[i]
        val = values[i]
        if op is None:
            result.append((col, val))
        elif op[0] == 'scale100':
            val = scale_100(val)
            if to_round and op[1]:
                val = round(val, 7)
            result.append((col, val))
        elif op[0] == 'latlon':
            val = decode_latlon(val)
            if to_round and op[1]:
                val = round(val, 7)
            result.append((col, val))
        elif op[0] == 'string':
            result.append((col, bytes(val).partition(b'\x00')[0].decode('ascii', errors='ignore')))
        else:
            result.append((col, val))

    return dict(result)
