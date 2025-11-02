# get_value_by_format_cython.pyx
# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: initializedcheck=False

import struct
from cpython.memoryview cimport memoryview
from cpython.bytes cimport PyBytes_AsStringAndSize

# --- חשוב: אין כאן STRUCT_CACHE ---
# אנחנו ניגשים ישירות ל-STRUCT_CACHE מה-Python!

# הפונקציה – **זהה בחתימה ל-Python המקורי**
cpdef dict get_value_by_format_cython(
    memoryview payload,
    str types,
    list cols_list,
    int type_msg,
    int header_to_skip,
    int num_of_cols
):
    # --- ניגשים ישירות ל-STRUCT_CACHE הגלובלי של Python ---
    # Cython רואה אותו כי הוא מוגדר במודול Python
    cdef struct.Struct struct_fmt = STRUCT_CACHE[type_msg]

    cdef tuple values = struct_fmt.unpack_from(payload, offset=header_to_skip)

    cdef dict result = {}
    cdef int i
    cdef str col
    cdef object val
    cdef char t

    for i in range(num_of_cols):
        col = cols_list[i]
        val = values[i]
        t = types[i]  # char

        if t in 'cCeE':
            val = val / 100.0
        elif t in 'nNZ':
            if col != "Data":
                cdef char* c_buf
                cdef Py_ssize_t buf_len
                PyBytes_AsStringAndSize(val, &c_buf, &buf_len)
                cdef int j = 0
                while j < buf_len and c_buf[j] != 0:
                    j += 1
                val = c_buf[:j].decode('ascii', errors='ignore')
        elif t == 'L':
            val = val * 1e-7

        result[col] = val

    return result