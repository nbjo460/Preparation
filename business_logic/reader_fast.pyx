# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
# cython: infer_types=True

from cpython.bytes cimport PyBytes_FromStringAndSize
from libc.string cimport memcpy, memchr
from libc.stdint cimport uint8_t, uint16_t, int32_t
import struct

# Optimized constant sets
cdef frozenset SCALE_100 = frozenset(['c','C','e','E'])
cdef frozenset STRINGS = frozenset(['n','N','Z'])
cdef frozenset ROUND_FIELDS = frozenset([
    'Lat','Lng','TLat','TLng','Pitch','Roll','Yaw',
    'IPE','IPN','IYAW','DesPitch','DesRoll','NavPitch',
    'NavRoll','NavBrg','Temp','AltE','HAGL','VDop','HDop',
    'VAcc','HAcc','SAcc','SM','SH','VWN','VWE','IVT',
    'TAW','TAsp','TAT','IPD','ErrRP','SVT','SP','GZ',
    'TBrg','AX'
])

# Pre-compiled struct formats
cdef dict TYPE_MAP = {
    'a':'32h','b':'b','B':'B','h':'h','H':'H',
    'i':'i','I':'I','f':'f','d':'d','n':'4s',
    'N':'16s','Z':'64s','c':'h','C':'H','e':'i',
    'E':'I','L':'i','M':'B','q':'q','Q':'Q'
}

ctypedef unsigned char uchar

cdef inline str decode_string(const uchar[:] data, int length):
    """Fast decode of null-terminated ASCII string using memchr."""
    cdef const uchar* ptr = &data[0]
    cdef uchar* end = <uchar*>memchr(ptr, 0, length)
    cdef int str_len = length if end == NULL else end - ptr
    return PyBytes_FromStringAndSize(<char*>ptr, str_len).decode('ascii', 'ignore')

cdef str build_struct_format(str fmt_format):
    """Build struct format string with pre-allocation."""
    cdef list parts = []
    parts.append('<')
    parts.extend(TYPE_MAP[t] for t in fmt_format)
    return ''.join(parts)

cpdef dict parse_fmt_message(const uchar[:] data, int offset, dict fmt_messages):
    """Parse format message with optimized memory access."""
    cdef:
        uchar fmt_type = data[offset+3]
        uchar fmt_length = data[offset+4]
        str fmt_name = decode_string(data[offset+5:offset+9], 4)
        str fmt_format = decode_string(data[offset+9:offset+25], 16)
        str fmt_cols = decode_string(data[offset+25:offset+89], 64)
        list columns = fmt_cols.split(',')
        str struct_fmt = build_struct_format(fmt_format)

    fmt_messages[fmt_type] = {
        'name': fmt_name,
        'length': fmt_length,
        'format': fmt_format,
        'columns': columns,
        'struct': struct.Struct(struct_fmt)
    }

    return {
        'mavpackettype': 'FMT',
        'Name': fmt_name,
        'Length': fmt_length,
        'Format': fmt_format,
        'Columns': fmt_cols,
        'Type': fmt_type
    }


cpdef dict parse_data_message(const unsigned char[:] data, int offset, object struct_obj,
                              list columns, str format_str, str msg_name, bint to_round):
    """Ultra-fast data message parser."""
    cdef tuple values = struct_obj.unpack_from(data, offset)
    cdef dict result = {'mavpackettype': msg_name}
    cdef int i
    cdef str col, fmt_char
    cdef object val
    cdef double dval

    for i in range(len(columns)):
        col = columns[i]
        fmt_char = format_str[i]
        val = values[i]

        if fmt_char in SCALE_100:
            dval = <double>val/100.0
            if to_round and col in ROUND_FIELDS:
                result[col] = round(dval,7)
            else:
                result[col] = dval
        elif fmt_char in STRINGS and col != 'Data':
            result[col] = val.partition(b'\x00')[0].decode('ascii','ignore')
        elif fmt_char=='L':
            dval = <double>val*1e-7
            if to_round and col in ROUND_FIELDS:
                result[col] = round(dval,7)
            else:
                result[col] = dval
        else:
            result[col] = val
    return result
