# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


from libc cimport math


cdef pgbase_float4_encode(CodecContext settings, WriteBuffer buf, obj):
    cdef double dval = cpython.PyFloat_AsDouble(obj)
    cdef float fval = <float>dval
    if math.isinf(fval) and not math.isinf(dval):
        raise ValueError('value out of float32 range')

    buf.write_int32(4)
    buf.write_float(fval)


cdef pgbase_float4_decode(CodecContext settings, FastReadBuffer buf):
    cdef float f = hton.unpack_float(buf.read(4))
    return cpython.PyFloat_FromDouble(f)


cdef pgbase_float8_encode(CodecContext settings, WriteBuffer buf, obj):
    cdef double dval = cpython.PyFloat_AsDouble(obj)
    buf.write_int32(8)
    buf.write_double(dval)


cdef pgbase_float8_decode(CodecContext settings, FastReadBuffer buf):
    cdef double f = hton.unpack_double(buf.read(8))
    return cpython.PyFloat_FromDouble(f)
