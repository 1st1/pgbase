# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


from collections.abc import Iterable as IterableABC, Sized as SizedABC


DEF ARRAY_MAXDIM = 6  # defined in postgresql/src/includes/c.h


ctypedef object (*encode_func_ex)(CodecContext settings,
                                  WriteBuffer buf,
                                  object obj,
                                  const void *arg)


ctypedef object (*decode_func_ex)(CodecContext settings,
                                  FastReadBuffer buf,
                                  const void *arg)


cdef inline bint _is_trivial_container(object obj):
    return cpython.PyUnicode_Check(obj) or cpython.PyBytes_Check(obj) or \
            PyByteArray_Check(obj) or PyMemoryView_Check(obj)


cdef inline pgbase_is_array_iterable(object obj):
    return (
        isinstance(obj, IterableABC) and
        isinstance(obj, SizedABC) and
        not _is_trivial_container(obj)
    )


cdef inline _is_sub_array_iterable(object obj):
    # Sub-arrays have a specialized check, because we treat
    # nested tuples as records.
    return pgbase_is_array_iterable(obj) and not cpython.PyTuple_Check(obj)


cdef pgbase_get_array_shape(object obj, int32_t *dims, int32_t *ndims):
    cdef:
        ssize_t mylen = len(obj)
        ssize_t elemlen = -2
        object it

    if mylen > _MAXINT32:
        raise ValueError('too many elements in array value')

    if ndims[0] > ARRAY_MAXDIM:
        raise ValueError(
            'number of array dimensions ({}) exceed the maximum expected ({})'.
                format(ndims[0], ARRAY_MAXDIM))

    dims[ndims[0] - 1] = <int32_t>mylen

    for elem in obj:
        if _is_sub_array_iterable(elem):
            if elemlen == -2:
                elemlen = len(elem)
                if elemlen > _MAXINT32:
                    raise ValueError('too many elements in array value')
                ndims[0] += 1
                pgbase_get_array_shape(elem, dims, ndims)
            else:
                if len(elem) != elemlen:
                    raise ValueError('non-homogeneous array')
        else:
            if elemlen >= 0:
                raise ValueError('non-homogeneous array')
            else:
                elemlen = -1


cdef _write_array_data(CodecContext settings, object obj, int32_t ndims,
                       int32_t dim, WriteBuffer elem_data,
                       encode_func_ex encoder, const void *encoder_arg):
    if dim < ndims - 1:
        for item in obj:
            _write_array_data(settings, item, ndims, dim + 1, elem_data,
                              encoder, encoder_arg)
    else:
        for item in obj:
            if item is None:
                elem_data.write_int32(-1)
            else:
                try:
                    encoder(settings, elem_data, item, encoder_arg)
                except TypeError as e:
                    raise ValueError(
                        'invalid array element: {}'.format(e.args[0])) from None


cdef inline pgbase_array_encode(
        CodecContext settings, WriteBuffer buf,
        object obj, uint32_t elem_oid,
        encode_func_ex encoder, const void *encoder_arg):

    cdef:
        WriteBuffer elem_data
        int32_t dims[ARRAY_MAXDIM]
        int32_t ndims = 1
        int32_t i

    if not pgbase_is_array_iterable(obj):
        raise TypeError(
            'a sized iterable container expected (got type {!r})'.format(
                type(obj).__name__))

    pgbase_get_array_shape(obj, dims, &ndims)

    elem_data = WriteBuffer.new()

    if ndims > 1:
        _write_array_data(settings, obj, ndims, 0, elem_data,
                          encoder, encoder_arg)
    else:
        for i, item in enumerate(obj):
            if item is None:
                elem_data.write_int32(-1)
            else:
                try:
                    encoder(settings, elem_data, item, encoder_arg)
                except TypeError as e:
                    raise ValueError(
                        'invalid array element at index {}: {}'.format(
                            i, e.args[0])) from None

    buf.write_int32(12 + 8 * ndims + elem_data.len())
    # Number of dimensions
    buf.write_int32(ndims)
    # flags
    buf.write_int32(0)
    # element type
    buf.write_int32(<int32_t>elem_oid)
    # upper / lower bounds
    for i in range(ndims):
        buf.write_int32(dims[i])
        buf.write_int32(1)
    # element data
    buf.write_buffer(elem_data)


cdef inline pgbase_array_decode(
        CodecContext settings, FastReadBuffer buf,
        decode_func_ex decoder, const void *decoder_arg):

    cdef:
        int32_t ndims = hton.unpack_int32(buf.read(4))
        int32_t flags = hton.unpack_int32(buf.read(4))
        uint32_t elem_oid = <uint32_t>hton.unpack_int32(buf.read(4))
        list result
        int i
        int32_t elem_len
        int32_t elem_count = 1
        FastReadBuffer elem_buf = FastReadBuffer.new()
        int32_t dims[ARRAY_MAXDIM]

    if ndims == 0:
        result = cpython.PyList_New(0)
        return result

    if ndims > ARRAY_MAXDIM:
        raise RuntimeError(
            'number of array dimensions ({}) exceed the maximum expected ({})'.
            format(ndims, ARRAY_MAXDIM))

    for i in range(ndims):
        dims[i] = hton.unpack_int32(buf.read(4))
        # Ignore the lower bound information
        buf.read(4)

    if ndims == 1:
        # Fast path for flat arrays
        elem_count = dims[0]
        result = cpython.PyList_New(elem_count)

        for i in range(elem_count):
            elem_len = hton.unpack_int32(buf.read(4))
            if elem_len == -1:
                elem = None
            else:
                elem_buf.slice_from(buf, elem_len)
                elem = decoder(settings, elem_buf, decoder_arg)

            cpython.Py_INCREF(elem)
            cpython.PyList_SET_ITEM(result, i, elem)

    else:
        result = _nested_array_decode(settings, buf,
                                      decoder, decoder_arg, ndims, dims,
                                      elem_buf)

    return result


cdef _nested_array_decode(CodecContext settings,
                          FastReadBuffer buf,
                          decode_func_ex decoder,
                          const void *decoder_arg,
                          int32_t ndims, int32_t *dims,
                          FastReadBuffer elem_buf):

    cdef:
        int32_t elem_len
        int64_t i, j
        int64_t array_len = 1
        object elem, stride
        # An array of pointers to lists for each current array level.
        void *strides[ARRAY_MAXDIM]
        # An array of current positions at each array level.
        int32_t indexes[ARRAY_MAXDIM]

    if PG_DEBUG:
        if ndims <= 0:
            raise RuntimeError('unexpected ndims value: {}'.format(ndims))

    for i in range(ndims):
        array_len *= dims[i]
        indexes[i] = 0

    for i in range(array_len):
        # Decode the element.
        elem_len = hton.unpack_int32(buf.read(4))
        if elem_len == -1:
            elem = None
        else:
            elem = decoder(settings,
                           elem_buf.slice_from(buf, elem_len),
                           decoder_arg)

        # Take an explicit reference for PyList_SET_ITEM in the below
        # loop expects this.
        cpython.Py_INCREF(elem)

        # Iterate over array dimentions and put the element in
        # the correctly nested sublist.
        for j in reversed(range(ndims)):
            if indexes[j] == 0:
                # Allocate the list for this array level.
                stride = cpython.PyList_New(dims[j])

                strides[j] = <void*><cpython.PyObject>stride
                # Take an explicit reference for PyList_SET_ITEM below
                # expects this.
                cpython.Py_INCREF(stride)

            stride = <object><cpython.PyObject*>strides[j]
            cpython.PyList_SET_ITEM(stride, indexes[j], elem)
            indexes[j] += 1

            if indexes[j] == dims[j] and j != 0:
                # This array level is full, continue the
                # ascent in the dimensions so that this level
                # sublist will be appened to the parent list.
                elem = stride
                # Reset the index, this will cause the
                # new list to be allocated on the next
                # iteration on this array axis.
                indexes[j] = 0
            else:
                break

    stride = <object><cpython.PyObject*>strides[0]
    # Since each element in strides has a refcount of 1,
    # returning strides[0] will increment it to 2, so
    # balance that.
    cpython.Py_DECREF(stride)
    return stride
