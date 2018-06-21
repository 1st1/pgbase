# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


from .pgbase.inc.python cimport (
    PyMem_Malloc, PyMem_Realloc, PyMem_Calloc, PyMem_Free,
    PyMemoryView_GET_BUFFER, PyMemoryView_Check,
    PyMemoryView_FromMemory, PyMemoryView_GetContiguous,
    PyUnicode_AsUTF8AndSize, PyByteArray_AsString,
    PyByteArray_Check, PyUnicode_AsUCS4Copy,
    PyByteArray_Size, PyByteArray_Resize,
    PyByteArray_FromStringAndSize,
    PyUnicode_FromKindAndData, PyUnicode_4BYTE_KIND,
    PyUnicode_FromString
)


from .pgbase.inc cimport hton


include "./inc/consts.pxi"
include "./inc/buffer.pyx"

include "./inc/codecs/base.pyx"

include "./inc/codecs/bytea.pyx"
include "./inc/codecs/text.pyx"

include "./inc/codecs/datetime.pyx"
include "./inc/codecs/float.pyx"
include "./inc/codecs/int.pyx"
include "./inc/codecs/json.pyx"
include "./inc/codecs/uuid.pyx"
include "./inc/codecs/numeric.pyx"

include "./inc/codecs/array.pyx"
