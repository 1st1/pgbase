# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


from libc.stdint cimport (
    int8_t, uint8_t, int16_t, uint16_t,
    int32_t, uint32_t, int64_t, uint64_t,
    INT16_MIN, INT16_MAX, INT32_MIN, INT32_MAX,
    UINT32_MAX, INT64_MIN, INT64_MAX
)


from .pgbase.inc.debug cimport PG_DEBUG

include "./inc/consts.pxi"
include "./inc/buffer.pxd"


include "./inc/codecs/base.pxd"
