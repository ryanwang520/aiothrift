import struct

from io import BytesIO
from struct import pack, unpack
from thriftpy2.protocol.exc import TProtocolException
from thriftpy2.thrift import TType

VERSION_MASK = -65536
VERSION_1 = -2147418112
TYPE_MASK = 0x000000FF


def pack_i8(byte):
    return struct.pack("!b", byte)


def pack_i16(i16):
    return struct.pack("!h", i16)


def pack_i32(i32):
    return struct.pack("!i", i32)


def pack_i64(i64):
    return struct.pack("!q", i64)


def pack_double(dub):
    return struct.pack("!d", dub)


def pack_string(string):
    return struct.pack("!i%ds" % len(string), len(string), string)


def unpack_i8(buffer):
    return struct.unpack("!b", buffer)[0]


def unpack_i16(buffer):
    return struct.unpack("!h", buffer)[0]


def unpack_i32(buffer):
    return struct.unpack("!i", buffer)[0]


def unpack_i64(buffer):
    return struct.unpack("!q", buffer)[0]


def unpack_double(buffer):
    return struct.unpack("!d", buffer)[0]


def write_message_begin(writer, name, ttype, seqid, strict=True):
    if strict:
        writer.write(pack_i32(VERSION_1 | ttype))
        writer.write(pack_string(name.encode("utf-8")))
    else:
        writer.write(pack_string(name.encode("utf-8")))
        writer.write(pack_i8(ttype))

    writer.write(pack_i32(seqid))


def write_field_begin(writer, ttype, fid):
    writer.write(pack_i8(ttype) + pack_i16(fid))


def write_field_stop(writer):
    writer.write(pack_i8(TType.STOP))


def write_list_begin(writer, etype, size):
    writer.write(pack_i8(etype) + pack_i32(size))


def write_map_begin(writer, ktype, vtype, size):
    writer.write(pack_i8(ktype) + pack_i8(vtype) + pack_i32(size))


def write_val(writer, ttype, val, spec=None):
    if ttype == TType.BOOL:
        if val:
            writer.write(pack_i8(1))
        else:
            writer.write(pack_i8(0))

    elif ttype == TType.BYTE:
        writer.write(pack_i8(val))

    elif ttype == TType.I16:
        writer.write(pack_i16(val))

    elif ttype == TType.I32:
        writer.write(pack_i32(val))

    elif ttype == TType.I64:
        writer.write(pack_i64(val))

    elif ttype == TType.DOUBLE:
        writer.write(pack_double(val))

    elif ttype == TType.STRING:
        if not isinstance(val, bytes):
            val = val.encode("utf-8")
        writer.write(pack_string(val))

    elif ttype == TType.SET or ttype == TType.LIST:
        if isinstance(spec, tuple):
            e_type, t_spec = spec[0], spec[1]
        else:
            e_type, t_spec = spec, None

        val_len = len(val)
        write_list_begin(writer, e_type, val_len)
        for e_val in val:
            write_val(writer, e_type, e_val, t_spec)

    elif ttype == TType.MAP:
        if isinstance(spec[0], int):
            k_type = spec[0]
            k_spec = None
        else:
            k_type, k_spec = spec[0]

        if isinstance(spec[1], int):
            v_type = spec[1]
            v_spec = None
        else:
            v_type, v_spec = spec[1]

        write_map_begin(writer, k_type, v_type, len(val))
        for k in iter(val):
            write_val(writer, k_type, k, k_spec)
            write_val(writer, v_type, val[k], v_spec)

    elif ttype == TType.STRUCT:
        for fid in iter(val.thrift_spec):
            f_spec = val.thrift_spec[fid]
            if len(f_spec) == 3:
                f_type, f_name, f_req = f_spec
                f_container_spec = None
            else:
                f_type, f_name, f_container_spec, f_req = f_spec

            v = getattr(val, f_name)
            if v is None:
                continue

            write_field_begin(writer, f_type, fid)
            write_val(writer, f_type, v, f_container_spec)
        write_field_stop(writer)


async def read_message_begin(reader, strict=True):
    data = await reader.readexactly(4)
    sz = unpack_i32(data)
    if sz < 0:
        version = sz & VERSION_MASK
        if version != VERSION_1:
            raise TProtocolException(
                type=TProtocolException.BAD_VERSION,
                message="Bad version in read_message_begin: %d" % (sz),
            )

        data = await reader.readexactly(4)
        name_sz = unpack_i32(data)
        data = await reader.readexactly(name_sz)
        name = data.decode("utf-8")
        type_ = sz & TYPE_MASK
    else:
        if strict:
            raise TProtocolException(
                type=TProtocolException.BAD_VERSION,
                message="No protocol version header",
            )

        data = await reader.readexactly(sz)
        name = data.decode("utf-8")
        data = await reader.readexactly(1)
        type_ = unpack_i8(data)

    data = await reader.readexactly(4)
    seqid = unpack_i32(data)

    return name, type_, seqid


async def read_field_begin(reader):
    data = await reader.readexactly(1)
    f_type = unpack_i8(data)
    if f_type == TType.STOP:
        return f_type, 0

    data = await reader.readexactly(2)
    return f_type, unpack_i16(data)


async def read_list_begin(reader):
    data = await reader.readexactly(1)
    e_type = unpack_i8(data)
    data = await reader.readexactly(4)
    sz = unpack_i32(data)
    return e_type, sz


async def read_map_begin(reader):
    k = await reader.readexactly(1)
    v = await reader.readexactly(1)
    k_type, v_type = unpack_i8(k), unpack_i8(v)
    data = await reader.readexactly(4)
    sz = unpack_i32(data)
    return k_type, v_type, sz


async def read_val(reader, ttype, spec=None, decode_response=True):
    if ttype == TType.BOOL:
        data = await reader.readexactly(1)
        return bool(unpack_i8(data))

    elif ttype == TType.BYTE:
        data = await reader.readexactly(1)
        return unpack_i8(data)

    elif ttype == TType.I16:
        data = await reader.readexactly(2)
        return unpack_i16(data)

    elif ttype == TType.I32:
        data = await reader.readexactly(4)
        return unpack_i32(data)

    elif ttype == TType.I64:
        data = await reader.readexactly(8)
        return unpack_i64(data)

    elif ttype == TType.DOUBLE:
        data = await reader.readexactly(8)
        return unpack_double(data)

    elif ttype == TType.STRING:
        data = await reader.readexactly(4)
        sz = unpack_i32(data)
        byte_payload = await reader.readexactly(sz)

        # Since we cannot tell if we're getting STRING or BINARY
        # if not asked not to decode, try both
        if decode_response:
            try:
                return byte_payload.decode("utf-8")
            except UnicodeDecodeError:
                pass
        return byte_payload

    elif ttype == TType.SET or ttype == TType.LIST:
        if isinstance(spec, tuple):
            v_type, v_spec = spec[0], spec[1]
        else:
            v_type, v_spec = spec, None

        result = []
        r_type, sz = await read_list_begin(reader)
        # the v_type is useless here since we already get it from spec
        if r_type != v_type:
            for _ in range(sz):
                await skip(reader, r_type)
            return []

        for i in range(sz):
            data = await read_val(reader, v_type, v_spec, decode_response)
            result.append(data)
        return result

    elif ttype == TType.MAP:
        if isinstance(spec[0], int):
            k_type = spec[0]
            k_spec = None
        else:
            k_type, k_spec = spec[0]

        if isinstance(spec[1], int):
            v_type = spec[1]
            v_spec = None
        else:
            v_type, v_spec = spec[1]

        result = {}
        sk_type, sv_type, sz = await read_map_begin(reader)
        if sk_type != k_type or sv_type != v_type:
            for _ in range(sz):
                await skip(reader, sk_type)
                await skip(reader, sv_type)
            return {}

        for i in range(sz):
            k_val = await read_val(reader, k_type, k_spec, decode_response)
            v_val = await read_val(reader, v_type, v_spec, decode_response)
            result[k_val] = v_val

        return result

    elif ttype == TType.STRUCT:
        obj = spec()
        await read_struct(reader, obj, decode_response)
        return obj


async def read_struct(reader, obj, decode_response=True):
    while True:
        f_type, fid = await read_field_begin(reader)
        if f_type == TType.STOP:
            break

        if fid not in obj.thrift_spec:
            await skip(reader, f_type)
            continue

        if len(obj.thrift_spec[fid]) == 3:
            sf_type, f_name, f_req = obj.thrift_spec[fid]
            f_container_spec = None
        else:
            sf_type, f_name, f_container_spec, f_req = obj.thrift_spec[fid]

        # it really should equal here. but since we already wasted
        # space storing the duplicate info, let's check it.
        if f_type != sf_type:
            await skip(reader, f_type)
            continue

        data = await read_val(reader, f_type, f_container_spec, decode_response)
        setattr(obj, f_name, data)


async def skip(reader, ftype):
    if ftype == TType.BOOL or ftype == TType.BYTE:
        await reader.readexactly(1)

    elif ftype == TType.I16:
        await reader.readexactly(2)

    elif ftype == TType.I32:
        await reader.readexactly(4)

    elif ftype == TType.I64:
        await reader.readexactly(8)

    elif ftype == TType.DOUBLE:
        await reader.readexactly(8)

    elif ftype == TType.STRING:
        await reader.readexactly(unpack_i32(reader.readexactly(4)))

    elif ftype == TType.SET or ftype == TType.LIST:
        v_type, sz = await read_list_begin(reader)
        for i in range(sz):
            await skip(reader, v_type)

    elif ftype == TType.MAP:
        k_type, v_type, sz = read_map_begin(reader)
        for i in range(sz):
            await skip(reader, k_type)
            await skip(reader, v_type)

    elif ftype == TType.STRUCT:
        while True:
            f_type, fid = await read_field_begin(reader)
            if f_type == TType.STOP:
                break
            await skip(reader, f_type)


class TFramedTransport:
    """Implement the Twisted framed transport protocol.

    Since most async servers use this including the python2 twisted
    bindings, this is likely of interest.
    """
    def __init__(self, base):
        self.__base = base
        self.__read_buffer = BytesIO()
        self.__write_buffer = BytesIO()

    def read(self, n=-1):
        if len(self.__read_buffer.getvalue()) == 0:
            self.read_frame()
        return self.__read_buffer.read(n)

    async def read_frame(self):
        buff = await self.__base.readexactly(4)
        sz, = unpack('!i', buff)
        self.__read_buffer = BytesIO(await self.__base.readexactly(sz))

    async def readexactly(self, n):
        now, end = self.__read_buffer.tell(), self.__read_buffer.seek(0, 2)
        remaining = end - now
        self.__read_buffer.seek(now)

        if 0 < remaining < n:
            raise IOError("Tried to read invalid amount from framed transport")
        elif remaining == 0:
            await self.read_frame()

        return self.__read_buffer.read(n)

    def write(self, val):
        self.__write_buffer.write(val)

    async def drain(self):
        wout = self.__write_buffer.getvalue()
        wsz = len(wout)
        self.__write_buffer = BytesIO()
        buf = pack("!i", wsz) + wout
        self.__base.write(buf)
        await self.__base.drain()

    def at_eof(self):
        return self.__base.at_eof()

    def close(self):
        return self.__base.close()


class TProtocol:
    """
    Base class for thrift protocols, subclass should implement some of the protocol methods,
    currently we only have :class:`TBinaryProtocol` implemented for you.
    """

    def __init__(
            self, trans, strict_read=True, strict_write=True, decode_response=True
    ):
        self.trans = trans
        self.strict_read = strict_read
        self.strict_write = strict_write
        self.decode_response = decode_response

    def skip(self, ttype):
        pass

    async def read_message_begin(self):
        pass

    async def read_message_end(self):
        pass

    def write_message_begin(self, name, ttype, seqid):
        pass

    def write_message_end(self):
        pass

    async def read_struct(self, obj):
        pass

    def write_struct(self, obj):
        pass


class TBinaryProtocol(TProtocol):
    """Binary implementation of the Thrift protocol driver."""

    def skip(self, ttype):
        skip(self.trans, ttype)

    async def read_message_begin(self):
        api, ttype, seqid = await read_message_begin(
            self.trans, strict=self.strict_read
        )
        return api, ttype, seqid

    def write_message_begin(self, name, ttype, seqid):
        write_message_begin(self.trans, name, ttype, seqid, strict=self.strict_write)

    async def read_struct(self, obj):
        data = await read_struct(self.trans, obj, self.decode_response)
        return data

    def write_struct(self, obj):
        write_val(self.trans, TType.STRUCT, obj)
