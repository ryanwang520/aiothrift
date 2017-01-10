import asyncio
import async_timeout

import functools

from aioredis import ConnectionClosedError
from thriftpy.thrift import TMessageType, TApplicationException

from aiothrift.protocol import TBinaryProtocol
from aiothrift.util import args2kwargs


@asyncio.coroutine
def create_connection(service, address, *, protocol_cls=TBinaryProtocol,
                      timeout, loop=None):
    host, port = address
    reader, writer = yield from asyncio.open_connection(
        host, port, loop=loop)
    sock = writer.transport.get_extra_info('socket')
    address = sock.getpeername()
    address = tuple(address[:2])
    iprotocol = protocol_cls(reader)
    oprotocol = protocol_cls(writer)

    return ThriftConnection(service, iprot=iprotocol, oprot=oprotocol,
                            address=address, loop=loop, timeout=timeout)


class ThriftConnection:
    def __init__(self, service, *, iprot, oprot, address, loop=None, timeout=None):
        self.service = service
        self._reader = iprot.trans
        self._writer = oprot.trans
        self.loop = loop
        self.timeout = timeout
        self.address = address
        self.closed = False
        self._init_rpc_apis()
        self._oprot = oprot
        self._iprot = iprot
        self._seqid = 0

    def _init_rpc_apis(self):
        for api in self.service.thrift_services:
            setattr(self, api, functools.partial(self._send_call, api))

    @asyncio.coroutine
    def _send_call(self, api, *args, **kwargs):
        with async_timeout.timeout(self.timeout):
            if self._reader is None or self._reader.at_eof():
                raise ConnectionClosedError('Connection closed')

            _kw = args2kwargs(getattr(self.service, api + "_args").thrift_spec,
                              *args)
            kwargs.update(_kw)
            result_cls = getattr(self.service, api + "_result")

            self._oprot.write_message_begin(api, TMessageType.CALL, self._seqid)
            args = getattr(self.service, api + '_args')()
            for k, v in kwargs.items():
                setattr(args, k, v)
            args.write(self._oprot)
            self._oprot.write_message_end()
            yield from self._oprot.trans.drain()

            # writer.write
            # wait result only if non-oneway
            if not getattr(result_cls, "oneway"):
                result = yield from self._recv(api)
                return result

    @asyncio.coroutine
    def _recv(self, api):
        fname, mtype, rseqid = yield from self._iprot.read_message_begin()
        if mtype == TMessageType.EXCEPTION:
            x = TApplicationException()
            yield from self._iprot.read_struct(x)
            yield from self._iprot.read_message_end()
            raise x
        result = getattr(self.service, api + '_result')()
        yield from self._iprot.read_struct(result)
        yield from self._iprot.read_message_end()

        if hasattr(result, "success") and result.success is not None:
            return result.success

        # void api without throws
        if len(result.thrift_spec) == 0:
            return

        # check throws
        for k, v in result.__dict__.items():
            if k != "success" and v:
                raise v
        if hasattr(result, 'success'):
            raise TApplicationException(TApplicationException.MISSING_RESULT)

    def close(self):
        self._writer.close()
        self.closed = True
