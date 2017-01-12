import sys
import asyncio
import async_timeout

import functools

from thriftpy.thrift import TMessageType, TApplicationException
from thriftpy.transport import TTransportException

from aiothrift.transport import TTransport
from .protocol import TBinaryProtocol
from .util import args2kwargs
from .errors import ConnectionClosedError
from .log import logger


@asyncio.coroutine
def create_connection(service, address, *, protocol_cls=TBinaryProtocol,
                      timeout=None, loop=None):
    host, port = address
    reader, writer = yield from asyncio.open_connection(
        host, port, loop=loop)
    sock = writer.transport.get_extra_info('socket')
    address = sock.getpeername()
    address = tuple(address[:2])
    itransport = TTransport(reader)
    iprotocol = protocol_cls(itransport)
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
        self._oprot = oprot
        self._iprot = iprot
        self._seqid = 0
        self._init_rpc_apis()

    def _init_rpc_apis(self):
        for api in self.service.thrift_services:
            setattr(self, api, functools.partial(self._send_call, api))

    @asyncio.coroutine
    def _send_call(self, api, *args, **kwargs):
        if self.closed:
            raise ConnectionClosedError('Connection closed')

        try:
            with async_timeout.timeout(self.timeout):
                _kw = args2kwargs(getattr(self.service, api + "_args").thrift_spec,
                                  *args)
                kwargs.update(_kw)
                result_cls = getattr(self.service, api + "_result")

                self._seqid += 1
                if self._seqid == sys.maxsize:
                    self._seqid = 0

                self._oprot.write_message_begin(api, TMessageType.CALL, self._seqid)
                args = getattr(self.service, api + '_args')()
                for k, v in kwargs.items():
                    setattr(args, k, v)
                args.write(self._oprot)
                self._oprot.write_message_end()
                try:
                    yield from self._oprot.trans.drain()
                except ConnectionError as e:
                    logger.debug('connection error {}'.format(str(e)))
                    raise ConnectionClosedError('the server has closed this connection')

                # writer.write
                # wait result only if non-oneway
                if not getattr(result_cls, "oneway"):
                    try:
                        result = yield from self._recv(api)
                    except TTransportException as e:
                        # handle EOF
                        if e.type == TTransportException.END_OF_FILE:
                            self.close()
                        raise
                    return result
        except asyncio.TimeoutError:
            self.close()
            raise

    @asyncio.coroutine
    def _recv(self, api):
        fname, mtype, rseqid = yield from self._iprot.read_message_begin()
        if rseqid != self._seqid:
            # transport should be closed if bad seq happened
            self.close()
            raise TApplicationException(TApplicationException.BAD_SEQUENCE_ID,
                                        fname + ' failed: out of sequence response')

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
