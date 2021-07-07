import asyncio
import functools
from collections.abc import Sequence

import async_timeout
from thriftpy2.thrift import TMessageType

from .protocol import TBinaryProtocol, TFramedTransport
from .util import args2kwargs
from .errors import ConnectionClosedError, ThriftAppError
from .log import logger


async def create_connection(
    service,
    address=("127.0.0.1", 6000),
    *,
    protocol_cls=TBinaryProtocol,
    timeout=None,
    framed=False,
    **kw,
):
    """Create a thrift connection.
    This function is a :ref:`coroutine <coroutine>`.

    Open a connection to the thrift server by address argument.

    :param service: a thrift service object
    :param address: a (host, port) tuple
    :param protocol_cls: protocol type, default is :class:`TBinaryProtocol`
    :param timeout: if timeout is a number, would raise `asyncio.TimeoutError`
        if one rpc call is longer than `timeout` or connection cannot be
        eatablishd within `timeout`, if timeout is a tuple of two number,
        each number is corresponding to tcp connection timeout and read timeout of each
        rpc call.
    :param kw: params related to asyncio.open_connection()
    :return: newly created :class:`ThriftConnection` instance.
    """
    connection_timeout, read_timeout = None, None
    if timeout:
        if isinstance(timeout, (float, int)):
            connection_timeout, read_timeout = timeout, timeout
        elif isinstance(timeout, Sequence):
            if len(timeout) != 2:
                raise ValueError("timeout should be a sequence of 2 number")
            connection_timeout, read_timeout = timeout
        else:
            raise ValueError("timeout must be a number or tuple of 2 number")

    host, port = address
    connection_future = asyncio.open_connection(host, port, **kw)
    reader, writer = await asyncio.wait_for(
        connection_future, timeout=connection_timeout
    )
    if framed:
        reader = TFramedTransport(reader)
        writer = TFramedTransport(writer)

    iprotocol = protocol_cls(reader)
    oprotocol = protocol_cls(writer)

    return ThriftConnection(
        service, iprot=iprotocol, oprot=oprotocol, address=address, timeout=read_timeout
    )


class ThriftConnection:
    """
    Thrift Connection.
    """

    def __init__(self, service, *, iprot, oprot, address, timeout=None):
        self.service = service
        self._reader = iprot.trans
        self._writer = oprot.trans
        self.timeout = timeout
        self.address = address
        self.closed = False
        self._oprot = oprot
        self._iprot = iprot
        self._seqid = 0
        self._init_rpc_apis()

    def _init_rpc_apis(self):
        """
        find out all apis defined in thrift service, and create corresponding
        method on the connection object, ignore it if some api name is conflicted with
        an existed attribute of the connection object, which you should call by using
        the :meth:`execute` method.
        """
        for api in self.service.thrift_services:
            if not hasattr(self, api):
                setattr(self, api, functools.partial(self.execute, api))
            else:
                logger.warning(
                    "api name {0} is conflicted with connection attribute "
                    '{0}, while you can still call this api by `execute("{0}")`'.format(
                        api
                    )
                )

    def __repr__(self):
        return "<ThriftConnection {} to>".format(self.address)

    async def execute(self, api, *args, **kwargs):
        """
        Execute a rpc call by api name. This is function is a :ref:`coroutine <coroutine>`.

        :param api: api name defined in thrift file
        :param args: positional arguments passed to api function
        :param kwargs:  keyword arguments passed to api function
        :return: result of this rpc call
        :raises: :class:`~asyncio.TimeoutError` if this task has exceeded the `timeout`
        :raises: :class:`ThriftAppError` if thrift response is an exception defined in thrift.
        :raises: :class:`ConnectionClosedError`: if server has closed this connection.
        """
        if self.closed:
            raise ConnectionClosedError("Connection closed")

        try:
            with async_timeout.timeout(self.timeout):
                kw = args2kwargs(
                    getattr(self.service, api + "_args").thrift_spec, *args
                )
                kwargs.update(kw)
                result_cls = getattr(self.service, api + "_result")

                self._seqid += 1
                self._oprot.write_message_begin(api, TMessageType.CALL, self._seqid)
                args = getattr(self.service, api + "_args")()
                for k, v in kwargs.items():
                    setattr(args, k, v)
                args.write(self._oprot)
                self._oprot.write_message_end()
                await self._oprot.trans.drain()
                if not getattr(result_cls, "oneway"):
                    result = await self._recv(api)
                    return result
        except asyncio.TimeoutError:
            self.close()
            raise
        except ConnectionError as e:
            self.close()
            logger.debug("connection error {}".format(str(e)))
            raise ConnectionClosedError("the server has closed this connection") from e
        except asyncio.IncompleteReadError as e:
            self.close()
            raise ConnectionClosedError("Server connection has closed") from e

    async def _recv(self, api):
        """
        A :ref:`coroutine <coroutine>` which receive response from the thrift server
        """
        fname, mtype, rseqid = await self._iprot.read_message_begin()
        if rseqid != self._seqid:
            # transport should be closed if bad seq happened
            self.close()
            raise ThriftAppError(
                ThriftAppError.BAD_SEQUENCE_ID,
                fname + " failed: out of sequence response",
            )

        if mtype == TMessageType.EXCEPTION:
            x = ThriftAppError()
            await self._iprot.read_struct(x)
            await self._iprot.read_message_end()
            raise x
        result = getattr(self.service, api + "_result")()
        await self._iprot.read_struct(result)
        await self._iprot.read_message_end()

        if hasattr(result, "success") and result.success is not None:
            return result.success

        # void api without throws
        if len(result.thrift_spec) == 0:
            return

        # check throws
        for k, v in result.__dict__.items():
            if k != "success" and v:
                raise v
        if hasattr(result, "success"):
            raise ThriftAppError(ThriftAppError.MISSING_RESULT)

    def close(self):
        self._writer.close()
        self.closed = True
