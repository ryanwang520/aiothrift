import asyncio

import async_timeout

from .log import logger
from .processor import TProcessor
from .protocol import TBinaryProtocol


class Server:
    def __init__(self, processor, protocol_cls=TBinaryProtocol, timeout=None):
        self.processor = processor
        self.protocol_cls = protocol_cls
        self.timeout = timeout

    @asyncio.coroutine
    def __call__(self, reader, writer):
        iproto = self.protocol_cls(reader)
        oproto = self.protocol_cls(writer)
        while not reader.at_eof():
            try:
                with async_timeout.timeout(self.timeout):
                    yield from self.processor.process(iproto, oproto)
            except ConnectionError:
                logger.debug('client has closed the connection')
                writer.close()
            except asyncio.TimeoutError:
                logger.debug('timeout when processing the client request')
                writer.close()
            except asyncio.IncompleteReadError:
                logger.debug('client has closed the connection')
                writer.close()
            except Exception:
                # app exception
                logger.exception('unhandled app exception')
                writer.close()
        writer.close()


@asyncio.coroutine
def create_server(service, handler,
                  address=('127.0.0.1', 6000),
                  loop=None,
                  protocol_cls=TBinaryProtocol,
                  timeout=None
                  ):
    """ create a thrift server.
    This function is a :ref:`coroutine <coroutine>`.

    :param service: thrift Service
    :param handler: a dispatcher object which is a namespace for all thrift api functions.
    :param address:  (host, port) tuple, default is ('127.0.0.1', 6000)
    :param loop: :class:`Eventloop <asyncio.AbstractEventLoop>` instance
    :param protocol_cls: thrift protocol class, default is :class:`TBinaryProtocol`
    :param timeout: server side timeout, default is None
    :return: a :class:`Server` object which can be used to stop the service
    """
    host, port = address
    processor = TProcessor(service, handler)
    if loop is None:
        loop = asyncio.get_event_loop()
    server = yield from asyncio.start_server(
        Server(processor, protocol_cls, timeout=timeout), host, port, loop=loop)
    return server
