import asyncio

import async_timeout

from .log import logger
from .processor import TProcessor
from .protocol import TBinaryProtocol, TFramedTransport


class Server:
    def __init__(self, processor, protocol_cls=TBinaryProtocol, timeout=None, framed=False):
        self.processor = processor
        self.protocol_cls = protocol_cls
        self.timeout = timeout
        self.framed = framed

    async def __call__(self, reader, writer):
        client_addr = writer.get_extra_info('peername')
        logger.debug(f"client {client_addr} has opened a connection")

        if self.framed:
            reader = TFramedTransport(reader)
            writer = TFramedTransport(writer)

        iproto = self.protocol_cls(reader)
        oproto = self.protocol_cls(writer)

        try:
            while not reader.at_eof():
                with async_timeout.timeout(self.timeout):
                    await self.processor.process(iproto, oproto)
        except ConnectionError:
            logger.debug(f"client {client_addr} has closed the connection (ConnectionError)")
        except asyncio.TimeoutError:
            logger.debug(f"timeout when processing the client request from {client_addr}")
        except asyncio.IncompleteReadError:
            logger.debug(f"client {client_addr} has closed the connection")
        except Exception:
            # app exception
            logger.exception("unhandled app exception")
        finally:
            writer.close()


async def create_server(
    service,
    handler,
    address=("127.0.0.1", 6000),
    protocol_cls=TBinaryProtocol,
    timeout=None,
    framed=False,
    **kw,
):
    """ create a thrift server.
    This function is a :ref:`coroutine <coroutine>`.

    :param service: thrift Service
    :param handler: a dispatcher object which is a namespace for all thrift api functions.
    :param address:  (host, port) tuple, default is ('127.0.0.1', 6000)
    :param protocol_cls: thrift protocol class, default is :class:`TBinaryProtocol`
    :param timeout: server side timeout, default is None
    :param kw: params relaied to asyncio.start_server
    :return: a :class:`Server` object which can be used to stop the service
    """
    host, port = address
    processor = TProcessor(service, handler)
    server = await asyncio.start_server(
        Server(processor, protocol_cls, timeout=timeout, framed=framed), host, port, **kw
    )
    return server
