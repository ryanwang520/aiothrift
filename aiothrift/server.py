import asyncio

from thriftpy.transport import TTransportException
import async_timeout

from .log import logger
from .processor import TProcessor
from .protocol import TBinaryProtocol
from .transport import TTransport


class Server:
    def __init__(self, processor, protocol_cls=TBinaryProtocol, timeout=None):
        self.processor = processor
        self.protocol_cls = protocol_cls
        self.timeout = timeout

    @asyncio.coroutine
    def __call__(self, reader, writer):
        itransport = TTransport(reader)
        iproto = self.protocol_cls(itransport)
        oproto = self.protocol_cls(writer)
        while not reader.at_eof():
            try:
                with async_timeout.timeout(self.timeout):
                    yield from self.processor.process(iproto, oproto)
            except TTransportException:
                logger.debug('transport exception')
                writer.close()
            except asyncio.TimeoutError:
                logger.debug('timeout when processing the client request')
                writer.close()
            except Exception:
                # app exception
                logger.exception('un handled app exception')
                writer.close()
        writer.close()


@asyncio.coroutine
def make_server(service, handler,
                host="localhost", port=9090,
                loop=None,
                protocol_cls=TBinaryProtocol,
                timeout=None
                ):
    processor = TProcessor(service, handler)
    if loop is None:
        loop = asyncio.get_event_loop()
    server = yield from asyncio.start_server(
        Server(processor, protocol_cls, timeout=timeout), host, port, loop=loop)
    return server
