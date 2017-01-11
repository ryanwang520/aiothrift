import asyncio

from thriftpy.transport import TTransportException

from aiothrift.processor import TProcessor
from aiothrift.protocol import TBinaryProtocol
from aiothrift.transport import TTransport


class Server:
    def __init__(self, processor, protocol_cls=TBinaryProtocol):
        self.processor = processor
        self.protocol_cls = protocol_cls

    @asyncio.coroutine
    def __call__(self, reader, writer):
        itransport = TTransport(reader)
        iproto = self.protocol_cls(itransport)
        oproto = self.protocol_cls(writer)
        while not reader.at_eof():
            try:
                yield from self.processor.process(iproto, oproto)
            except TTransportException:
                pass
            except Exception:
                pass
        writer.close()


@asyncio.coroutine
def make_server(service, handler,
                host="localhost", port=9090,
                loop=None,
                protocol_cls=TBinaryProtocol,
                ):
    processor = TProcessor(service, handler)
    if loop is None:
        loop = asyncio.get_event_loop()
    server = yield from asyncio.start_server(
        Server(processor, protocol_cls), host, port, loop=loop)
    return server
