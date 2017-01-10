import asyncio

from thriftpy.transport import TTransportException

from aiothrift.processor import TProcessor
from aiothrift.protocol import TBinaryProtocolFactory


class Server:
    def __init__(self, processor, iproto_factory, oproto_factory):
        self.processor = processor
        self.iproto_factory = iproto_factory
        self.oproto_factory = oproto_factory

    @asyncio.coroutine
    def __call__(self, reader, writer):
        iproto = self.iproto_factory.get_protocol(reader)
        oproto = self.iproto_factory.get_protocol(writer)
        while not reader.at_eof():
            try:
                yield from self.processor.process(iproto, oproto)
            except TTransportException:
                pass
            except Exception:
                pass
        writer.close()


def make_server(service, handler,
                host="localhost", port=9090,
                loop=None,
                iproto_factory=TBinaryProtocolFactory(),
                oproto_factory=None,
                ):
    processor = TProcessor(service, handler)
    if loop is None:
        loop = asyncio.get_event_loop()
    if oproto_factory is None:
        oproto_factory = iproto_factory
    coro = asyncio.start_server(Server(processor, iproto_factory, oproto_factory), host, port, loop=loop)
    return loop.run_until_complete(coro)

