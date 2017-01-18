import asyncio
import sys
import thriftpy

from aiothrift.server import create_server

pingpong_thrift = thriftpy.load('tests/test.thrift', module_name='test_thrift')


@asyncio.coroutine
def _add(a, b):
    yield from asyncio.sleep(0)
    return a + b


class Dispatcher:
    def ping(self):
        return "pong"

    @asyncio.coroutine
    def add(self, a, b):
        result = yield from _add(a, b)
        return result

    def address(self, name):
        return 'address ' + name


loop = asyncio.get_event_loop()

server = loop.run_until_complete(
    create_server(pingpong_thrift.Test, Dispatcher(), ('127.0.0.1', 6000), loop=loop, timeout=10))

print('server is listening on host {} and port {}'.format('127.0.0.1', 6000))
sys.stdout.flush()

try:
    loop.run_forever()
except KeyboardInterrupt:
    pass

server.close()
loop.run_until_complete(server.wait_closed())
loop.close()
