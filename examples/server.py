import asyncio
import thriftpy

from aiothrift.server import make_server

pingpong_thrift = thriftpy.load('pingpong.thrift', module_name='pingpong_thrift')


class Dispatcher:
    def ping(self):
        return "pong"

    async def add(self, a, b):
        await asyncio.sleep(2)
        return a + b


loop = asyncio.get_event_loop()

server = loop.run_until_complete(
    make_server(pingpong_thrift.PingPong, Dispatcher(), '127.0.0.1', 6000, loop=loop, timeout=10))

try:
    loop.run_forever()
except KeyboardInterrupt:
    pass

server.close()
loop.run_until_complete(server.wait_closed())
loop.close()
