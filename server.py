import asyncio

import thriftpy

from aiothrift.server import make_server

pingpong_thrift = thriftpy.load('pingpong.thrift', module_name='pingpong_thrift')


# from thriftpy.rpc import make_server


class Dispatcher(object):
    def ping(self):
        return "pong"

    async def add(self, a, b):
        await asyncio.sleep(1)
        return a + b


loop = asyncio.get_event_loop()

server = make_server(pingpong_thrift.PingPong, Dispatcher(), '127.0.0.1', 6000, loop=loop)

print(f'Serving on {server.sockets[0].getsockname()}')
try:
    loop.run_forever()
except KeyboardInterrupt:
    pass
server.close()
loop.run_until_complete(server.wait_closed())
loop.close()
