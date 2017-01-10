work in progress, there would be a first viable version soon.
======================

For the impatient guys, below is a simple demo of usage.


create 'pingpong.thrift' file:

::

    service PingPong {
        string ping(),
        i64 add(1:i32 a, 2:i64 b),
    }

Then we can make a server:

.. code:: python

    import asyncio
    import thriftpy
    from aiothrift.server import make_server

    pingpong_thrift = thriftpy.load('pingpong.thrift', module_name='pingpong_thrift')

    class Dispatcher(object):
        def ping(self):
            return "pong"

        async def add(self, a, b):
            await asyncio.sleep(1)
            return a + b

    loop = asyncio.get_event_loop()
    server = loop.run_until_complete(
        make_server(pingpong_thrift.PingPong, Dispatcher(), '127.0.0.1', 6000, loop=loop))
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()

And a client:

.. code:: python

    import thriftpy
    import asyncio
    import aiothrift

    loop = asyncio.get_event_loop()
    pingpong_thrift = thriftpy.load('pingpong.thrift', module_name='pingpong_thrift')

    async def go():
        conn = await aiothrift.create_connection(pingpong_thrift.PingPong, ('127.0.0.1', 6000), loop=loop, timeout=2)
        print(await conn.ping())
        print(await conn.add(5, 6))
        conn.close()

    loop.run_until_complete(go())


pretty cool.
