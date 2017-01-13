aiothrift
=========

Asyncio implementation for thrift protocol, which is heavily based on thriftpy_.

.. image:: http://img.shields.io/travis/moonshadow/aiothrift/master.svg?style=flat
   :target: https://travis-ci.org/moonshadow/aiothrift

This project is still in early develop state and thus is not recommended for production usage.

Documentation: https://aiothrift.readthedocs.org/

Installation
------------

::

    $ pip install aiothrift


Usage example
-------------

Thrift file
^^^^^^^^^^^

::

    service PingPong {
        string ping(),
        i64 add(1:i32 a, 2:i64 b),
    }


Server
^^^^^^

.. code:: python

    import asyncio
    import thriftpy
    from aiothrift.server import make_server

    pingpong_thrift = thriftpy.load('pingpong.thrift', module_name='pingpong_thrift')

    class Dispatcher:
        def ping(self):
            return "pong"

        async def add(self, a, b):
            await asyncio.sleep(1)
            return a + b

    loop = asyncio.get_event_loop()
    server = loop.run_until_complete(
        make_server(pingpong_thrift.PingPong, Dispatcher(), ('127.0.0.1', 6000), loop=loop))
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()

Client
^^^^^^

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
    loop.close()

Or use ConnectionPool
^^^^^^^^^^^^^^^^^^^^^

.. code:: python

    import thriftpy
    import asyncio
    import aiothrift

    loop = asyncio.get_event_loop()
    pingpong_thrift = thriftpy.load('pingpong.thrift', module_name='pingpong_thrift')

    async def go():
        pool = await aiothrift.create_pool(pingpong_thrift.PingPong,
            ('127.0.0.1', 6000), loop=loop, timeout=2)
        async with pool.get() as conn:
            print(await conn.ping())
            print(await conn.add(5, 6))
        pool.close()
        yield from pool.wait_closed()

    loop.run_until_complete(go())
    loop.close()


It's just that simple to begin with ``aiothrift``, and you are not forced to use ``aiothrift`` on both server and client side.
So if you already have a normal thrift server setup, feel free to create an async thrift client to communicate with that server.

Requirements
------------

- Python >= 3.4.2
- async-timeout_
- thriftpy_

.. _async-timeout: https://pypi.python.org/pypi/async_timeout
.. _thriftpy: https://thriftpy.readthedocs.org/en/latest/


LICENSE
-------

``aiothrift`` is offered under the MIT license.
