.. _quickstart:

Quickstart
==========

This page gives you a good introduction to aiothrift. It assumes you already have aiothrift installed.

A Minimal Application
---------------------

At first you should have a thrift file which defines at least one service. Go to
create a thrift file named :file:`pingpong.thrift`::

    service PingPong {
        string ping(),
        i32 add(1:i32 a, 2:i32 b),
    }


Now you can fire an asyncio thrift server easily::

    import asyncio
    import aiothrift

    pingpong_thrift = aiothrift.load('pingpong.thrift', module_name='pingpong_thrift')

    class Dispatcher:
        def ping(self):
            return "pong"

        async def add(self, a, b):
            await asyncio.sleep(1)
            return a + b

    async def main():
      server = await aiothrift.create_server(pingpong_thrift.PingPong, Dispatcher()))
      async with server:
          await server.serve_forever()

    asyncio.run(main())

let's have a look at what the code above does.

1. First we parse a thrift file to a valid python module,
thanks for the great job done by `thriftpy2`, we don't have to generate thrift python sdk files manually.

2. We create a `Dispatcher` class as the namespace for all thrift rpc functions. Here we define a `ping` method
which corresponds to the `ping` function defined in ``pingpong.thrift``. You may notice that the `add` method is
actually a :ref:`coroutine <coroutine>` but a normal one. if you define the rpc function as a :ref:`coroutine <coroutine>`,
it would scheduled by our thrift server and send the result back to client after the :ref:`coroutine <coroutine>` task is completed.

3. We then create the server by using :func:`~aiothrift.create_server` function, and it returns a :ref:`coroutine <coroutine>`
instance which can be scheduled by the event loop later.

4. Lastly we call ``asyncio.run(main())`` to run the event loop to schedule the server task.

Just save it as :file:`server.py` and then you can start the thrift server::

    $ python3 server.py


It will listening at `localhost:6000` by default.

Now you'd like to visit the thrift server through a thrift client::

    import asyncio
    import aiothrift

    pingpong_thrift = aiothrift.load('pingpong.thrift', module_name='pingpong_thrift')

    async def go():
        client = await aiothrift.create_pool(pingpong_thrift.PingPong)
        print(await client.ping())
        print(await client.add(5, 6))
        client.close()
        await client.wait_closed()

    asyncio.run(go())


 Save it as :file:`client.py`, and run the client by::

    $ python client.py
     * pong


That's all you need to make a minimal thrift application on both the server and client side, I hope you will enjoy it.
