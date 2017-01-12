import thriftpy
import asyncio
import aiothrift

pingpong_thrift = thriftpy.load('pingpong.thrift', module_name='pingpong_thrift')

loop = asyncio.get_event_loop()


async def go():
    conn = await aiothrift.create_connection(pingpong_thrift.PingPong, ('127.0.0.1', 6000), loop=loop, timeout=10)
    print(await conn.ping())
    print(await conn.add(5, 6))
    conn.close()


async def create_pool():
    return await aiothrift.create_pool(pingpong_thrift.PingPong, ('127.0.0.1', 6000), loop=loop, timeout=1)


async def run_pool(pool):
    try:
        async with pool.get() as conn:
            print(await conn.add(5, 6))
            print(await conn.ping())
    except asyncio.TimeoutError:
        pass

    async with pool.get() as conn:
        print(await conn.ping())


loop.run_until_complete(go())
pool = loop.run_until_complete(create_pool())
tasks = []
for i in range(10):
    tasks.append(asyncio.ensure_future(run_pool(pool)))

loop.run_until_complete(asyncio.gather(*tasks))
pool.close()
loop.run_until_complete(pool.wait_closed())
loop.close()
