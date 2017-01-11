import thriftpy
import asyncio
import aiothrift

pingpong_thrift = thriftpy.load('pingpong.thrift', module_name='pingpong_thrift')

loop = asyncio.get_event_loop()


async def go():
    conn = await aiothrift.create_connection(pingpong_thrift.PingPong, ('127.0.0.1', 6000), loop=loop, timeout=2)
    print(await conn.ping())
    print(await conn.add(5, 6))
    conn.close()


async def go_pool():
    pool = await aiothrift.create_pool(pingpong_thrift.PingPong, ('127.0.0.1', 6000), loop=loop, timeout=2)
    async with pool.get() as conn:
        print(await conn.ping())
        print(await conn.add(5, 6))
    pool.close()


loop.run_until_complete(go())
tasks = []
for i in range(10):
    tasks.append(asyncio.ensure_future(go_pool()))
loop.run_until_complete(asyncio.gather(*tasks))
loop.close()
