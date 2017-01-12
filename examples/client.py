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


async def go_pool():
    pool = await aiothrift.create_pool(pingpong_thrift.PingPong, ('127.0.0.1', 6000), loop=loop, timeout=1)
    try:
        async with pool.get() as conn:
            print(await conn.add(5, 6))
            print(await conn.ping())
    except asyncio.TimeoutError:
        pass

    async with pool.get() as conn:
        print(await conn.ping())
    pool.close()
    await pool.wait_closed()


loop.run_until_complete(go())
loop.run_until_complete(go_pool())
loop.close()
