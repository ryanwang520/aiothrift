import thriftpy
import asyncio
import aiothrift

pingpong_thrift = thriftpy.load('pingpong.thrift', module_name='pingpong_thrift')

loop = asyncio.get_event_loop()


async def create_connection():
    conn = await aiothrift.create_connection(pingpong_thrift.PingPong, ('127.0.0.1', 6000), loop=loop, timeout=10)
    print(await conn.ping())
    print(await conn.add(5, 6))
    conn.close()


loop.run_until_complete(create_connection())

loop.close()
