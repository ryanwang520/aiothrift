import asyncio
import aiothrift

pingpong_thrift = aiothrift.load("pingpong.thrift", module_name="pingpong_thrift")


async def create_connection():
    conn = await aiothrift.create_connection(
        pingpong_thrift.PingPong, ("127.0.0.1", 6000), timeout=10
    )
    print(await conn.ping())
    print(await conn.add(5, 6))
    conn.close()


asyncio.run(create_connection())
