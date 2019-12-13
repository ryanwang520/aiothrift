import aiothrift
import asyncio

pingpong_thrift = aiothrift.load("pingpong.thrift", module_name="pingpong_thrift")


async def create_pool():
    return await aiothrift.create_pool(
        pingpong_thrift.PingPong, ("127.0.0.1", 6000), timeout=3
    )


async def main(pool):
    async with pool as conn:
        print(await conn.ping())
        # async with pool as conn:
        print(await conn.add(5, 6))
        print(await conn.ping())


if __name__ == "__main__":

    async def f():
        pool = await create_pool()
        await asyncio.gather(main(pool), main(pool))

    asyncio.run(f())
