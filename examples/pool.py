import aiothrift
import asyncio

pingpong_thrift = aiothrift.load("pingpong.thrift", module_name="pingpong_thrift")


async def create_pool():
    return await aiothrift.create_pool(
        pingpong_thrift.PingPong, ("127.0.0.1", 6000), timeout=3
    )


async def main(pool):
    print(await pool.ping())
    print(await pool.add(5, 6))
    print(await pool.ping())


if __name__ == "__main__":

    async def f():
        pool = await create_pool()
        await asyncio.gather(main(pool), main(pool))
        pool.close()
        await pool.wait_closed()

    asyncio.run(f())
