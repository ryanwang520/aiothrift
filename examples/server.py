import asyncio

import aiothrift

pingpong_thrift = aiothrift.load("pingpong.thrift", module_name="pingpong_thrift")


class Dispatcher:
    def ping(self):
        return "pong"

    async def add(self, a, b):
        await asyncio.sleep(2)
        return a + b


async def main():
    server = await aiothrift.create_server(pingpong_thrift.PingPong, Dispatcher())
    async with server:
        print("server is listening on host {} and port {}".format("127.0.0.1", 6000))
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
