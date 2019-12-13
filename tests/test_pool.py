import asyncio
import pytest

from aiothrift import (
    ThriftPool,
    PoolClosedError,
    ConnectionClosedError,
)
from aiothrift.util import async_task


def _assert_defaults(pool):
    assert isinstance(pool, ThriftPool)
    assert pool.minsize == 1
    assert pool.maxsize == 10
    assert pool.size == 1
    assert pool.freesize == 1


def test_connect(pool):
    _assert_defaults(pool)


@pytest.mark.asyncio
async def test_clear(pool):
    _assert_defaults(pool)

    await pool.clear()
    assert pool.freesize == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("minsize", [None, -100, 0.0, 100])
async def test_minsize(test_thrift, minsize, create_pool, server):
    with pytest.raises(AssertionError):
        await create_pool(test_thrift.Test, server.address, minsize=minsize, maxsize=10)


@pytest.mark.asyncio
@pytest.mark.parametrize("maxsize", [None, -100, 0.0, 1])
async def test_maxsize(test_thrift, maxsize, create_pool, server):
    with pytest.raises(AssertionError):
        await create_pool(test_thrift.Test, server.address, minsize=2, maxsize=maxsize)


def test_no_yield_from(pool):
    with pytest.raises(AttributeError):
        with pool:
            pass  # pragma: no cover


@pytest.mark.asyncio
async def test_rpc(test_thrift, create_pool, server):
    pool = await create_pool(test_thrift.Test, server.address, minsize=10)

    async with pool as conn:
        result = await conn.ping()
        assert result == "pong"
        result = await conn.add(100, 200)
        assert result == 300
        assert pool.size == 10
        assert pool.freesize == 9
    assert pool.size == 10
    assert pool.freesize == 10


@pytest.mark.asyncio
async def test_create_nested(test_thrift, create_pool, server):
    pool = await create_pool(test_thrift.Test, server.address, minsize=1)
    assert pool.size == 1
    assert pool.freesize == 1

    async with pool:
        assert pool.size == 1
        assert pool.freesize == 0

        with pytest.raises(RuntimeError):
            async with pool:
                pass

    assert pool.size == 1
    assert pool.freesize == 1


@pytest.mark.asyncio
async def test_create_constraints(test_thrift, create_pool, server):
    pool = await create_pool(test_thrift.Test, server.address, minsize=1, maxsize=1)
    assert pool.size == 1
    assert pool.freesize == 1

    async with pool:
        assert pool.size == 1
        assert pool.freesize == 0

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(pool.acquire(), timeout=0.2)


@pytest.mark.asyncio
async def test_create_no_minsize(test_thrift, create_pool, server):
    pool = await create_pool(test_thrift.Test, server.address, minsize=0, maxsize=1)
    assert pool.size == 0
    assert pool.freesize == 0

    async with pool:
        assert pool.size == 1
        assert pool.freesize == 0

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(pool.acquire(), timeout=0.2)
    assert pool.size == 1
    assert pool.freesize == 1


@pytest.mark.asyncio
async def test_release_closed(test_thrift, create_pool, server):
    pool = await create_pool(test_thrift.Test, server.address, minsize=1)
    assert pool.size == 1
    assert pool.freesize == 1

    async with pool as conn:
        conn.close()
    assert pool.size == 0
    assert pool.freesize == 0


@pytest.mark.asyncio
async def test_release_bad_connection(
    test_thrift, create_pool, create_connection, server
):
    pool = await create_pool(test_thrift.Test, server.address)
    conn = await pool.acquire()
    other_conn = await create_connection(test_thrift.Test, server.address)
    with pytest.raises(AssertionError):
        pool.release(other_conn)

    pool.release(conn)
    other_conn.close()


@pytest.mark.asyncio
async def test_pool_size_growth(test_thrift, create_pool, server):
    pool = await create_pool(test_thrift.Test, server.address, minsize=1, maxsize=1)

    done = set()
    tasks = []

    async def task1(i):
        async with pool:
            assert pool.size <= pool.maxsize
            assert pool.freesize == 0
            await asyncio.sleep(0.2)
            done.add(i)

    async def task2():
        async with pool:
            assert pool.size <= pool.maxsize
            assert pool.freesize >= 0
            assert done == {0, 1}

    for _ in range(2):
        tasks.append(async_task(task1(_)))
    tasks.append(async_task(task2()))
    await asyncio.gather(*tasks)


@pytest.mark.asyncio
async def test_pool_with_closed_connections(test_thrift, create_pool, server):
    pool = await create_pool(test_thrift.Test, server.address, minsize=1, maxsize=2)
    assert 1 == pool.freesize
    conn1 = pool._pool[0]
    conn1.close()
    assert conn1.closed is True
    assert 1 == pool.freesize
    async with pool as conn2:
        assert conn2.closed is False
        assert conn1 is not conn2


@pytest.mark.asyncio
async def test_pool_close(test_thrift, create_pool, server):
    pool = await create_pool(test_thrift.Test, server.address)

    assert pool.closed is False

    async with pool as cli:
        assert (await cli.ping()) == "pong"

    pool.close()
    await pool.wait_closed()
    assert pool.closed is True

    with pytest.raises(PoolClosedError):
        async with pool as cli:
            assert (await cli.ping()) == "PONG"


@pytest.mark.asyncio
async def test_pool_close__used(test_thrift, create_pool, server):
    pool = await create_pool(test_thrift.Test, server.address)

    assert pool.closed is False

    async with pool as cli:
        pool.close()
        await pool.wait_closed()
        assert pool.closed is True

        with pytest.raises(ConnectionClosedError):
            await cli.ping()
