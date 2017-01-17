import asyncio
import pytest

from aiothrift import (
    ThriftPool,
    PoolClosedError,
    ConnectionClosedError,
)


def _assert_defaults(pool):
    assert isinstance(pool, ThriftPool)
    assert pool.minsize == 1
    assert pool.maxsize == 10
    assert pool.size == 1
    assert pool.freesize == 1


def test_connect(pool):
    _assert_defaults(pool)


def test_global_loop(test_thrift, create_pool, loop, server):
    asyncio.set_event_loop(loop)

    pool = loop.run_until_complete(create_pool(
        test_thrift.Test,
        server.address))
    _assert_defaults(pool)


@pytest.mark.run_loop
def test_clear(pool):
    _assert_defaults(pool)

    yield from pool.clear()
    assert pool.freesize == 0


@pytest.mark.run_loop
@pytest.mark.parametrize('minsize', [None, -100, 0.0, 100])
def test_minsize(test_thrift, minsize, create_pool, loop, server):
    with pytest.raises(AssertionError):
        yield from create_pool(test_thrift.Test,
                               server.address, minsize=minsize, maxsize=10, loop=loop)


@pytest.mark.run_loop
@pytest.mark.parametrize('maxsize', [None, -100, 0.0, 1])
def test_maxsize(test_thrift, maxsize, create_pool, loop, server):
    with pytest.raises(AssertionError):
        yield from create_pool(
            test_thrift.Test,
            server.address,
            minsize=2, maxsize=maxsize, loop=loop)


def test_no_yield_from(pool):
    with pytest.raises(RuntimeError):
        with pool:
            pass  # pragma: no cover


@pytest.mark.run_loop
def test_rpc(test_thrift, create_pool, loop, server):
    pool = yield from create_pool(
        test_thrift.Test,
        server.address,
        minsize=10, loop=loop)

    with (yield from pool) as conn:
        result = yield from conn.ping()
        assert result == 'pong'
        result = yield from conn.add(100, 200)
        assert result == 300
        assert pool.size == 10
        assert pool.freesize == 9
    assert pool.size == 10
    assert pool.freesize == 10


@pytest.mark.run_loop
def test_create_new(test_thrift, create_pool, loop, server):
    pool = yield from create_pool(
        test_thrift.Test,
        server.address,
        minsize=1, loop=loop)
    assert pool.size == 1
    assert pool.freesize == 1

    with (yield from pool):
        assert pool.size == 1
        assert pool.freesize == 0

        with (yield from pool):
            assert pool.size == 2
            assert pool.freesize == 0

    assert pool.size == 2
    assert pool.freesize == 2


@pytest.mark.run_loop
def test_create_constraints(test_thrift, create_pool, loop, server):
    pool = yield from create_pool(
        test_thrift.Test,
        server.address,
        minsize=1, maxsize=1, loop=loop)
    assert pool.size == 1
    assert pool.freesize == 1

    with (yield from pool):
        assert pool.size == 1
        assert pool.freesize == 0

        with pytest.raises(asyncio.TimeoutError):
            yield from asyncio.wait_for(pool.acquire(),
                                        timeout=0.2,
                                        loop=loop)


@pytest.mark.run_loop
def test_create_no_minsize(test_thrift, create_pool, loop, server):
    pool = yield from create_pool(
        test_thrift.Test,
        server.address,
        minsize=0, maxsize=1, loop=loop)
    assert pool.size == 0
    assert pool.freesize == 0

    with (yield from pool):
        assert pool.size == 1
        assert pool.freesize == 0

        with pytest.raises(asyncio.TimeoutError):
            yield from asyncio.wait_for(pool.acquire(),
                                        timeout=0.2,
                                        loop=loop)
    assert pool.size == 1
    assert pool.freesize == 1


@pytest.mark.run_loop
def test_release_closed(test_thrift, create_pool, loop, server):
    pool = yield from create_pool(
        test_thrift.Test,
        server.address,
        minsize=1, loop=loop)
    assert pool.size == 1
    assert pool.freesize == 1

    with (yield from pool) as conn:
        conn.close()
    assert pool.size == 0
    assert pool.freesize == 0


@pytest.mark.run_loop
def test_release_bad_connection(test_thrift, create_pool, create_connection, loop, server):
    pool = yield from create_pool(
        test_thrift.Test,
        server.address,
        loop=loop)
    conn = yield from pool.acquire()
    other_conn = yield from create_connection(
        test_thrift.Test,
        server.address,
        loop=loop)
    with pytest.raises(AssertionError):
        pool.release(other_conn)

    pool.release(conn)
    other_conn.close()


@pytest.mark.run_loop
def test_pool_size_growth(test_thrift, create_pool, server, loop):
    pool = yield from create_pool(
        test_thrift.Test,
        server.address,
        loop=loop,
        minsize=1, maxsize=1)

    done = set()
    tasks = []

    @asyncio.coroutine
    def task1(i):
        with (yield from pool):
            assert pool.size <= pool.maxsize
            assert pool.freesize == 0
            yield from asyncio.sleep(0.2, loop=loop)
            done.add(i)

    @asyncio.coroutine
    def task2():
        with (yield from pool):
            assert pool.size <= pool.maxsize
            assert pool.freesize >= 0
            assert done == {0, 1}

    for _ in range(2):
        tasks.append(asyncio.async(task1(_), loop=loop))
    tasks.append(asyncio.async(task2(), loop=loop))
    yield from asyncio.gather(*tasks, loop=loop)


@pytest.mark.run_loop
def test_pool_with_closed_connections(test_thrift, create_pool, server, loop):
    pool = yield from create_pool(
        test_thrift.Test,
        server.address,
        loop=loop,
        minsize=1, maxsize=2)
    assert 1 == pool.freesize
    conn1 = pool._pool[0]
    conn1.close()
    assert conn1.closed is True
    assert 1 == pool.freesize
    with (yield from pool) as conn2:
        assert conn2.closed is False
        assert conn1 is not conn2


@pytest.mark.run_loop
def test_pool_close(test_thrift, create_pool, server, loop):
    pool = yield from create_pool(
        test_thrift.Test,
        server.address, loop=loop)

    assert pool.closed is False

    with (yield from pool) as cli:
        assert (yield from cli.ping()) == 'pong'

    pool.close()
    yield from pool.wait_closed()
    assert pool.closed is True

    with pytest.raises(PoolClosedError):
        with (yield from pool) as cli:
            assert (yield from cli.ping()) == 'PONG'


@pytest.mark.run_loop
def test_pool_close__used(test_thrift, create_pool, server, loop):
    pool = yield from create_pool(
        test_thrift.Test,
        server.address, loop=loop)

    assert pool.closed is False

    with (yield from pool) as cli:
        pool.close()
        yield from pool.wait_closed()
        assert pool.closed is True

        with pytest.raises(ConnectionClosedError):
            yield from cli.ping()
