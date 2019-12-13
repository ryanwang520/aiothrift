import asyncio
import subprocess
import pytest
import thriftpy2 as thriftpy

import aiothrift


@pytest.fixture(scope="session")
def test_thrift():
    return thriftpy.load("tests/test.thrift", module_name="test_thrift")


@pytest.fixture
async def _closable():
    conns = []

    yield conns.append

    waiters = []
    while conns:
        conn = conns.pop(0)
        conn.close()
        waiters.append(conn.wait_closed())
    if waiters:
        await asyncio.gather(*waiters)


@pytest.fixture
def create_connection():
    async def f(*args, **kwargs):
        conn = await aiothrift.create_connection(*args, **kwargs)
        return conn

    return f


@pytest.fixture
def create_pool(_closable):
    async def f(*args, **kwargs):
        pool = await aiothrift.create_pool(*args, **kwargs)
        _closable(pool)
        return pool

    return f


@pytest.fixture(scope="session")
def server(request):
    proc = subprocess.Popen(["python3", "tests/server.py"], stdout=subprocess.PIPE)
    log = b""
    while b"server is listening" not in log:
        log = proc.stdout.readline()

    def close():
        proc.terminate()
        proc.wait()

    request.addfinalizer(close)
    proc.address = ("127.0.0.1", 6000)
    return proc


@pytest.fixture
async def pool(test_thrift, create_pool, server):
    pool = await create_pool(test_thrift.Test, server.address)
    return pool
