import asyncio
import subprocess
import pytest
import thriftpy

import aiothrift


@pytest.fixture(scope='session')
def test_thrift():
    return thriftpy.load('tests/test.thrift', module_name='test_thrift')


@pytest.yield_fixture
def loop():
    """Creates new event loop."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    yield loop

    if hasattr(loop, 'is_closed'):
        closed = loop.is_closed()
    else:
        closed = loop._closed
    if not closed:
        loop.call_soon(loop.stop)
        loop.run_forever()
        loop.close()


@pytest.yield_fixture
def _closable(loop):
    conns = []

    yield conns.append

    waiters = []
    while conns:
        conn = conns.pop(0)
        conn.close()
        waiters.append(conn.wait_closed())
    if waiters:
        loop.run_until_complete(asyncio.gather(*waiters, loop=loop))


@pytest.fixture
def create_connection():
    @asyncio.coroutine
    def f(*args, **kwargs):
        conn = yield from aiothrift.create_connection(*args, **kwargs)
        return conn

    return f


@pytest.fixture
def create_pool(_closable):
    @asyncio.coroutine
    def f(*args, **kwargs):
        pool = yield from aiothrift.create_pool(*args, **kwargs)
        _closable(pool)
        return pool

    return f


@pytest.fixture(scope='session')
def server(request):
    proc = subprocess.Popen(['python3',
                             'tests/server.py'
                             ], stdout=subprocess.PIPE)
    log = b''
    while b'server is listening' not in log:
        log = proc.stdout.readline()

    def close():
        proc.terminate()
        proc.wait()

    request.addfinalizer(close)
    proc.address = ('127.0.0.1', 6000)
    return proc


@pytest.fixture
def pool(test_thrift, create_pool, server, loop):
    pool = loop.run_until_complete(
        create_pool(test_thrift.Test, server.address, loop=loop))
    return pool


@pytest.mark.tryfirst
def pytest_pycollect_makeitem(collector, name, obj):
    if collector.funcnamefilter(name):
        if not callable(obj):
            return
        item = pytest.Function(name, parent=collector)
        if 'run_loop' in item.keywords:
            return list(collector._genfunctions(name, obj))


@pytest.mark.tryfirst
def pytest_pyfunc_call(pyfuncitem):
    """
    Run asyncio marked test functions in an event loop instead of a normal
    function call.
    """
    if 'run_loop' in pyfuncitem.keywords:
        funcargs = pyfuncitem.funcargs
        loop = funcargs['loop']
        testargs = {arg: funcargs[arg]
                    for arg in pyfuncitem._fixtureinfo.argnames}
        loop.run_until_complete(
            asyncio.wait_for(pyfuncitem.obj(**testargs),
                             15, loop=loop))
        return True
