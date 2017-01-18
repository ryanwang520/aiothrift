"""
The connection pool implementation is heavily borrowed from `aioredis`
"""
import asyncio
import collections
import sys

from .connection import create_connection
from .log import logger
from .util import async_task
from .errors import PoolClosedError

PY_35 = sys.version_info >= (3, 5)


@asyncio.coroutine
def create_pool(service, address=('127.0.0.1', 6000), *, minsize=1, maxsize=10, loop=None, timeout=None):
    """
    Create a thrift connection pool. This function is a :ref:`coroutine <coroutine>`.

    :param service: service object defined by thrift file
    :param address: (host, port) tuple, default is ('127.0.0.1', 6000)
    :param minsize: minimal thrift connection, default is 1
    :param maxsize: maximal thrift connection, default is 10
    :param loop: targeting :class:`eventloop <asyncio.AbstractEventLoop>`
    :param timeout: default timeout for each connection, default is None
    :return: :class:`ThriftPool` instance
    """

    pool = ThriftPool(service, address, minsize=minsize,
                      maxsize=maxsize, loop=loop, timeout=timeout)
    try:
        yield from pool.fill_free(override_min=False)
    except Exception:
        pool.close()
        raise

    return pool


class ThriftPool:
    """Thrift connection pool.
    """

    def __init__(self, service, address,
                 *, minsize, maxsize, loop=None, timeout=None):
        assert isinstance(minsize, int) and minsize >= 0, (
            "minsize must be int >= 0", minsize, type(minsize))
        assert maxsize is not None, "Arbitrary pool size is disallowed."
        assert isinstance(maxsize, int) and maxsize > 0, (
            "maxsize must be int > 0", maxsize, type(maxsize))
        assert minsize <= maxsize, (
            "Invalid pool min/max sizes", minsize, maxsize)
        if loop is None:
            loop = asyncio.get_event_loop()
        self._address = address
        self.minsize = minsize
        self.maxsize = maxsize
        self._loop = loop
        self._pool = collections.deque(maxlen=maxsize)
        self._used = set()
        self._acquiring = 0
        self._cond = asyncio.Condition(loop=loop)
        self._service = service
        self._timeout = timeout
        self.closed = False
        self._release_tasks = set()

    @property
    def size(self):
        """Current connection total num, acquiring connection num is counted"""
        return self.freesize + len(self._used) + self._acquiring

    @property
    def freesize(self):
        """Current number of free connections."""
        return len(self._pool)

    @asyncio.coroutine
    def clear(self):
        """Clear pool connections.

        Close and remove all free connections.
        this pattern is interesting
        """
        while self._pool:
            conn = self._pool.popleft()
            conn.close()

    def close(self):
        """Close all free and in-progress connections and mark pool as closed.
        """
        self.closed = True
        conn_num = 0
        while self._pool:
            conn = self._pool.popleft()
            conn.close()
            conn_num += 1
        for conn in self._used:
            conn.close()
            conn_num += 1
        logger.debug("Closed %d connections", conn_num)

    @asyncio.coroutine
    def wait_closed(self):
        for task in self._release_tasks:
            yield from asyncio.shield(task, loop=self._loop)

    @asyncio.coroutine
    def acquire(self):
        """Acquires a connection from free pool.

        Creates new connection if needed.
        """
        if self.closed:
            raise PoolClosedError('Pool is closed')
        with (yield from self._cond):
            if self.closed:
                raise PoolClosedError('Pool is closed')
            while True:
                yield from self.fill_free(override_min=True)
                # new connection has been added to the pool
                if self.freesize:
                    conn = self._pool.popleft()
                    assert not conn.closed, conn
                    assert conn not in self._used, (conn, self._used)
                    # each acquire would move a conn from `self._pool` to `self._used`
                    self._used.add(conn)
                    return conn
                else:
                    # wait when no available connection
                    yield from self._cond.wait()

    def release(self, conn):
        """Returns used connection back into pool.

        When queue of free connections is full the connection will be dropped.
        """
        assert conn in self._used, 'Invalid connection, maybe from other pool'
        self._used.remove(conn)
        if not conn.closed:
            assert self.freesize < self.maxsize, 'max connection size should not exceed'
            self._pool.append(conn)
        if not self._loop.is_closed():
            tasks = set()
            for task in self._release_tasks:
                if not task.done():
                    tasks.add(task)
            self._release_tasks = tasks
            future = async_task(self._notify_conn_returned(), loop=self._loop)
            self._release_tasks.add(future)

    def _drop_closed(self):
        for i in range(self.freesize):
            conn = self._pool[0]
            if conn.closed:
                self._pool.popleft()
            else:
                self._pool.rotate(1)

    @asyncio.coroutine
    def fill_free(self, *, override_min):
        """
        make sure at least `self.minsize` amount of connections in the pool
        if `override_min` is True, fill to the `self.maxsize`.
        """
        # drop closed connections first, in case that the user closed the connection manually
        self._drop_closed()

        while self.size < self.minsize:
            self._acquiring += 1
            try:
                conn = yield from self._create_new_connection()
                self._pool.append(conn)
            finally:
                self._acquiring -= 1
                self._drop_closed()
        if self.freesize:
            return
        if override_min:
            # when self.size >= minsize and no available connection
            while not self._pool and self.size < self.maxsize:
                self._acquiring += 1
                try:
                    conn = yield from self._create_new_connection()
                    self._pool.append(conn)
                finally:
                    self._acquiring -= 1

    def _create_new_connection(self):
        return create_connection(self._service, self._address,
                                 loop=self._loop, timeout=self._timeout)

    @asyncio.coroutine
    def _notify_conn_returned(self):
        with (yield from self._cond):
            self._cond.notify()

    def __enter__(self):
        raise RuntimeError(
            "'yield from' should be used as a context manager expression")

    def __exit__(self, *args):
        pass  # pragma: nocover

    def __iter__(self):
        # this method is needed to allow `yield`ing from pool
        conn = yield from self.acquire()
        return _ConnectionContextManager(self, conn)

    if PY_35:
        def __await__(self):
            # To make `with await pool` work
            conn = yield from self.acquire()
            return _ConnectionContextManager(self, conn)

        def get(self):
            """
            Return async context manager for working with connection::

                async with pool.get() as conn:
                    await conn.get(key)
            """
            return _AsyncConnectionContextManager(self)


class _ConnectionContextManager:
    __slots__ = ('_pool', '_conn')

    def __init__(self, pool, conn):
        self._pool = pool
        self._conn = conn

    def __enter__(self):
        return self._conn

    def __exit__(self, exc_type, exc_value, tb):
        try:
            self._pool.release(self._conn)
        finally:
            self._pool = None
            self._conn = None


if PY_35:
    class _AsyncConnectionContextManager:

        __slots__ = ('_pool', '_conn')

        def __init__(self, pool):
            self._pool = pool
            self._conn = None

        @asyncio.coroutine
        def __aenter__(self):
            self._conn = yield from self._pool.acquire()
            return self._conn

        @asyncio.coroutine
        def __aexit__(self, exc_type, exc_value, tb):
            try:
                self._pool.release(self._conn)
            finally:
                self._pool = None
                self._conn = None
