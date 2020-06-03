"""
The connection pool implementation is heavily borrowed from `aioredis`
"""
import asyncio
import contextvars as cv

import collections
import functools

from .connection import create_connection
from .log import logger
from .errors import PoolClosedError

acquired_connection = cv.ContextVar("acquired_connection")


async def create_pool(
    service,
    address=("127.0.0.1", 6000),
    *,
    minsize=1,
    maxsize=10,
    timeout=None,
    framed=False
):
    """
    Create a thrift connection pool. This function is a :ref:`coroutine <coroutine>`.

    :param service: service object defined by thrift file
    :param address: (host, port) tuple, default is ('127.0.0.1', 6000)
    :param minsize: minimal thrift connection, default is 1
    :param maxsize: maximal thrift connection, default is 10
    :param timeout: default timeout for each connection, default is None
    :param framed: use TFramedTransport, default is False
    :return: :class:`ThriftPool` instance
    """

    pool = ThriftPool(
        service,
        address,
        minsize=minsize,
        maxsize=maxsize,
        timeout=timeout,
        framed=framed,
    )
    try:
        await pool.fill_free(override_min=False)
    except Exception:
        pool.close()
        raise

    return pool


class ThriftPool:
    """Thrift connection pool.
    """

    def __init__(self, service, address, *, minsize, maxsize, timeout=None, framed=False):
        assert isinstance(minsize, int) and minsize >= 0, (
            "minsize must be int >= 0",
            minsize,
            type(minsize),
        )
        assert maxsize is not None, "Arbitrary pool size is disallowed."
        assert isinstance(maxsize, int) and maxsize > 0, (
            "maxsize must be int > 0",
            maxsize,
            type(maxsize),
        )
        assert minsize <= maxsize, ("Invalid pool min/max sizes", minsize, maxsize)
        self._address = address
        self.minsize = minsize
        self.maxsize = maxsize
        self._pool = collections.deque(maxlen=maxsize)
        self._used = set()
        self._acquiring = 0
        self._cond = asyncio.Condition()
        self._service = service
        self._timeout = timeout
        self._framed = framed
        self.closed = False
        self._release_tasks = set()
        self._init_rpc_apis()

    def _init_rpc_apis(self):
        for api in self._service.thrift_services:
            if not hasattr(self, api):

                setattr(self, api, functools.partial(self.execute, api))
            else:
                logger.warning(
                    "api name {0} is conflicted with connection attribute "
                    '{0}, while you can still call this api by `execute("{0}")`'.format(
                        api
                    )
                )

    async def execute(self, cmd, *args, **kwargs):
        conn = await self.acquire()
        try:
            return await conn.execute(cmd, *args, **kwargs)
        finally:
            self.release(conn)

    @property
    def size(self):
        """Current connection total num, acquiring connection num is counted"""
        return self.freesize + len(self._used) + self._acquiring

    @property
    def freesize(self):
        """Current number of free connections."""
        return len(self._pool)

    async def clear(self):
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

    async def wait_closed(self):
        for task in self._release_tasks:
            await asyncio.shield(task)

    async def acquire(self):
        """Acquires a connection from free pool.

        Creates new connection if needed.
        """
        if self.closed:
            raise PoolClosedError("Pool is closed")
        async with self._cond:
            if self.closed:
                raise PoolClosedError("Pool is closed")
            while True:
                await self.fill_free(override_min=True)
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
                    await self._cond.wait()

    def release(self, conn):
        """Returns used connection back into pool.

        When queue of free connections is full the connection will be dropped.
        """
        assert conn in self._used, "Invalid connection, maybe from other pool"
        self._used.remove(conn)
        if not conn.closed:
            assert self.freesize < self.maxsize, "max connection size should not exceed"
            self._pool.append(conn)

        loop = asyncio.get_running_loop()
        if not loop.is_closed():
            tasks = set()
            for task in self._release_tasks:
                if not task.done():
                    tasks.add(task)
            self._release_tasks = tasks
            future = asyncio.create_task(self._notify_conn_returned())
            self._release_tasks.add(future)

    def _drop_closed(self):
        for i in range(self.freesize):
            conn = self._pool[0]
            if conn.closed:
                self._pool.popleft()
            else:
                self._pool.rotate(1)

    async def fill_free(self, *, override_min):
        """
        make sure at least `self.minsize` amount of connections in the pool
        if `override_min` is True, fill to the `self.maxsize`.
        """
        # drop closed connections first, in case that the user closed the connection manually
        self._drop_closed()

        while self.size < self.minsize:
            self._acquiring += 1
            try:
                conn = await self._create_new_connection()
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
                    conn = await self._create_new_connection()
                    self._pool.append(conn)
                finally:
                    self._acquiring -= 1

    def _create_new_connection(self):
        return create_connection(
            self._service, self._address, timeout=self._timeout, framed=self._framed
        )

    async def _notify_conn_returned(self):
        async with self._cond:
            self._cond.notify()

    async def __aenter__(self):
        if self.closed:
            raise PoolClosedError("cannot acquire a connection from a closed pool")
        if acquired_connection.get(None) is not None:
            raise RuntimeError(
                "cannot acquire a connection if you already have one inside the same task"
            )
        _conn = await self.acquire()
        acquired_connection.set(_conn)
        return _conn

    async def __aexit__(self, *exc_info):
        self.release(acquired_connection.get())
