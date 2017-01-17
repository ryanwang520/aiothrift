import asyncio

import pytest


@pytest.mark.run_loop
def test_connect_tcp(test_thrift, create_connection, loop, server):
    conn = yield from create_connection(
        test_thrift.Test, server.address, loop=loop, timeout=5
    )
    assert conn.service is test_thrift.Test
    assert isinstance(conn.address, tuple)
    assert conn.address[0] == '127.0.0.1'
    assert conn.address[1] == 6000
    assert conn.timeout == 5
    assert conn.closed is False
    assert conn.ping
    assert conn.add


def test_global_loop(test_thrift, create_connection, loop, server):
    assert server
    asyncio.set_event_loop(loop)
    conn = loop.run_until_complete(create_connection(test_thrift.Test, server.address))
    assert conn.service is test_thrift.Test
