import pytest


@pytest.mark.asyncio
async def test_connect_tcp(test_thrift, create_connection, server):
    conn = await create_connection(test_thrift.Test, server.address, timeout=5)
    assert conn.service is test_thrift.Test
    assert isinstance(conn.address, tuple)
    assert conn.address[0] == "127.0.0.1"
    assert conn.address[1] == 6000
    assert conn.timeout == 5
    assert conn.closed is False
    assert conn.ping
    assert conn.add


@pytest.mark.asyncio
async def test_conflict_function_name(test_thrift, create_connection, server):
    conn = await create_connection(test_thrift.Test, server.address, timeout=5)
    assert conn.service is test_thrift.Test
    assert isinstance(conn.address, tuple)
    assert conn.address[0] == "127.0.0.1"
    assert conn.address[1] == 6000
    assert conn.timeout == 5
    assert conn.closed is False
    result = await conn.execute("address", "moon")
    assert result == "address moon"
