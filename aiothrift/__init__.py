from .connection import create_connection, ThriftConnection
from .pool import create_pool, ThriftPool
from .errors import (
    ConnectionClosedError
)


__version__ = '0.0.6'
