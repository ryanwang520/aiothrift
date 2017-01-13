class ThriftError(Exception):
    """ Base Exception defined by `aiothrift` """


class ConnectionClosedError(ThriftError):
    """Raised if connection to server was closed."""


class PoolClosedError(ThriftError):
    """Raised when operating on a closed thrift connection pool"""
