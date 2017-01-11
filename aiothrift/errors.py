class ThriftError(Exception):
    pass


class ConnectionClosedError(ThriftError):
    """Raised if connection to server was closed."""


class PoolClosedError(ThriftError):
    pass
