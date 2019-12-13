from thriftpy2.thrift import TType


class ThriftError(Exception):
    """ Base Exception defined by `aiothrift` """


class ConnectionClosedError(ThriftError):
    """Raised if connection to server was closed."""


class PoolClosedError(ThriftError):
    """Raised when operating on a closed thrift connection pool"""


class ThriftAppError(ThriftError):
    """Application level thrift exceptions."""

    thrift_spec = {
        1: (TType.STRING, "message", False),
        2: (TType.I32, "type", False),
    }

    UNKNOWN = 0
    UNKNOWN_METHOD = 1
    INVALID_MESSAGE_TYPE = 2
    WRONG_METHOD_NAME = 3
    BAD_SEQUENCE_ID = 4
    MISSING_RESULT = 5
    INTERNAL_ERROR = 6
    PROTOCOL_ERROR = 7

    def __init__(self, type=UNKNOWN, message=None):
        super().__init__()
        self.type = type
        self.message = message

    def __str__(self):
        if self.message:
            return self.message

        if self.type == self.UNKNOWN_METHOD:
            return "Unknown method"
        elif self.type == self.INVALID_MESSAGE_TYPE:
            return "Invalid message type"
        elif self.type == self.WRONG_METHOD_NAME:
            return "Wrong method name"
        elif self.type == self.BAD_SEQUENCE_ID:
            return "Bad sequence ID"
        elif self.type == self.MISSING_RESULT:
            return "Missing result"
        else:
            return "Default (unknown) TApplicationException"
