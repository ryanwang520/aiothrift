import asyncio

from thriftpy.transport import TTransportException


class TTransport:
    def __init__(self, reader):
        self._reader = reader

    @asyncio.coroutine
    def read(self, sz):
        buff = b''
        have = 0
        while have < sz:
            chunk = yield from self._reader.read(sz - have)
            if len(chunk) == 0:
                raise TTransportException(type=TTransportException.END_OF_FILE,
                                          message='TSocket read 0 bytes')
            have += len(chunk)
            buff += chunk
        return buff
