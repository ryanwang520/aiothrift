import asyncio
import sys

from asyncio.base_events import BaseEventLoop

PY_35 = sys.version_info >= (3, 5)

if hasattr(asyncio, 'ensure_future'):
    async_task = asyncio.ensure_future
else:
    async_task = asyncio.async  # Deprecated since 3.4.4

# create_future is new in version 3.5.2
if hasattr(BaseEventLoop, 'create_future'):
    def create_future(loop):
        return loop.create_future()
else:
    def create_future(loop):
        return asyncio.Future(loop=loop)


def args2kwargs(thrift_spec, *args):
    arg_names = [item[1][1] for item in sorted(thrift_spec.items())]
    return dict(zip(arg_names, args))
