import asyncio
import sys

from asyncio.base_events import BaseEventLoop

if sys.version_info < (3, 7):
    async_task = asyncio.ensure_future
else:
    async_task = asyncio.create_task

# create_future is new in version 3.5.2
if hasattr(BaseEventLoop, "create_future"):

    def create_future(loop):
        return loop.create_future()


else:

    def create_future(loop):
        return asyncio.Future(loop=loop)


def args2kwargs(thrift_spec, *args):
    arg_names = [item[1][1] for item in sorted(thrift_spec.items())]
    return dict(zip(arg_names, args))
