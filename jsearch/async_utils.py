import asyncio
from functools import wraps
from typing import Any, Iterable

import async_timeout

from jsearch.typing import AsyncCallback
from jsearch.utils import split

DEFAULT_TIMEOUT = 60 * 5  # sec


def timeout(seconds=DEFAULT_TIMEOUT):
    def wrapper(func):
        @wraps(func)
        async def _wrapper(*args, **kwargs):
            async with async_timeout.timeout(seconds):
                return await func(*args, **kwargs)

        return _wrapper

    return wrapper


async def do_parallel(func: AsyncCallback, argument_list: Iterable[Any], chunk_size: int):
    tasks = [func(chunk) for chunk in split(argument_list, chunk_size)]
    return await asyncio.gather(*tasks)
