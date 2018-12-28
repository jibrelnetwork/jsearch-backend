import asyncio
import logging
from functools import wraps
from pprint import saferepr

log = logging.getLogger(__name__)


def suppress_exception(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception:
            log.exception(
                f"{func.__name__} was failed with args %s and kwargs %s",
                saferepr(args), saferepr(kwargs)
            )

    return wrapper


def async_suppress_exception(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception:
            log.exception(
                f"{func.__name__} was failed with args %s and kwargs %s",
                saferepr(args), saferepr(kwargs)
            )

    return wrapper


def add_semaphore(func, value: int):
    semaphore = asyncio.Semaphore(value=value)

    @wraps(func)
    async def _wrapper(*args, **kwargs):
        async with semaphore:
            return await func(*args, **kwargs)

    return _wrapper


def run_on_loop(func):
    @wraps(func)
    def _wrapper(*args, **kwargs):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(func(*args, **kwargs))

    return _wrapper


