from functools import wraps

import async_timeout

DEFAULT_TIMEOUT = 60 * 5  # sec


def timeout(seconds=DEFAULT_TIMEOUT):
    def wrapper(func):
        @wraps(func)
        async def _wrapper(*args, **kwargs):
            async with async_timeout.timeout(seconds):
                return await func(*args, **kwargs)

        return _wrapper

    return wrapper
