import asyncio
import logging
from asyncio import AbstractEventLoop

import subprocess
import time
from functools import wraps
from typing import List, Any, Optional

logger = logging.getLogger(__name__)


def get_git_revision_num():
    label = subprocess.check_output(['git', 'describe', '--always']).strip()
    return label.decode()


def as_dicts(func):
    def to_dics(result):
        return [dict(item) for item in result]

    if asyncio.iscoroutinefunction(func):
        @wraps(func)
        async def _wrapper(*args, **kwargs):
            result: List[Any] = await func(*args, **kwargs)
            return to_dics(result)
    else:
        @wraps(func)
        def _wrapper(*args, **kwargs):
            result: List[Any] = func(*args, **kwargs)
            return to_dics(result)

    return _wrapper


def async_timeit(timeout: Optional[int] = None, name: Optional[str] = None):
    def _wrapper(func):
        @wraps(func)
        async def _async_wrapper(*args, **kwargs):
            started_at = time.monotonic()

            result = await func(*args, **kwargs)

            duration = time.monotonic() - started_at
            if timeout is None or timeout < duration:
                func_name = name is not None and name or func.__name__
                logger.info(f"{func_name} has took", extra={"seconds": round(duration, 2)})

                return result

        return _async_wrapper

    return _wrapper


def get_loop_tasks_count(loop: Optional[AbstractEventLoop] = None) -> int:
    # TODO: Replace with `asyncio.all_tasks()` when migrating to `3.7`.
    return len(asyncio.Task.all_tasks(loop))
