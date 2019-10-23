import asyncio
import logging
from asyncio import AbstractEventLoop
from contextlib import contextmanager
from dataclasses import dataclass

import subprocess
import time
from functools import wraps, partial
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


@dataclass
class Timer:
    start_at: float
    end_at: Optional[float] = None

    @property
    def seconds(self):
        if self.end_at:
            return self.end_at - self.start_at


@contextmanager
def timer():
    timer_ = Timer(start_at=time.time(), end_at=None)
    yield timer_
    timer_.end_at = time.time()


def timeit(name: Optional[str] = None, timeout: Optional[int] = None, precision: int = 3):
    def _wrapper(func):

        def log_time(t):
            if not (timeout and timeout > t.get()):
                func_name = name is not None and name or func.__name__
                logger.info(f"async {func_name} has taken", extra={"seconds": round(t.seconds, precision)})

        if asyncio.iscoroutinefunction(func):
            @wraps(func)
            async def wrapper(*args, **kwargs):
                with timer() as t:
                    result = await func(*args, **kwargs)

                log_time(t)
                return result
        else:
            @wraps(func)
            def wrapper(*args, **kwargs):
                with timer() as t:
                    result = func(*args, **kwargs)

                log_time(t)
                return result

        return wrapper

    return _wrapper


def get_loop_tasks_count(loop: Optional[AbstractEventLoop] = None) -> int:
    # TODO: Replace with `asyncio.all_tasks()` when migrating to `3.7`.
    return len(asyncio.Task.all_tasks(loop))
