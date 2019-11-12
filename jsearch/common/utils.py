import asyncio
import logging
from asyncio import AbstractEventLoop
from contextlib import contextmanager
from dataclasses import dataclass

import time
from functools import wraps
from typing import Optional, TypeVar, List, Any, Hashable

logger = logging.getLogger(__name__)


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
    timer_ = Timer(start_at=time.perf_counter(), end_at=None)
    yield timer_
    timer_.end_at = time.perf_counter()


def timeit(name: Optional[str] = None, timeout: Optional[int] = None, precision: int = 3):
    def _wrapper(func):

        def log_time(t):
            if not (timeout and timeout > t.get()):
                func_name = name or func.__name__
                logger.info(f"{func_name} has taken", extra={"seconds": round(t.seconds, precision)})

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


T = TypeVar('T')


def unique(list_: List[T]) -> List[T]:
    return list(dict.fromkeys(list_))


def safe_get(obj: Any, key: Hashable) -> Any:
    try:
        return obj.get(key)
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.info("Cannot get %r from %r", key, obj, exc_info=True)

    return None
