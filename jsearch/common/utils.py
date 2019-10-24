import asyncio
import logging
from asyncio import AbstractEventLoop
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass

import subprocess
import time
from functools import wraps, reduce
from typing import List, Any, Optional, NamedTuple, Dict, Union

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
    name: str

    start_at: float
    end_at: Optional[float] = None

    precision: int = 3

    @property
    def seconds(self):
        if self.end_at:
            return round(self.end_at - self.start_at, self.precision)


def add_timer(acc: Dict[str, Union[List[float], str]], t: Timer) -> Dict[str, List[float]]:
    acc[t.name].append(t.seconds)
    return acc


class Timers(NamedTuple):
    timers: List[Timer] = []
    meta: Dict[str, str] = {}

    def add(self, t: Timer) -> None:
        self.timers.append(t)

    def as_dict(self) -> Dict[str, List[str]]:
        total = defaultdict(list)
        total = reduce(add_timer, self.timers, total)
        return {
            **self.meta,
            **{key: list(map(str, values)) for key, values in total.items()}
        }

    def add_meta(self, **kwargs: Any) -> None:
        self.meta.update(**kwargs)

    def flush(self, msg: str) -> None:
        logging.info(msg, extra=self.as_dict())
        self.timers.clear()
        self.meta.clear()


timers = Timers()


@contextmanager
def timer(name, precision=3):
    timer_ = Timer(name=name, precision=precision, start_at=time.perf_counter(), end_at=None)
    yield timer_
    timer_.end_at = time.perf_counter()


def timeit(name: Optional[str] = None, timeout: Optional[int] = None, precision: int = 3, accumulate: bool = False):
    """
    Examples:
        import asyncio

        @timeit(name="[CPU]")
        def foo():
            print(1 + 2)

        @timeit()
        def async_action():
            asyncio.sleep(2)

        foo()
        foo()
        await async_action()
        await async_action()

        timers.flush("timer")
    """

    def _wrapper(func):

        func_name = name is not None and name or func.__name__

        def log_time(t):
            if not (timeout and timeout > t.get()):
                logger.info(f"{func_name} has taken", extra={"seconds": round(t.seconds, precision)})

        if asyncio.iscoroutinefunction(func):
            @wraps(func)
            async def wrapper(*args, **kwargs):
                with timer(name) as t:
                    result = await func(*args, **kwargs)

                if accumulate:
                    timers.add(t)
                else:
                    log_time(t)
                return result
        else:
            @wraps(func)
            def wrapper(*args, **kwargs):
                with timer(name) as t:
                    result = func(*args, **kwargs)

                if accumulate:
                    timers.add(t)
                else:
                    log_time(t)

                return result

        return wrapper

    return _wrapper


def get_loop_tasks_count(loop: Optional[AbstractEventLoop] = None) -> int:
    # TODO: Replace with `asyncio.all_tasks()` when migrating to `3.7`.
    return len(asyncio.Task.all_tasks(loop))
