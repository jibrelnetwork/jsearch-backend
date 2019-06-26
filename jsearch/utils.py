import asyncio
import logging

import signal
from typing import List, Any, Tuple, Optional, Callable, Iterable

logger = logging.getLogger(__name__)

DEFAULT_RANGE_START = 0
DEFAULT_RANGE_END = None


class Singleton(object):
    _instance = None  # Keep instance reference

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls, *args, **kwargs)
        return cls._instance


def split(iterable: Iterable[Any], size: int) -> List[List[Any]]:
    if isinstance(iterable, set):
        iterable = list(iterable)
    return [iterable[i:i + size] for i in range(0, len(iterable), size)]


def parse_range(value: Optional[str] = None) -> Tuple[int, Optional[int]]:
    """
    >>> parse_range(None)
    (0, None)
    >>> parse_range('')
    (0, None)
    >>> parse_range('10-')
    (10, None)
    >>> parse_range('-10')
    (0, 10)
    >>> parse_range('10-20')
    (10, 20)
    """
    if not value:
        return DEFAULT_RANGE_START, DEFAULT_RANGE_END

    parts = [p.strip() for p in value.split('-')]

    if len(parts) != 2:
        raise ValueError('Invalid sync_range option')

    value_from = int(parts[0]) if parts[0] else DEFAULT_RANGE_START
    value_until = int(parts[1]) if parts[1] else DEFAULT_RANGE_END

    return value_from, value_until


def add_gracefully_shutdown_handlers(callback: Callable[..., Any]):
    loop = asyncio.get_event_loop()
    for signame in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig=signame, callback=callback)


async def shutdown():
    loop = asyncio.get_event_loop()

    logger.info('Received exit signal ...')
    tasks = [task for task in asyncio.Task.all_tasks() if task is not asyncio.Task.current_task()]

    for task in tasks:
        task.cancel()

    logger.info('Canceling outstanding tasks')

    results = await asyncio.gather(*tasks, return_exceptions=True)
    for task, result in zip(tasks, results):
        if isinstance(result, Exception):
            logger.info(
                'Finished awaiting cancelled task.',
                extra={
                    'task_name': task,
                    'task_result_type': type(result),
                    'task_result': result,
                }
            )

    loop.stop()
    logger.info('Shutdown complete.')
