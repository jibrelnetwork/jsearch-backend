import asyncio
import logging
import signal
from typing import List, Any, Tuple, Optional, Callable

log = logging.getLogger(__name__)


class Singleton(object):
    _instance = None  # Keep instance reference

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls, *args, **kwargs)
        return cls._instance


def split(iterable: List[Any], size: int) -> List[List[Any]]:
    if isinstance(iterable, set):
        iterable = list(iterable)
    return [iterable[i:i + size] for i in range(0, len(iterable), size)]


def parse_range(value: Optional[str] = None) -> Tuple[int, Optional[int]]:
    if not value:
        result = (0, None)
    else:
        parts = [p.strip() for p in value.split('-')]
        if len(parts) != 2:
            raise ValueError('Invalid sync_range option')

        value_from = int(parts[0]) if parts[0] else 1
        value_until = int(parts[1]) if parts[1] else None

        result = (value_from, value_until)

    return result


def add_gracefully_shutdown_handlers(callback: Callable[..., Any]):
    loop = asyncio.get_event_loop()
    for signame in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig=signame, callback=callback)


async def shutdown():
    loop = asyncio.get_event_loop()

    logging.info(f'Received exit signal ...')
    tasks = [task for task in asyncio.Task.all_tasks() if task is not asyncio.Task.current_task()]

    for task in tasks:
        task.cancel()

    logging.info('Canceling outstanding tasks')

    results = await asyncio.gather(*tasks, return_exceptions=True)
    for task, result in zip(tasks, results):
        if isinstance(result, Exception):
            logging.info('finished awaiting cancelled task %s with results: %s %s', task, type(result), result)

    loop.stop()
    logging.info('Shutdown complete.')
