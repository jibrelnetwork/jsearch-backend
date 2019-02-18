import asyncio
import logging
import time
from collections import defaultdict
from typing import Optional, Union, DefaultDict, List

from jsearch.utils import Singleton

logger = logging.getLogger(__name__)


class Metric:
    name: str
    value: Optional[Union[int, float]]

    started_at: float
    finished_at: Optional[float]

    def __init__(self, name):
        self.name = name
        self.started_at = time.time()

    @property
    def worked_time(self):
        if self.finished_at:
            return self.finished_at - self.started_at
        return time.time() - self.started_at

    @property
    def speed(self):
        return self.value / self.worked_time

    def finish(self, value: Union[int, float]):
        self.value = value
        self.finished_at = time.time()


class Metrics(Singleton):
    """
    Examples:

    - start new metric
    >>> logs_per_second = Metric('logs_per_seconds')
    >>> logs_per_second.finish(100)

    >>> metrics = Metrics()
    >>> metrics.update(logs_per_second)

    - show average metrics
    >>> metrics.show()
    """
    logs: int
    blocks: int
    timeout: int  # seconds

    metrics: DefaultDict[str, List[Metric]]

    def __init__(self, timeout=10):
        self.logs = 0
        self.blocks = 0
        self.timeout = timeout

        self.future = None
        self.metrics = defaultdict(list)

    def update(self, metric: Metric):
        self.ensure_started()
        self.metrics[metric.name].append(metric)

    def ensure_started(self):
        if not self.future:
            self.future = asyncio.ensure_future(self.task())

    def ensure_stop(self):
        if self.future:
            self.future.cancel()

    def show(self):
        for name, metrics in self.metrics.items():
            if metrics:
                value = sum([metric.value for metric in metrics], 0)
                worked_time = sum([metric.worked_time for metric in metrics], 0)
                speed = value / worked_time
                logger.info(
                    "[METRICS] %s: handled %0.2f, time %0.2f speed %0.2f %s/second for each worker",
                    name, value, worked_time, speed, name
                )

            self.metrics[name].clear()

    async def task(self):
        while True:
            self.show()
            await asyncio.sleep(self.timeout)
