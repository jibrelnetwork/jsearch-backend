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

    @property
    def speed(self):
        if self.value and self.worked_time:
            return self.value / self.worked_time

    def finish(self, value: Union[int, float]):
        self.value = value
        self.finished_at = time.time()

    def __repr__(self):
        return f"<Metric {self.name} = {self.value} / {self.worked_time} = {self.speed}"


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

    def __init__(self, timeout=5):
        self.logs = 0
        self.blocks = 0
        self.timeout = timeout

        self.future = None
        self.metrics = defaultdict(list)
        self.values = {}

    def update(self, metric: Metric):
        self.ensure_started()
        self.metrics[metric.name].append(metric)

    def set_value(self, name, value, is_need_to_update):
        """
        set value to metrics

        it there is a callback - value will rewrite only if callback returns True
        """
        prev = self.values.get(name)
        if is_need_to_update(prev, value):
            self.values[name] = value

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
                    'Metrics',
                    extra={
                        'tag': 'METRICS',
                        'metrics_name': name,
                        'total_items_count': value,
                        'total_time': worked_time,
                        'per_worker_speed': speed,
                    }
                )

            self.metrics[name].clear()

        for name, value in self.values.items():
            logger.info(
                'Metrics by values',
                extra={
                    'tag': 'METRICS',
                    'metrics_name': name,
                    'total_items_count': value,
                }
            )

    async def task(self):
        while True:
            self.show()
            await asyncio.sleep(self.timeout)
