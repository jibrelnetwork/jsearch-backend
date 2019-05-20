import asyncio
import logging
import time
from collections import defaultdict
from typing import Optional, Union, DefaultDict, List, Dict, Any, Callable, NoReturn

from jsearch.utils import Singleton

logger = logging.getLogger(__name__)


class Metric:
    name: str
    value: Union[int, float]

    started_at: float
    finished_at: Optional[float]

    def __init__(self, name) -> None:
        self.name = name
        self.value = 0
        self.started_at = time.monotonic()
        self.finished_at = None

    def __repr__(self) -> str:
        return f"<Metric {self.name} = {self.value} / {self.worked_time} = {self.speed}"

    @property
    def worked_time(self) -> float:
        if self.finished_at:
            return self.finished_at - self.started_at

        return 0.0

    @property
    def speed(self) -> float:
        if self.value and self.worked_time:
            return self.value / self.worked_time

        return 0.0

    def finish(self, value: Union[int, float]) -> None:
        self.value = value
        self.finished_at = time.monotonic()


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
    timeout: int

    future: Optional[asyncio.Future]
    metrics: DefaultDict[str, List[Metric]]
    values: Dict[str, Any]

    def __init__(self, timeout=5) -> None:
        self.timeout = timeout

        self.future = None
        self.metrics = defaultdict(list)
        self.values = {}

    def update(self, metric: Metric) -> None:
        self.ensure_started()
        self.metrics[metric.name].append(metric)

    def update_value(
            self,
            name: str,
            value: Any,
            is_need_to_update: Optional[Callable[[Any, Any], bool]] = None,
    ) -> None:
        """
        set value to metrics

        it there is a callback - value will rewrite only if callback returns True
        """
        is_need_to_update = is_need_to_update or (lambda x, y: True)
        prev = self.values.get(name)

        if is_need_to_update(prev, value):
            self.values[name] = value

    def ensure_started(self) -> None:
        if not self.future:
            self.future = asyncio.ensure_future(self.task())

    def show(self) -> None:
        for name, metrics_list in self.metrics.items():
            show_metrics(name, metrics_list)

        for name, value in self.values.items():
            show_value(name, value)

    async def task(self) -> NoReturn:
        while True:
            self.show()
            await asyncio.sleep(self.timeout)


def show_metrics(name: str, metrics_list: List[Metric]) -> None:
    if not metrics_list:
        return

    value = sum([metric.value for metric in metrics_list], 0.0)
    worked_time = sum([metric.worked_time for metric in metrics_list], 0.0)
    speed = value / worked_time if worked_time else 0.0

    logger.info(
        'Metrics',
        extra={
            'tag': 'METRICS',
            'metrics_name': name,
            'metrics_total_count': value,
            'metrics_total_time': worked_time,
            'per_worker_speed': speed,
        }
    )

    metrics_list.clear()


def show_value(name: str, value: Any) -> None:
    logger.info(
        'Metrics by values',
        extra={
            'tag': 'METRICS',
            'metrics_name': name,
            'metrics_value': value,
        }
    )


metrics = Metrics()
