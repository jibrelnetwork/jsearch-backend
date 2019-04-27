from typing import NamedTuple


class DbStats(NamedTuple):
    is_healthy: bool


class RawDbStats(NamedTuple):
    is_healthy: bool


class LoopStats(NamedTuple):
    is_healthy: bool
    tasks_count: int


class KafkaStats(NamedTuple):
    is_healthy: bool


class NodeStats(NamedTuple):
    is_healthy: bool
