from typing import NamedTuple, Optional, List


class DbStats(NamedTuple):
    is_healthy: bool


class RawDbStats(NamedTuple):
    is_healthy: bool


class LoopStats(NamedTuple):
    is_healthy: bool


class NodeStats(NamedTuple):
    is_healthy: bool


class SyncRange(NamedTuple):
    start: int
    end: Optional[int]


class ChainStats(NamedTuple):
    is_healthy: bool
    chain_holes: Optional[List]
