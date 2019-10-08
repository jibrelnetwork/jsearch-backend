from typing import NamedTuple, Optional, List


class DbStats(NamedTuple):
    is_healthy: bool


class RawDbStats(NamedTuple):
    is_healthy: bool


class LoopStats(NamedTuple):
    is_healthy: bool


class NodeStats(NamedTuple):
    is_healthy: bool


class BlockRange(NamedTuple):
    start: int
    end: Optional[int]

    def __str__(self):
        return f"{self.start}-{self.end if self.end is not None else ''}"


class ChainStats(NamedTuple):
    is_healthy: bool
    chain_holes: Optional[List]


class LagStats(NamedTuple):
    is_healthy: bool
    lag: Optional[int]
