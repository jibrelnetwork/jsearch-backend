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

    def __gt__(self, other: 'BlockRange'):
        return other.end > self.end

    def __contains__(self, item: int) -> bool:
        """
        >>> 5 in BlockRange(0, 10)
        True
        >>> 0 in BlockRange(0, 10)
        True
        >>> 10 in BlockRange(0, 10)
        True
        >>> 1 in BlockRange(5, 10)
        False
        >>> 11 in BlockRange(5, 10)
        False
        """
        return self.start <= item <= self.end


class ChainStats(NamedTuple):
    is_healthy: bool
    chain_holes: Optional[List]


class LagStats(NamedTuple):
    is_healthy: bool
    lag: Optional[int]
