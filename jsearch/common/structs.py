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

    # FIXME (nickgashkov):
    #   * `other` could be something else than `BlockRange`.
    #   * `BlockRange.end` could be `None`.
    def __gt__(self, other: 'BlockRange'):  # type: ignore
        return other.end > self.end  # type: ignore

    # FIXME (nickgashkov):
    #   * `item` could be `Any`.
    #   * `BlockRange.end` could be `None`.
    def __contains__(self, item: int) -> bool:  # type: ignore
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
        return self.start <= item <= self.end  # type: ignore

    def __len__(self):
        """
        >>> len(BlockRange(0, 5))
        6
        >>> len(BlockRange(0, 0))
        1
        """
        if not self.is_closed:
            raise ValueError('To get a length - need to specify all borders')
        return (self.end + 1) - self.start

    @property
    def is_closed(self) -> bool:
        """
        >>> BlockRange(0, None).is_closed
        False
        >>> BlockRange(None, 0).is_closed
        False
        >>> BlockRange(0, 0).is_closed
        True
        >>> BlockRange(0, 100).is_closed
        True
        """
        return self.start is not None and self.end is not None

    def as_range(self) -> List[int]:
        """
        >>> BlockRange(0, 5).as_range()
        [0, 1, 2, 3, 4, 5]
        >>> BlockRange(0, 0).as_range()
        [0]
        """
        if not self.is_closed:
            raise ValueError('To get a range - need to specify all borders')
        return list(range(self.start, self.end + 1))  # type: ignore


class ChainStats(NamedTuple):
    is_healthy: bool
    chain_holes: List


class LagStats(NamedTuple):
    is_healthy: bool
    lag: Optional[int]
