import logging

from typing import List, Any, Tuple, Optional, Iterable

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
