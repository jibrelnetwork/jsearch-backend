import logging
from typing import List, Any, Tuple, Optional

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
