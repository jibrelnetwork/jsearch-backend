import logging

log = logging.getLogger(__name__)


def split(iterable, size):
    if isinstance(iterable, set):
        iterable = list(iterable)
    return [iterable[i:i + size] for i in range(0, len(iterable), size)]
