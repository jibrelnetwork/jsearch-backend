import logging

log = logging.getLogger(__name__)


def split(iterable, size):
    return [iterable[i:i + size] for i in range(0, len(iterable), size)]
