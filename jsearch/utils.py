import logging
from functools import wraps
from pprint import saferepr

log = logging.getLogger(__name__)


def suppress_exception(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception:
            log.exception(
                f"{func.__name__} was failed with args %s and kwargs %s",
                saferepr(args), saferepr(kwargs)
            )

    return wrapper


def split(iterable, size):
    return [iterable[i:i + size] for i in range(0, len(iterable), size)]
