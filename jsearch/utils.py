import logging
from functools import wraps
from pprint import pformat

log = logging.getLogger(__name__)


def suppress_exception(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception:
            log.exception(f"{func.__name__} was failed with args %s and kwargs %s", pformat(args), pformat(kwargs))

    return wrapper
