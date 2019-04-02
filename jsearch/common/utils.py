import asyncio
import subprocess
from functools import wraps
from typing import List, Any


def get_git_revesion_num():
    label = subprocess.check_output(['git', 'describe', '--always']).strip()
    return label.decode()


def as_dicts(func):
    def to_dics(result):
        return [dict(item) for item in result]

    if asyncio.iscoroutinefunction(func):
        @wraps(func)
        async def _wrapper(*args, **kwargs):
            result: List[Any] = await func(*args, **kwargs)
            return to_dics(result)
    else:
        @wraps(func)
        def _wrapper(*args, **kwargs):
            result: List[Any] = func(*args, **kwargs)
            return to_dics(result)

    return _wrapper
