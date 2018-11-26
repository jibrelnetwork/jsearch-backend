import subprocess
from functools import wraps
from typing import List, Any


def get_git_revesion_num():
    label = subprocess.check_output(['git', 'describe', '--always']).strip()
    return label.decode()


def as_dicts(func):
    @wraps(func)
    def _wrapper(*args, **kwargs):
        result: List[Any] = func(*args, **kwargs)
        return [dict(item) for item in result]

    return _wrapper
