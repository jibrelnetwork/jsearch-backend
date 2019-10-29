import pytest


@pytest.fixture(scope='function', autouse=True)
def reset_timers():
    from jsearch.common.utils import timers
    timers.flush("Force flush")
