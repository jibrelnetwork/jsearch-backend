from typing import Optional, Type

import pytest


class InfiniteLoop(Exception):
    pass


@pytest.mark.parametrize(
    "version_id, exception", [
        ('20191015163320', None,),
        ('30191015163320', InfiniteLoop)
    ],
    ids=[
        'existed',
        'not-existed'
    ]
)
def test_migration_version_waiting(version_id, exception: Optional[Type[Exception]], mocker):
    from jsearch.syncer.utils import wait_for_version, execute

    attempts = 0

    def execute_mock(*args, **kwargs):
        nonlocal attempts
        attempts += 1
        result = execute(*args, **kwargs)

        if attempts > 5:
            raise InfiniteLoop

        return result

    mocker.patch('jsearch.syncer.utils.execute', execute_mock)

    try:
        wait_for_version(version_id, timeout=0.1)
    except InfiniteLoop as e:
        assert isinstance(e, exception)
    else:
        assert not exception
