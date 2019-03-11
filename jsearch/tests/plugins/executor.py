from concurrent.futures import Future

import pytest


@pytest.fixture()
def mock_executor(mocker):
    class FakeExecutor:

        def submit(self, fn, *args):
            result = fn(*args)
            future = Future()
            future.set_result(result)
            return future

    from jsearch.multiprocessing import executor
    mocker.patch.object(executor, 'get', lambda: FakeExecutor())
