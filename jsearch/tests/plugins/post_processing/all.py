from typing import Tuple, Callable, Coroutine, Any

import pytest

from jsearch.typing import Logs, Transfers

pytest_plugins = (
    'jsearch.tests.plugins.databases.main_db',
    'jsearch.tests.plugins.post_processing.logs',
    'jsearch.tests.plugins.post_processing.transfers'
)


@pytest.fixture(scope="function")
def post_processing(post_processing_logs, post_processing_transfers, load_logs) \
        -> Callable[[Logs], Coroutine[Any, Any, Tuple[Logs, Transfers]]]:
    async def _wrapper(logs: Logs) -> Tuple[Logs, Transfers]:
        logs, transfers = await post_processing_logs(logs)
        transfers = await post_processing_transfers(transfers)

        return logs, transfers

    return _wrapper
