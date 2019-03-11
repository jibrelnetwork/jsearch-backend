from typing import Callable, Coroutine, Any

import pytest

from jsearch.post_processing.worker_transfers import handle_new_transfers
from jsearch.typing import Logs, Transfers

pytest_plugins = (
    'jsearch.tests.plugins.databases.main_db',
    'jsearch.tests.plugins.post_processing.side_effects'
)


@pytest.fixture(scope="function")
def post_processing_transfers(mocker,
                              main_db_dump,
                              load_transfers,
                              mock_async_fetch_contracts,
                              mock_fetch_erc20_balance_bulk) -> Callable[[Logs], Coroutine[Any, Any, Transfers]]:
    async def _wrapper(logs: Logs):
        mocker.patch('time.sleep')
        mock_async_fetch_contracts(main_db_dump['contracts'])

        await handle_new_transfers([logs])
        return load_transfers(logs)

    return _wrapper
