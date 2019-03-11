from typing import Callable, Coroutine, Any

import pytest

from jsearch.typing import Logs

pytest_plugins = (
    'jsearch.tests.plugins.databases.main_db',
    'jsearch.tests.plugins.post_processing.side_effects'
)


@pytest.fixture(scope="function")
def post_processing_transfers(main_db_dump,
                              load_transfers,
                              mock_async_fetch_contracts,
                              mock_prefetch_decimals,
                              mock_fetch_erc20_balance_bulk) -> Callable[[Logs], Coroutine[Any, Any, None]]:
    from jsearch.post_processing.worker_transfers import handle_new_transfers

    async def _wrapper(logs: Logs):
        mock_async_fetch_contracts(main_db_dump['contracts'])

        await handle_new_transfers([logs])

    return _wrapper
