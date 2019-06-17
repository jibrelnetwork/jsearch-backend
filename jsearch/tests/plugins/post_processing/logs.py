from typing import Callable, List, Coroutine, Any

import pytest
from sqlalchemy import select, and_

from jsearch.typing import Logs

pytest_plugins = (
    'jsearch.tests.plugins.databases.main_db',
    'jsearch.tests.plugins.post_processing.side_effects'
)


# @pytest.fixture(scope="function")
# def load_logs(db_connection_string) -> Callable[[Logs], Logs]:
#     from jsearch.common.tables import logs_t
#     from jsearch.typing import Logs
#
#     from jsearch.common.utils import as_dicts
#     from jsearch.common.database import MainDBSync
#
#     @as_dicts
#     def _wrapper(tx_hash, block_hash) -> Logs:
#         with MainDBSync(db_connection_string) as db:
#             query = select(logs_t.c).where(
#                 and_(
#                     logs_t.c.transaction_hash == tx_hash,
#                     logs_t.c.block_hash == block_hash,
#                 )
#             )
#             return db.execute(query).fetchall()
#
#     return _wrapper


@pytest.fixture(scope="function")
def post_processing_logs(mocker,
                         main_db_dump,
                         kafka_buffer,
                         mock_fetch_contracts,
                         mock_prefetch_decimals) -> Callable[[List[Logs]], Coroutine[Any, Any, None]]:
    from jsearch.post_processing.worker_logs import handle_transaction_logs

    async def _wrapper(logs):
        mocker.patch('time.sleep')
        mock_fetch_contracts(main_db_dump['contracts'])

        await handle_transaction_logs([logs])

    return _wrapper
