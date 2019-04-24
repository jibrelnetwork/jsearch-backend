from typing import Callable, Coroutine, Any

import pytest
from sqlalchemy import and_, select

from jsearch.typing import Logs, Transfers

pytest_plugins = (
    'jsearch.tests.plugins.databases.main_db',
    'jsearch.tests.plugins.post_processing.side_effects'
)


@pytest.fixture(scope="function")
def load_transfers(db_connection_string) -> Callable[[Logs], Transfers]:
    from jsearch.common.tables import token_transfers_t
    from jsearch.typing import Transfers

    from jsearch.syncer.database import MainDBSync

    fields = [
        token_transfers_t.c.address,
        token_transfers_t.c.transaction_hash,
        token_transfers_t.c.transaction_index,
        token_transfers_t.c.log_index,
        token_transfers_t.c.block_number,
        token_transfers_t.c.block_hash,
        token_transfers_t.c.timestamp,
        token_transfers_t.c.from_address,
        token_transfers_t.c.to_address,
        token_transfers_t.c.token_address,
        token_transfers_t.c.token_value,
        token_transfers_t.c.token_decimals,
        token_transfers_t.c.token_name,
        token_transfers_t.c.token_symbol,
        token_transfers_t.c.is_forked,
        token_transfers_t.c.status,
    ]

    def _wrapper(tx_hash, block_hash) -> Transfers:
        with MainDBSync(db_connection_string) as db:
            query = select(fields).where(
                and_(
                    token_transfers_t.c.transaction_hash == tx_hash,
                    token_transfers_t.c.block_hash == block_hash
                )
            )
            result = db.execute(query).fetchall()
            result = [dict(item) for item in result]
            for item in result:
                item['token_value'] = int(item['token_value'])

        return result

    return _wrapper


@pytest.fixture(scope="function")
def post_processing_transfers(main_db_dump,
                              load_transfers,
                              mock_fetch_contracts,
                              mock_prefetch_decimals,
                              mock_fetch_erc20_balance_bulk) -> Callable[[Logs], Coroutine[Any, Any, None]]:
    from jsearch.post_processing.worker_transfers import handle_new_transfers

    async def _wrapper(logs: Logs):
        mock_fetch_contracts(main_db_dump['contracts'])

        await handle_new_transfers([logs])

    return _wrapper
