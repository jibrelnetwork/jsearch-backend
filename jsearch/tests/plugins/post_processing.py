from itertools import chain
from typing import List, Tuple, Callable, Coroutine, Any

import pytest
from sqlalchemy import select, and_
from sqlalchemy.orm import Query

from jsearch.common.tables import logs_t, token_transfers_t
from jsearch.post_processing.worker_transfers import handle_new_transfers
from jsearch.typing import Logs, Log, Transfers

pytest_plugins = (
    'jsearch.tests.plugins.databases.main_db',
)


@pytest.fixture(scope="function")
def load_logs(db_connection_string) -> Callable[[Logs], Logs]:
    from jsearch.common.utils import as_dicts
    from jsearch.syncer.database import MainDBSync

    def get_query(log: Log) -> Query:
        return select(logs_t.c).where(
            and_(
                logs_t.c.transaction_hash == log['transaction_hash'],
                logs_t.c.block_hash == log['block_hash'],
                logs_t.c.log_index == log['log_index']
            )
        )

    @as_dicts
    def _wrapper(logs) -> Logs:
        with MainDBSync(db_connection_string) as db:
            return [db.execute(get_query(log)).fetchone() for log in logs]

    return _wrapper


@pytest.fixture(scope="function")
def load_transfers(db_connection_string) -> Callable[[Logs], Transfers]:
    from jsearch.common.utils import as_dicts
    from jsearch.syncer.database import MainDBSync

    def get_queries(log: Log) -> List[Query]:
        return [
            select([
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
            ]).where(
                and_(
                    token_transfers_t.c.transaction_hash == log['transaction_hash'],
                    token_transfers_t.c.log_index == log['log_index'],
                    token_transfers_t.c.address == log['event_args']['from']
                )
            ),
            select([
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
            ]).where(
                and_(
                    token_transfers_t.c.transaction_hash == log['transaction_hash'],
                    token_transfers_t.c.log_index == log['log_index'],
                    token_transfers_t.c.address == log['event_args']['to']
                )
            ),
        ]

    def _wrapper(logs) -> Transfers:
        with MainDBSync(db_connection_string) as db:
            results = []
            for log in logs:
                for query in get_queries(log):
                    result = db.execute(query).fetchone()

                    result = dict(result)
                    result['token_value'] = int(result['token_value'])

                    results.append(result)
            return results

    return _wrapper


@pytest.fixture(scope="function")
def post_processing_logs(mocker, load_logs, kafka_buffer) \
        -> Callable[[List[Logs]], Coroutine[Any, Any, Tuple[Logs, Transfers]]]:
    from jsearch.post_processing.worker_logs import handle_transaction_logs
    from jsearch.service_bus import ROUTE_HANDLE_ERC20_TRANSFERS

    async def _wrapper(logs) -> Tuple[Logs, Transfers]:
        mocker.patch('time.sleep')

        await handle_transaction_logs([logs])

        logs = load_logs(logs)
        transfers = kafka_buffer[ROUTE_HANDLE_ERC20_TRANSFERS][:]
        return logs, list(chain(*transfers))

    return _wrapper


@pytest.fixture(scope="function")
def post_processing_transfers(mocker, main_db_dump, load_transfers) -> Callable[[Logs], Coroutine[Any, Any, Transfers]]:
    async def get_contracts(service_bus, logs: Logs):
        result = []
        for log in logs:
            contracts = [contract for contract in main_db_dump['contracts'] if contract['address'] == log['address']]
            contract = contracts and contracts[0] or {'address': log['address']}
            result.append(contract)
        return result

    def prefetch_decimals(contracts):
        return {contract['address']: dict(decimals=10, **contract) for contract in contracts}

    def fetch_erc20_balance_bulk(updates):
        for update in updates:
            update.value = 100
        return updates

    async def _wrapper(logs: Logs):
        mocker.patch('time.sleep')
        mocker.patch('jsearch.common.processing.erc20_balances.fetch_erc20_balance_bulk', fetch_erc20_balance_bulk)
        mocker.patch('jsearch.post_processing.worker_transfers.fetch_contracts', get_contracts)
        mocker.patch('jsearch.post_processing.worker_transfers.prefetch_decimals', prefetch_decimals)

        await handle_new_transfers([logs])
        return load_transfers(logs)

    return _wrapper


@pytest.fixture(scope="function")
def post_processing(post_processing_logs, post_processing_transfers, load_logs) \
        -> Callable[[Logs], Coroutine[Any, Any, Tuple[Logs, Transfers]]]:
    async def _wrapper(logs: Logs) -> Tuple[Logs, Transfers]:
        logs, transfers = await post_processing_logs(logs)
        transfers = await post_processing_transfers(transfers)

        return logs, transfers

    return _wrapper
