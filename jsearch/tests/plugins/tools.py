from typing import List

import pytest

from jsearch.tests.utils import get_dump

pytest_plugins = (
    'jsearch.tests.plugins.databases.main_db',
)


@pytest.fixture(scope="function")
def post_processing(db_connection_string, mocker):
    async def fetch_erc20_balance_bulk(contracts):
        for contract in contracts:
            contract.value = 100
        return contracts

    async def _wrapper(dump):
        mocker.patch('time.sleep')
        mocker.patch('jsearch.common.processing.erc20_balances.fetch_erc20_balance_bulk', fetch_erc20_balance_bulk)
       
        contracts = {contract['address']: {'decimals': 2, **contract} for contract in dump.get('contracts')}

        async def get_contracts(addresses: List[str]):
            return {address: contracts.get(address) for address in addresses}

        mocker.patch('jsearch.post_processing.service.fetch_contracts', get_contracts)

        from jsearch.post_processing.service import post_processing, ACTION_LOG_OPERATIONS, ACTION_LOG_EVENTS

        await post_processing(ACTION_LOG_EVENTS)
        await post_processing(ACTION_LOG_OPERATIONS)

        return get_dump(db_connection_string)

    return _wrapper
