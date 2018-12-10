from typing import Dict, Any

import pytest

from jsearch.tests.utils import get_dump

pytest_plugins = (
    'jsearch.tests.plugins.databases.main_db',
)


@pytest.fixture(scope="function")
def post_processing(db_connection_string, mocker):
    mocker.patch('time.sleep')
    mocker.patch(
        'jsearch.common.processing.erc20_transfer_logs.fetch_erc20_token_decimal_bulk',
        lambda updates: [setattr(update, 'decimals', 2) for update in updates] and updates
    )
    mocker.patch(
        'jsearch.common.processing.erc20_transfer_logs.fetch_erc20_balance_bulk',
        lambda updates: [setattr(update, 'value', 100) for update in updates] and updates
    )

    def _wrapper(dump):
        contracts: Dict[str, Dict[str, Any]] = {contract['address']: contract for contract in dump.get('contracts')}

        def get_contract(address: str):
            print(address)
            return contracts.get(address)

        mocker.patch('jsearch.common.processing.erc20_transfer_logs.get_contract', get_contract)

        from jsearch.post_processing.service import post_processing, ACTION_LOG_OPERATIONS, ACTION_LOG_EVENTS

        post_processing(ACTION_LOG_EVENTS)
        post_processing(ACTION_LOG_OPERATIONS)

        return get_dump(db_connection_string)

    return _wrapper
