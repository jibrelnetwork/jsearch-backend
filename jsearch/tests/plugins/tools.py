from typing import Dict, Any, List

import pytest

from jsearch.tests.utils import get_dump

pytest_plugins = (
    'jsearch.tests.plugins.databases.main_db',
)


@pytest.fixture(scope="function")
def post_processing(db_connection_string, mocker):
    mocker.patch('time.sleep')
    mocker.patch(
        'jsearch.common.processing.erc20_balances.fetch_erc20_token_decimal_bulk',
        lambda contracts: [contract.update(decimals=2) for contract in contracts] and contracts
    )
    mocker.patch(
        'jsearch.common.processing.erc20_balances.fetch_erc20_balance_bulk',
        lambda updates: [setattr(update, 'value', 100) for update in updates] and updates
    )

    def _wrapper(dump):
        contracts: Dict[str, Dict[str, Any]] = {contract['address']: contract for contract in dump.get('contracts')}

        def get_contract(addresses: List[str]):
            return [contracts.get(address) for address in addresses]

        mocker.patch('jsearch.common.processing.erc20_balances.get_contracts', get_contract)

        from jsearch.post_processing.service import post_processing, ACTION_LOG_OPERATIONS, ACTION_LOG_EVENTS

        post_processing(ACTION_LOG_EVENTS)
        post_processing(ACTION_LOG_OPERATIONS)

        return get_dump(db_connection_string)

    return _wrapper
