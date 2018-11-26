from typing import Dict, Any

import pytest

from jsearch.tests.utils import get_dump

pytest_plugins = (
    'jsearch.tests.plugins.databases.main_db',
)


@pytest.fixture(scope="function")
def post_processing(db_connection_string, mocker):
    mocker.patch('time.sleep')
    mocker.patch('jsearch.common.processing.operations.update_token_info')
    mocker.patch('jsearch.common.processing.operations.update_token_holder_balance')

    def _wrapper(dump):
        contracts: Dict[str, Dict[str, Any]] = {contract['address']: contract for contract in dump.get('contracts')}

        def get_contract(address: str):
            return contracts.get(address)

        mocker.patch('jsearch.common.processing.logs.get_contract', get_contract)

        from jsearch.post_processing.service import post_processing
        post_processing()

        return get_dump(db_connection_string)

    return _wrapper
