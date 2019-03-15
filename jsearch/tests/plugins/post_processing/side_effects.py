from typing import List

import pytest

from jsearch.common.contracts import ERC20_ABI
from jsearch.typing import Contracts


@pytest.fixture()
def mock_fetch_contracts(mocker):
    def _wrapper(contracts: Contracts):
        async def get_contracts(addresses: List[str]):
            result = []
            for address in addresses:
                matched_contracts = [item for item in contracts if item['address'] == address]
                contract = matched_contracts and matched_contracts[0] or {
                    'address': address,
                    'abi': ERC20_ABI,
                    'token_decimals': 18,
                }
                result.append(contract)
            return result

        mocker.patch('jsearch.post_processing.worker_transfers.fetch_contracts', get_contracts)

    return _wrapper


@pytest.fixture()
def mock_prefetch_decimals(mocker, main_db_data):
    def prefetch_decimals(contracts):
        return {contract['address']: dict(decimals=10, **contract) for contract in contracts}

    mocker.patch('jsearch.post_processing.worker_transfers.prefetch_decimals', prefetch_decimals)


@pytest.fixture()
def mock_fetch_erc20_balance_bulk(mocker):
    def fetch_erc20_balance_bulk(updates):
        for update in updates:
            update.value = 100
        return updates

    mocker.patch('jsearch.common.processing.erc20_balances.fetch_erc20_balance_bulk', fetch_erc20_balance_bulk)
