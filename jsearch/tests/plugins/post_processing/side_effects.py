import pytest

from jsearch.typing import Transfers, Contracts


@pytest.fixture()
def mock_async_fetch_contracts(mocker):
    def _wrapper(contracts: Contracts):
        async def get_contracts(transfers: Transfers):
            nonlocal contracts

            result = []
            for transfer in transfers:
                contracts = [item for item in contracts if item['address'] == transfer['address']]
                contract = contracts and contracts[0] or {'address': transfer['address']}
                result.append(contract)
            return result

        mocker.patch('jsearch.post_processing.worker_transfers.async_fetch_contracts', get_contracts)

    return _wrapper


@pytest.fixture()
def mock_fetch_contracts(mocker):
    def _wrapper(contracts: Contracts):
        def get_contracts(transfers: Transfers):
            nonlocal contracts

            result = []
            for transfer in transfers:
                contracts = [item for item in contracts if item['address'] == transfer['address']]
                contract = contracts and contracts[0] or {'address': transfer['address']}
                result.append(contract)
            return result

        mocker.patch('jsearch.post_processing.worker_logs.fetch_contracts', get_contracts)

    return _wrapper


@pytest.fixture()
def mock_prefetch_decimals(mocker, main_db_data):
    def prefetch_decimals(contracts):
        return {contract['address']: dict(decimals=10, **contract) for contract in contracts}

    mocker.patch('jsearch.post_processing.worker_transfers.prefetch_decimals', prefetch_decimals)
    mocker.patch('jsearch.post_processing.worker_logs.prefetch_decimals', prefetch_decimals)


@pytest.fixture()
def mock_fetch_erc20_balance_bulk(mocker):
    def fetch_erc20_balance_bulk(updates):
        for update in updates:
            update.value = 100
        return updates

    mocker.patch('jsearch.common.processing.erc20_balances.fetch_erc20_balance_bulk', fetch_erc20_balance_bulk)
