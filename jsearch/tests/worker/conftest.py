from collections import defaultdict
from typing import AsyncGenerator

import pytest
from aiopg.sa import Engine


@pytest.fixture()
def mock_service_bus_get_contracts(mocker):
    from jsearch.service_bus import service_bus
    store = {}

    async def get_contracts(addresses):
        return [contract for contract in store.values() if contract['address'] in addresses]

    mocker.patch.object(service_bus, 'get_contracts', get_contracts)
    return store


@pytest.fixture()
def mock_fetch_erc20_balance_bulk(mocker):
    store = defaultdict(dict)

    def fetch_erc20_token_balance(contract_abi, token_address, account_address):
        if account_address in store[token_address]:
            return store[token_address][account_address]

    mocker.patch('jsearch.worker.__main__.fetch_erc20_token_balance', fetch_erc20_token_balance)

    return store


@pytest.fixture(scope='function')
async def worker(sa_engine: Engine) -> AsyncGenerator[Engine, None]:
    from jsearch.worker.__main__ import service

    service.engine = sa_engine
    try:
        yield service
    finally:
        service.engine = None
