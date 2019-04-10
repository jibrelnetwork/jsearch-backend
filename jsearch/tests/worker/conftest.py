from collections import defaultdict
from typing import AsyncGenerator

import pytest
from aiopg.sa import Engine

from jsearch.worker.__main__ import service


@pytest.fixture()
def mock_service_bus_get_contracts(mocker):
    from jsearch.service_bus import service_bus
    store = {}

    async def get_contracts(addresses, fields):
        return [contract for contract in store.values() if contract['address'] in addresses]

    mocker.patch.object(service_bus, 'get_contracts', get_contracts)
    return store


@pytest.fixture()
def mock_fetch_erc20_balance_bulk(mocker):
    store = defaultdict(dict)

    def fetch_erc20_balance_bulk(chunk, block):
        for update in chunk:
            update.value = store[update.token_address][update.account_address]

        return chunk

    mocker.patch('jsearch.worker.token_balances.fetch_erc20_balance_bulk', fetch_erc20_balance_bulk)

    return store


@pytest.fixture(scope='function')
async def worker(sa_engine: Engine) -> AsyncGenerator[Engine, None]:
    service.engine = sa_engine
    try:
        yield service
    finally:
        service.engine = None
