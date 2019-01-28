import random

import pytest
from aioresponses import aioresponses
from asynctest import CoroutineMock

pytest_plugins = (
    'jsearch.tests.plugins.ether',
    'jsearch.tests.plugins.http',
)


@pytest.fixture()
def connect_timeout_on_geth_node(mock_aiohttp: aioresponses) -> None:
    from jsearch import settings
    from aiohttp import ClientConnectionError

    mock_aiohttp.post(url=f'{settings.ETH_NODE_URL}', exception=ClientConnectionError(), repeat=True)


@pytest.mark.usefixtures('connect_timeout_on_geth_node')
async def test_retries_when_fetch_decimals_from_eth_node(mocker, main_db_data):
    # given
    import aiohttp
    from jsearch.common.processing.erc20_balances import fetch_erc20_token_decimal_bulk
    from aiohttp import ClientError

    # mocker.patch('asyncio.sleep', AsyncMock)
    mocker.patch('asyncio.sleep', CoroutineMock())
    post_mock = mocker.patch('jsearch.common.rpc.request', side_effect=aiohttp.request)

    contracts = main_db_data['contracts']
    # when
    with pytest.raises(ClientError):
        await fetch_erc20_token_decimal_bulk(contracts)

    # then
    assert post_mock.call_count == 10


@pytest.mark.usefixtures('connect_timeout_on_geth_node')
async def test_retries_when_fetch_balances_from_eth_node(mocker, ether_address_generator):
    # given
    import aiohttp
    from aiohttp import ClientError
    from jsearch.common.contracts import ERC20_ABI
    from jsearch.common.processing.erc20_balances import BalanceUpdate, fetch_erc20_balance_bulk

    # mocker.patch('asyncio.sleep', AsyncMock)
    mocker.patch('asyncio.sleep', CoroutineMock())
    post_mock = mocker.patch('jsearch.common.rpc.request', side_effect=aiohttp.request)

    update = BalanceUpdate(
        account_address=next(ether_address_generator),
        token_address=next(ether_address_generator),
        block=random.randint(1, 6800000),
        abi=ERC20_ABI,
        decimals=2,
    )
    updates = [update]

    # when
    with pytest.raises(ClientError):
        await fetch_erc20_balance_bulk(updates=updates)

    # then
    assert post_mock.call_count == 10
