import random

import pytest
import requests
import requests_mock

from jsearch import settings

pytest_plugins = (
    'jsearch.tests.plugins.ether',
)


@pytest.fixture()
def connect_timeout_on_geth_node():
    with requests_mock.Mocker() as m:
        m.post(settings.ETH_NODE_URL, exc=requests.exceptions.ConnectTimeout)

        yield


@pytest.mark.usefixtures('connect_timeout_on_geth_node')
def test_retries_when_fetch_decimals_from_eth_node(mocker, main_db_data):
    # given
    from jsearch.common.processing.erc20_balances import fetch_erc20_token_decimal_bulk

    mocker.patch('time.sleep')
    post_mock = mocker.patch('requests.post', side_effect=requests.post)

    contracts = main_db_data['contracts']
    # when
    with pytest.raises(requests.exceptions.ConnectTimeout):
        fetch_erc20_token_decimal_bulk(contracts)

    # then
    assert post_mock.call_count == 10


@pytest.mark.usefixtures('connect_timeout_on_geth_node')
def test_retries_when_fetch_balances_from_eth_node(mocker, ether_address_generator):
    # given
    import requests
    from jsearch.common.contracts import ERC20_ABI
    from jsearch.common.processing.erc20_balances import BalanceUpdate, fetch_erc20_balance_bulk

    mocker.patch('time.sleep')
    post_mock = mocker.patch('requests.post', side_effect=requests.post)

    update = BalanceUpdate(
        account_address=next(ether_address_generator),
        token_address=next(ether_address_generator),
        block=random.randint(1, 6800000),
        abi=ERC20_ABI,
        decimals=2,
    )
    updates = [update]

    # when
    with pytest.raises(requests.exceptions.ConnectTimeout):
        fetch_erc20_balance_bulk(updates=updates)

    # then
    assert post_mock.call_count == 10
