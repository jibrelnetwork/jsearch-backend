import random

import pytest
import requests
import requests_mock
from requests import Session

pytest_plugins = (
    'jsearch.tests.plugins.ether',
)


@pytest.fixture()
def request_mock_adapter():
    from jsearch import settings
    from web3.utils.request import _get_session, _session_cache

    adapter = requests_mock.Adapter()

    session: Session = _get_session(settings.ETH_NODE_URL)
    session.mount('https://', adapter)
    session.mount('http://', adapter)

    yield adapter

    _session_cache.clear()


def test_fetch_decimals_from_eth_node(mocker, ether_address_generator, request_mock_adapter):
    # given
    from jsearch import settings
    from jsearch.common.contracts import ERC20_ABI
    from jsearch.common.processing.erc20_transfer_logs import BalanceUpdate, fetch_erc20_token_decimal_bulk
    from web3.utils.request import make_post_request

    mocker.patch('time.sleep')
    post_mock = mocker.patch('web3.providers.rpc.make_post_request', side_effect=make_post_request)
    request_mock_adapter.register_uri('POST', settings.ETH_NODE_URL, exc=requests.exceptions.ConnectTimeout)

    update = BalanceUpdate(
        account_address=next(ether_address_generator),
        token_address=next(ether_address_generator),
        block=random.randint(1, 6800000),
        abi=ERC20_ABI
    )
    updates = [update]

    # when
    with pytest.raises(requests.exceptions.ConnectTimeout):
        fetch_erc20_token_decimal_bulk(updates=updates)

    # then
    assert post_mock.call_count == 10


def test_fetch_balances_from_eth_node(mocker, ether_address_generator, request_mock_adapter):
    # given
    from jsearch import settings
    from jsearch.common.contracts import ERC20_ABI
    from jsearch.common.processing.erc20_transfer_logs import BalanceUpdate, fetch_erc20_balance_bulk
    from web3.utils.request import make_post_request

    mocker.patch('time.sleep')
    post_mock = mocker.patch('web3.providers.rpc.make_post_request', side_effect=make_post_request)
    request_mock_adapter.register_uri('POST', settings.ETH_NODE_URL, exc=requests.exceptions.ConnectTimeout)

    update = BalanceUpdate(
        account_address=next(ether_address_generator),
        token_address=next(ether_address_generator),
        block=random.randint(1, 6800000),
        abi=ERC20_ABI
    )
    updates = [update]

    # when
    with pytest.raises(requests.exceptions.ConnectTimeout):
        fetch_erc20_balance_bulk(updates=updates)

    # then
    assert post_mock.call_count == 10
