from urllib.parse import urlencode

import pytest
import time
from aiohttp.test_utils import TestClient
from typing import Callable, Optional, List, Dict, Any, Tuple

from jsearch.api.tests.utils import parse_url
from jsearch.common.wallet_events import WalletEventType, make_event_index
from jsearch.tests.plugins.databases.factories.blocks import BlockFactory
from jsearch.tests.plugins.databases.factories.common import generate_address
from jsearch.tests.plugins.databases.factories.transactions import TransactionFactory
from jsearch.tests.plugins.databases.factories.wallet_events import WalletEventsFactory
from jsearch.typing import AnyCoroutine


TIMESTAMP = int(time.time())


def get_tx_hashes(data: List[Dict[str, Any]]) -> List[int]:
    return [item['transactionHash'] for item in data]


@pytest.fixture()
def create_eth_transfers(
        block_factory,
        transaction_factory,
        internal_transaction_factory,
        wallet_events_factory
) -> Callable[[str], AnyCoroutine]:
    async def create_env(account: str,
                         block_count=5,
                         tx_in_block=4) -> None:
        for block_i in range(block_count):
            timestamp = TIMESTAMP + block_i
            block = block_factory.create(timestamp=timestamp)
            for i in range(0, tx_in_block):
                # WTF: The hash is exposed via API and used for order checking.
                kwargs = {
                    'hash': '0x' + str(make_event_index(block_i, i, 0)),
                    'transaction_index': i,
                    'from_': account,
                }

                tx, __ = transaction_factory.create_for_block(block=block, **kwargs)
                wallet_events_factory.create_eth_transfer(tx)

    return create_env


URL = '/v1/accounts/<address>/eth_transfers?{params}'


@pytest.mark.parametrize(
    'url, transfers_on_page, next_link, link',
    [
        (
                URL.format(params=urlencode({'limit': 3})),
                [
                    (4, 3, 0),
                    (4, 2, 0),
                    (4, 1, 0),
                ],
                URL.format(params=urlencode({
                    'event_index': make_event_index(4, 0, 0),
                    'limit': 3,
                    'order': 'desc'
                })),
                URL.format(params=urlencode({
                    'event_index': make_event_index(4, 3, 0),
                    'limit': 3,
                    'order': 'desc'
                })),
        ),
        (
                URL.format(params=urlencode({'timestamp': TIMESTAMP, 'limit': 5, 'order': 'asc'})),
                [
                    (0, 0, 0),
                    (0, 1, 0),
                    (0, 2, 0),
                    (0, 3, 0),
                    (1, 0, 0),
                ],
                URL.format(params=urlencode({
                    'event_index': make_event_index(1, 1, 0),
                    'limit': 5,
                    'order': 'asc'
                })),
                URL.format(params=urlencode({
                    'event_index': make_event_index(0, 0, 0),
                    'limit': 5,
                    'order': 'asc'
                })),
        ),
        (
                URL.format(params=urlencode({'order': 'asc', 'limit': 3})),
                [
                    (0, 0, 0),
                    (0, 1, 0),
                    (0, 2, 0),
                ],
                URL.format(params=urlencode({
                    'event_index': make_event_index(0, 3, 0),
                    'limit': 3,
                    'order': 'asc'
                })),
                URL.format(params=urlencode({
                    'event_index': make_event_index(0, 0, 0),
                    'limit': 3,
                    'order': 'asc'
                })),
        ),
        (
                URL.format(params=urlencode({'block_number': 3, 'limit': 3})),
                [
                    (3, 3, 0),
                    (3, 2, 0),
                    (3, 1, 0),
                ],
                URL.format(params=urlencode({
                    'event_index': make_event_index(3, 0, 0),
                    'limit': 3,
                    'order': 'desc'
                })),
                URL.format(params=urlencode({
                    'event_index': make_event_index(3, 3, 0),
                    'limit': 3,
                    'order': 'desc'
                })),
        ),
        (
                URL.format(params=urlencode({
                    'event_index': make_event_index(3, 1, 0),
                    'limit': 3
                })),
                [
                    (3, 1, 0),
                    (3, 0, 0),
                    (2, 3, 0),
                ],
                URL.format(params=urlencode({
                    'event_index': make_event_index(2, 2, 0),
                    'limit': 3,
                    'order': 'desc'
                })),
                URL.format(params=urlencode({
                    'event_index': make_event_index(3, 1, 0),
                    'limit': 3,
                    'order': 'desc'
                })),
        ),
        (
                URL.format(params=urlencode({'block_number': 'latest', 'limit': 3})),
                [
                    (4, 3, 0),
                    (4, 2, 0),
                    (4, 1, 0),
                ],
                URL.format(params=urlencode({
                    'event_index': make_event_index(4, 0, 0),
                    'limit': 3,
                    'order': 'desc'
                })),
                URL.format(params=urlencode({
                    'event_index': make_event_index(4, 3, 0),
                    'limit': 3,
                    'order': 'desc'
                })),
        ),
    ],
    ids=[
        URL.format(params=urlencode({'limit': 3})),
        URL.format(params=urlencode({'timestamp': TIMESTAMP, 'limit': 5, 'order': 'asc'})),
        URL.format(params=urlencode({'order': 'asc', 'limit': 3})),
        URL.format(params=urlencode({'block_number': 3, 'limit': 3})),
        URL.format(params=urlencode({'event_index': make_event_index(3, 1, 0), 'limit': 3})),
        URL.format(params=urlencode({'block_number': 'latest', 'limit': 3})),
    ]
)
async def test_get_wallet_events_pagination(
        cli,
        account_factory,
        create_eth_transfers,
        url,
        transfers_on_page: List[Tuple[int, int, int]],
        next_link: str,
        link: str
):
    # given
    account = account_factory.create()
    await create_eth_transfers(account.address)

    # when
    resp = await cli.get(url.replace("/<address>/", f"/{account.address}/"))
    resp_json = await resp.json()

    # then
    assert resp.status == 200
    assert resp_json['status']['success']

    assert parse_url(resp_json['paging']['next']) == parse_url(next_link.replace("/<address>/", f"/{account.address}/"))
    assert parse_url(resp_json['paging']['link']) == parse_url(link.replace("/<address>/", f"/{account.address}/"))

    assert get_tx_hashes(resp_json['data']) == ['0x' + str(make_event_index(*pnt)) for pnt in transfers_on_page]


@pytest.mark.parametrize(
    "target_limit, expected_items_count, expected_errors",
    (
            (None, 20, []),
            (19, 19, []),
            (20, 20, []),
            (21, None, [
                {
                    "field": "limit",
                    "message": "Must be between 1 and 20.",
                    "code": "INVALID_LIMIT_VALUE",
                }
            ]),
    ),
    ids=[
        "limit=None --- 20 rows returned",
        "limit=19   --- 19 rows returned",
        "limit=20   --- 20 rows returned",
        "limit=21   --- error is returned",
    ],
)
async def test_get_accounts_eth_transfers_limits(
        cli: TestClient,
        block_factory: BlockFactory,
        wallet_events_factory: WalletEventsFactory,
        transaction_factory: TransactionFactory,
        target_limit: Optional[int],
        expected_items_count: int,
        expected_errors: List[Dict[str, str]],
):
    # given
    address = '0xcd424c53f5dc7d22cdff536309c24ad87a97e6af'

    block = block_factory.create(number=1)
    tx, _ = transaction_factory.create_for_block(block=block, timestamp=100)
    wallet_events_factory.create_batch(
        25,
        block_number=1,
        address=address,
        type=WalletEventType.ETH_TRANSFER,
        event_data={'amount': '6000', 'sender': '0xaaa', 'recipient': address},
        tx_data=None,
        tx_hash=tx.hash,
    )

    # when
    reqv_params = 'block_number=latest'

    if target_limit is not None:
        reqv_params += f'&limit={target_limit}'

    resp = await cli.get(f'/v1/accounts/{address}/eth_transfers?{reqv_params}')
    resp_json = await resp.json()

    # then
    observed_errors = resp_json['status']['errors']
    if resp_json['data'] is None:
        observed_items_count = None
    else:
        observed_items_count = len(resp_json['data'])

    assert (observed_errors, observed_items_count) == (expected_errors, expected_items_count)


@pytest.mark.parametrize(
    "parameter, value, status",
    (
            ('block_number', 2 ** 128, 400),
            ('block_number', 2 ** 8, 200),
            ('timestamp', 2 ** 128, 400),
            ('timestamp', 2 ** 8, 200)
    ),
    ids=(
            "block_number_with_too_big_value",
            "block_number_with_normal_value",
            "timestamp_with_too_big_value",
            "timestamp_with_normal_value"
    )
)
async def test_get_eth_transfers_filter_by_big_value(
        cli: TestClient,
        parameter: str,
        value: int,
        status: int
):
    # given
    address = '0xcd424c53f5dc7d22cdff536309c24ad87a97e6af'

    params = urlencode({parameter: value})
    url = f"/v1/accounts/{address}/eth_transfers?{params}"

    # when
    resp = await cli.get(url)

    # then
    assert status == resp.status


@pytest.mark.parametrize(
    "url, errors",
    [
        (URL.format(params=urlencode({'block_number': 'aaaa'})), [
            {
                "field": "block_number",
                "message": "Not a valid number or tag.",
                "code": "INVALID_VALUE"
            }
        ]),
        (URL.format(params=urlencode({'timestamp': 'aaaa'})), [
            {
                "field": "timestamp",
                "message": "Not a valid number or tag.",
                "code": "INVALID_VALUE"
            }
        ]),
        (URL.format(params=urlencode({'timestamp': 10, 'block_number': 10})), [
            {
                "field": "__all__",
                "message": "Filtration should be either by number, by timestamp or by event_index",
                "code": "VALIDATION_ERROR"
            }
        ]),
        (URL.format(params=urlencode({'timestamp': 10, 'event_index': 10})), [
            {
                "field": "__all__",
                "message": "Filtration should be either by number, by timestamp or by event_index",
                "code": "VALIDATION_ERROR"
            }
        ]),
        (URL.format(params=urlencode({'event_index': 10, 'block_number': 10})), [
            {
                "field": "__all__",
                "message": "Filtration should be either by number, by timestamp or by event_index",
                "code": "VALIDATION_ERROR"
            }
        ]),
        (URL.format(params=urlencode({'limit': 100})), [
            {
                "field": "limit",
                "message": "Must be between 1 and 20.",
                "code": "INVALID_LIMIT_VALUE"
            }
        ]),
        (URL.format(params=urlencode({'order': 'ascending'})), [
            {
                "field": "order",
                "message": 'Ordering can be either "asc" or "desc".',
                "code": "INVALID_ORDER_VALUE"
            }
        ]),
    ],
    ids=[
        "invalid_tag",
        "invalid_timestamp",
        "timestamp_and_block_number",
        "timestamp_and_event_index",
        "block_number_and_event_index",
        "invalid_limit",
        "invalid_order",
    ]
)
async def test_get_eth_transfers_errors(
        cli,
        url,
        errors
):
    # when
    resp = await cli.get(url.replace('<address>', generate_address()))
    resp_json = await resp.json()

    # then
    assert resp.status == 400
    assert not resp_json['status']['success']
    assert resp_json['status']['errors'] == errors


async def test_get_account_eth_transfers(cli, transaction_factory, wallet_events_factory):
    address = '0x9cef2704a5ec2073bbba030906e24235ff2fde2f'
    wallet_events_factory.create(
        address=address,
        type="eth-transfer",
        tx_hash='0xa682d04025637c3b249586be9a7578e68af6d7b3060941ccd91202186251ba76',
        block_hash='0x9c6eba83e130251df94da89b87758d074a04a6859d9403d8dd8358bfb436aefe',
        block_number=8345641,
        event_index='83456410000000',
        is_forked=False,
        event_data={
            "sender": "0x9cef2704a5ec2073bbba030906e24235ff2fde2f",
            "recipient": "0x3c2f261fc6d26c27c49a9574defe469b53c09d0d",
            "amount": "399721000000000000",
            "status": 1,
        },
    )
    transaction_factory.create(
            r="0xf19b497d0ec5d040f72e8fb1b3705a1540386416b5bdc0cb555d35884bcd4e3",
            s="0x4a9aacab1dbe461e569b3826d8a64d124c96839613d0a3d6436d542f900caa1e",
            v="0x1b",
            to="0x3c2f261fc6d26c27c49a9574defe469b53c09d0d",
            gas="0x5208",
            from_="0x9cef2704a5ec2073bbba030906e24235ff2fde2f",
            hash="0xa682d04025637c3b249586be9a7578e68af6d7b3060941ccd91202186251ba76",
            input="0x",
            nonce="0x27",
            value="0x58c1821b6439000",
            gas_price="0x1000000000",
            transaction_index=0,
            block_hash="0x9c6eba83e130251df94da89b87758d074a04a6859d9403d8dd8358bfb436aefe",
            block_number=8345641,
            is_forked=False,
            timestamp=1565745636,
            address="0x9cef2704a5ec2073bbba030906e24235ff2fde2f",
            status=1
    )

    resp = await cli.get(f'v1/accounts/{address}/eth_transfers')
    resp_json = await resp.json()

    assert resp_json['data'] == [
        {
            'amount': '399721000000000000',
            'from': '0x9cef2704a5ec2073bbba030906e24235ff2fde2f',
            'timestamp': 1565745636,
            'to': '0x3c2f261fc6d26c27c49a9574defe469b53c09d0d',
            'transactionHash': '0xa682d04025637c3b249586be9a7578e68af6d7b3060941ccd91202186251ba76'
        },
    ]


async def test_get_account_eth_transfers_does_not_throws_500_if_there_s_no_timestamp_in_tx(
        cli,
        wallet_events_factory,
        transaction_factory,
):
    tx = transaction_factory.create(timestamp=None)
    event = wallet_events_factory.create(
        type=WalletEventType.ETH_TRANSFER,
        tx_hash=tx.hash,
        event_data={'amount': '', 'sender': '', 'recipient': ''}
    )

    resp = await cli.get(f'v1/accounts/{event.address}/eth_transfers')
    resp_json = await resp.json()

    assert len(resp_json['data']) == 1
