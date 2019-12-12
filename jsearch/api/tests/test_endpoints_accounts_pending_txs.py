from urllib.parse import urlencode

import pytest
import pytz
import time
from aiohttp.test_utils import TestClient
from datetime import datetime
from typing import List, Dict, Any, Tuple, Callable, Optional

from jsearch.api.tests.utils import parse_url
from jsearch.tests.plugins.databases.factories.accounts import AccountFactory
from jsearch.tests.plugins.databases.factories.common import generate_address
from jsearch.typing import AnyCoroutine

TIMESTAMP = int(time.time())
URL = '/v1/accounts/address/pending_transactions?{params}'


@pytest.fixture()
def create_account_pending_txs(
        pending_transaction_factory
) -> Callable[[str], AnyCoroutine]:
    tz = pytz.timezone('UTC')

    txs = {}

    async def create_env(account: str, count=10) -> None:
        for i in range(0, count):
            timestamp_with_tz = datetime.fromtimestamp(TIMESTAMP + int(i / 2), tz)
            kwargs = {'timestamp': timestamp_with_tz, 'id': i + 1}
            if i % 2:
                kwargs.update({"from_": account})
            else:
                kwargs.update({"to": account})

            tx = pending_transaction_factory.create(**kwargs)
            txs[i + 1] = tx.hash

        return txs

    return create_env


def get_hash(tx: Dict[str, Any]) -> Tuple[int, int]:
    return tx["id"]


@pytest.mark.parametrize(
    "from_,to",
    [
        (
                '0x1111111111111111111111111111111111111111',
                '0x2222222222222222222222222222222222222222',
        ),
        (
                '0x2222222222222222222222222222222222222222',
                '0x1111111111111111111111111111111111111111',
        ),
    ],
)
async def test_get_account_pending_transactions(cli, from_, to, pending_transaction_factory):
    pending_transaction_factory.create(
        hash='0xdf0237a2edf8f0a5bcdee4d806c7c3c899188d7b8a65dd9d3a4d39af1451a9bc',
        status='',
        removed=False,
        r='0x11',
        s='0x22',
        v='0x33',
        to=to,
        from_=from_,
        gas=2100,
        gas_price=10000000000,
        input='0x0',
        nonce=42,
        value='1111111111111111111111111111111111111111',
    )

    resp = await cli.get(f'v1/accounts/0x1111111111111111111111111111111111111111/pending_transactions')
    resp_json = await resp.json()

    assert resp.status == 200
    assert resp_json['data'] == [
        {
            'hash': '0xdf0237a2edf8f0a5bcdee4d806c7c3c899188d7b8a65dd9d3a4d39af1451a9bc',
            'status': '',
            'removed': False,
            'r': '0x11',
            's': '0x22',
            'v': '0x33',
            'to': to,
            'from': from_,
            'gas': '2100',
            'gasPrice': '10000000000',
            'input': '0x0',
            'nonce': '42',
            'value': '1111111111111111111111111111111111111111',
        },
    ]


@pytest.mark.parametrize(
    "url, txs_on_page, next_link, link",
    [
        (
                URL.format(params=urlencode({'limit': 3})),
                [10, 9, 8],
                URL.format(params=urlencode({
                    'timestamp': TIMESTAMP + 3,
                    'id': 7,
                    'limit': 3,
                    'order': 'desc'
                })),
                URL.format(params=urlencode({
                    'timestamp': TIMESTAMP + 4,
                    'id': 10,
                    'limit': 3,
                    'order': 'desc'
                })),
        ),
        (
                URL.format(params=urlencode({'timestamp': TIMESTAMP, 'limit': 3, 'order': 'asc'})),
                [1, 2, 3],
                URL.format(params=urlencode({
                    'timestamp': TIMESTAMP + 1,
                    'id': 4,
                    'limit': 3,
                    'order': 'asc'
                })),
                URL.format(params=urlencode({
                    'timestamp': TIMESTAMP,
                    'id': 1,
                    'limit': 3,
                    'order': 'asc'
                })),
        ),
        (
                URL.format(params=urlencode({'order': 'asc', 'limit': 3})),
                [1, 2, 3],
                URL.format(params=urlencode({
                    'timestamp': TIMESTAMP + 1,
                    'id': 4,
                    'limit': 3,
                    'order': 'asc'
                })),
                URL.format(params=urlencode({
                    'timestamp': TIMESTAMP,
                    'id': 1,
                    'limit': 3,
                    'order': 'asc'
                })),
        ),
        (
                URL.format(params=urlencode({'timestamp': TIMESTAMP + 1, 'limit': 3})),
                [4, 3, 2],
                URL.format(params=urlencode({
                    'timestamp': TIMESTAMP,
                    'id': 1,
                    'limit': 3,
                    'order': 'desc'
                })),
                URL.format(params=urlencode({
                    'timestamp': TIMESTAMP + 1,
                    'id': 4,
                    'limit': 3,
                    'order': 'desc'
                })),
        ),
        (
                URL.format(params=urlencode({'timestamp': TIMESTAMP + 1, 'id': 3, 'limit': 3})),
                [3, 2, 1],
                None,
                URL.format(params=urlencode({
                    'timestamp': TIMESTAMP + 1,
                    'id': 3,
                    'limit': 3,
                    'order': 'desc'
                })),
        ),
    ],
    ids=[
        URL.format(params=urlencode({'limit': 3})),
        URL.format(params=urlencode({'timestamp': TIMESTAMP, 'limit': 3, 'order': 'asc'})),
        URL.format(params=urlencode({'order': 'asc', 'limit': 3})),
        URL.format(params=urlencode({'timestamp': TIMESTAMP + 1, 'limit': 3})),
        URL.format(params=urlencode({'timestamp': TIMESTAMP + 1, 'id': 3, 'limit': 3})),
    ]
)
async def test_get_account_pending_txs_paging(
        cli,
        account_factory,
        create_account_pending_txs,
        url: str,
        txs_on_page: List[int],
        next_link: str,
        link: str
) -> None:
    # given
    account = account_factory.create()
    txs = await create_account_pending_txs(account.address)

    # when
    resp = await cli.get(url.replace('/address/', f'/{account.address}/'))
    resp_json = await resp.json()

    # then
    assert resp.status == 200
    assert resp_json['status']['success']

    link = link and link.replace('/address/', f'/{account.address}/')
    next_link = next_link and next_link.replace('/address/', f'/{account.address}/')

    expected_hashes = [txs[i] for i in txs_on_page]
    tx_hashes = [tx['hash'] for tx in resp_json['data']]

    assert tx_hashes == expected_hashes

    assert parse_url(resp_json['paging']['next']) == parse_url(next_link)
    assert parse_url(resp_json['paging']['link']) == parse_url(link)


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
async def test_get_accounts_pending_txs_limits(
        cli: TestClient,
        account_factory: AccountFactory,
        create_account_pending_txs,
        target_limit: Optional[int],
        expected_items_count: int,
        expected_errors: List[Dict[str, str]],
):
    # given
    account = account_factory.create()
    await create_account_pending_txs(account.address, count=25)  # ...therefore more than 20 TXs created.

    # when
    reqv_params = 'id=1'

    if target_limit is not None:
        reqv_params += f'&limit={target_limit}'

    resp = await cli.get(f'/v1/accounts/{account.address}/pending_transactions?{reqv_params}')
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
            ('id', 2 ** 128, 400),
            ('id', 2 ** 8, 200),
            ('timestamp', 2 ** 128, 400),
            ('timestamp', 2 ** 8, 200)
    ),
    ids=(
            "id_with_too_big_value",
            "id_with_normal_value",
            "timestamp_with_too_big_value",
            "timestamp_with_normal_value"
    )
)
async def test_get_account_pending_txs_filter_by_big_value(
        cli: TestClient,
        parameter: str,
        value: int,
        status: int
):
    # given
    account = generate_address()

    params = urlencode({parameter: value})
    url = URL.replace('address', account).format(params=params)

    # when
    resp = await cli.get(url)

    # then
    assert status == resp.status
