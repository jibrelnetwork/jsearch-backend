from urllib.parse import urlencode

import pytest
import pytz
import time
from datetime import datetime
from typing import List, Dict, Any, Tuple, Callable

from jsearch.api.tests.utils import parse_url
from jsearch.typing import AnyCoroutine

pytestmark = pytest.mark.usefixtures('disable_metrics_setup')

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
