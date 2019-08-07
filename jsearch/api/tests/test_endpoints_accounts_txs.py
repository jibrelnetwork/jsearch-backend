import logging
from urllib.parse import parse_qs, urlencode

import pytest
import time
from typing import List, Dict, Any, Tuple, Callable

from jsearch.typing import AnyCoroutine

logger = logging.getLogger(__name__)

TIMESTAMP = int(time.time())

pytestmark = pytest.mark.usefixtures('disable_metrics_setup')


def parse_url(url: str) -> Tuple[str, Dict[str, Any]]:
    if url:
        path, params = url.split("?")
        return path, parse_qs(params)


@pytest.fixture()
def create_account_txs(block_factory, transaction_factory) -> Callable[[str], AnyCoroutine]:
    account_address = None

    async def create_env(account: str, block_count=5, tx_in_block=2) -> None:
        # Notes: some black magic to increase tests speed
        # we need to pass
        nonlocal account_address
        if account_address and account_address != account:
            raise ValueError(f'Fixture already was called for {account_address}')
        elif not account_address:
            txs = []
            for block_i in range(block_count):
                timestamp = TIMESTAMP + block_i
                block = block_factory.create(timestamp=timestamp)
                for i in range(0, tx_in_block):
                    kwargs = {'transaction_index': i}
                    if block.number % 2:
                        kwargs.update({'from_': account})
                    else:
                        kwargs.update({'to': account})

                    txs.extend(transaction_factory.create_for_block(block=block, **kwargs))

            account_address = account

        else:
            print(f'Skip txs creation for {account}')

    return create_env


URL = '/v1/accounts/address/transactions?{params}'


@pytest.mark.parametrize(
    "url, txs_on_page, next_link, link",
    [
        (
                URL.format(params=urlencode({'limit': 3})),
                [(4, 1), (4, 0), (3, 1)],
                URL.format(params=urlencode({'block_number': 3, 'transaction_index': 0, 'limit': 3, 'order': 'desc'})),
                URL.format(params=urlencode({'block_number': 4, 'transaction_index': 1, 'limit': 3, 'order': 'desc'})),
        ),
        (
                URL.format(params=urlencode({'order': 'asc', 'limit': 3})),
                [(0, 0), (0, 1), (1, 0)],
                URL.format(params=urlencode({'block_number': 1, 'transaction_index': 1, 'limit': 3, 'order': 'asc'})),
                URL.format(params=urlencode({'block_number': 0, 'transaction_index': 0, 'limit': 3, 'order': 'asc'})),
        ),
        (
                URL.format(params=urlencode({'block_number': 3, 'limit': 3})),
                [(3, 1), (3, 0), (2, 1)],
                URL.format(params=urlencode({'block_number': 2, 'transaction_index': 0, 'limit': 3, 'order': 'desc'})),
                URL.format(params=urlencode({'block_number': 3, 'transaction_index': 1, 'limit': 3, 'order': 'desc'})),
        ),
        (
                URL.format(params=urlencode({'block_number': 3, 'transaction_index': 0, 'limit': 3})),
                [(3, 0), (2, 1), (2, 0)],
                URL.format(params=urlencode({'block_number': 1, 'transaction_index': 1, 'limit': 3, 'order': 'desc'})),
                URL.format(params=urlencode({'block_number': 3, 'transaction_index': 0, 'limit': 3, 'order': 'desc'})),
        ),
        (
                URL.format(params=urlencode({'block_number': 'latest', 'limit': 3})),
                [(4, 1), (4, 0), (3, 1)],
                URL.format(params=urlencode({'block_number': 3, 'transaction_index': 0, 'limit': 3, 'order': 'desc'})),
                URL.format(params=urlencode({'block_number': 4, 'transaction_index': 1, 'limit': 3, 'order': 'desc'}))
        ),
        (
                URL.format(params=urlencode({'timestamp': TIMESTAMP, 'limit': 3, 'order': 'asc'})),
                [(0, 0), (0, 1), (1, 0)],
                URL.format(params=urlencode({
                    'timestamp': TIMESTAMP + 1,
                    'transaction_index': 1,
                    'limit': 3,
                    'order': 'asc'
                })),
                URL.format(params=urlencode({
                    'timestamp': TIMESTAMP,
                    'transaction_index': 0,
                    'limit': 3,
                    'order': 'asc'
                })),
        ),
    ],
    ids=[
        URL.format(params=urlencode({'limit': 3})),
        URL.format(params=urlencode({'order': 'asc', 'limit': 3})),
        URL.format(params=urlencode({'block_number': 3, 'limit': 3})),
        URL.format(params=urlencode({'block_number': 3, 'transaction_index': 1, 'limit': 3})),
        URL.format(params=urlencode({'block_number': 'latest', 'limit': 3})),
        URL.format(params=urlencode({'timestamp': TIMESTAMP, 'limit': 3, 'order': 'asc'})),
    ]
)
async def test_account_transactions(cli,
                                    account_factory,
                                    create_account_txs,
                                    url: str,
                                    txs_on_page: List[int],
                                    next_link: str,
                                    link: str) -> None:
    # given
    account = account_factory.create()
    await create_account_txs(account.address)

    # when
    resp = await cli.get(url.replace('/address/', f'/{account.address}/'))
    resp_json = await resp.json()

    # then
    assert resp.status == 200
    assert resp_json['status']['success']

    link = link and link.replace('/address/', f'/{account.address}/')
    next_link = next_link and next_link.replace('/address/', f'/{account.address}/')

    assert parse_url(resp_json['paging']['next']) == parse_url(next_link)
    assert parse_url(resp_json['paging']['link']) == parse_url(link)

    assert [(tx['blockNumber'], tx['transactionIndex']) for tx in resp_json['data']] == txs_on_page


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
                "message": "Filtration should be either by number or by timestamp",
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
        "either_number_or_timestamp",
        "invalid_limit",
        "invalid_order"
    ]
)
async def test_account_transactions_errors(cli, account_factory, create_account_txs, url, errors):
    # given
    account = account_factory.create()
    await create_account_txs(account.address)

    # when
    resp = await cli.get(url)
    resp_json = await resp.json()

    # then
    assert resp.status == 400
    assert not resp_json['status']['success']
    assert resp_json['status']['errors'] == errors
