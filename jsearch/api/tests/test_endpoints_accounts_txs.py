import logging
import time
from typing import List, Dict, Any, Tuple, Callable, Optional
from urllib.parse import parse_qs, urlencode

import pytest
from aiohttp.test_utils import TestClient

from jsearch.tests.plugins.databases.factories.accounts import AccountFactory
from jsearch.tests.plugins.databases.factories.common import generate_address
from jsearch.typing import AnyCoroutine

logger = logging.getLogger(__name__)

TIMESTAMP = int(time.time())


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
            logger.info(f'Skip txs creation for {account}')

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
                URL.format(params=urlencode({'timestamp': 'latest', 'limit': 3})),
                [(4, 1), (4, 0), (3, 1)],
                URL.format(params=urlencode(
                    {
                        'timestamp': TIMESTAMP + 3,
                        'transaction_index': 0,
                        'limit': 3,
                        'order': 'desc',
                    },
                )),
                URL.format(params=urlencode(
                    {
                        'timestamp': TIMESTAMP + 4,
                        'transaction_index': 1,
                        'limit': 3,
                        'order': 'desc',
                    },
                )),
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
        URL.format(params=urlencode({'timestamp': 'latest', 'limit': 3})),
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
async def test_get_accounts_transactions_limits(
        cli: TestClient,
        account_factory: AccountFactory,
        create_account_txs,
        target_limit: Optional[int],
        expected_items_count: int,
        expected_errors: List[Dict[str, str]],
):
    # given
    account = account_factory.create()
    await create_account_txs(account.address, tx_in_block=5)  # ...therefore more than 20 TXs created.

    # when
    reqv_params = 'block_number=latest'

    if target_limit is not None:
        reqv_params += f'&limit={target_limit}'

    resp = await cli.get(f'/v1/accounts/{account.address}/transactions?{reqv_params}')
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
async def test_get_account_txs_filter_by_big_value(
        cli: TestClient,
        parameter: str,
        value: int,
        status: int
):
    # given
    address = generate_address()

    params = urlencode({parameter: value})
    url = f"/v1/accounts/{address}/transactions?{params}"

    # when
    resp = await cli.get(url)

    # then
    assert status == resp.status


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
async def test_get_accounts_txs_filter_by_big_value(
        cli: TestClient,
        parameter: str,
        value: int,
        status: int
):
    # given
    address = generate_address()

    params = urlencode({parameter: value})
    url = f"/v1/accounts/{address}/transactions?{params}"

    # when
    resp = await cli.get(url)

    # then
    assert status == resp.status
