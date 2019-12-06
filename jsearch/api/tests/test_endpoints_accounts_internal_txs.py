import logging
from urllib.parse import urlencode

import itertools
import pytest
import time
from aiohttp.test_utils import TestClient
from typing import List, Dict, Any, Callable, Optional

from jsearch.api.tests.utils import parse_url
from jsearch.tests.plugins.databases.factories.accounts import AccountFactory
from jsearch.tests.plugins.databases.factories.common import generate_address
from jsearch.typing import AnyCoroutine

logger = logging.getLogger(__name__)


@pytest.fixture()
def create_account_internal_txs(
        block_factory,
        transaction_factory,
        internal_transaction_factory
) -> Callable[[str], AnyCoroutine]:
    account_address = None

    async def create_env(account: str,
                         block_count=5,
                         tx_in_block=2,
                         internal_tx_in_block=2) -> None:
        # Notes: some black magic to increase tests speed
        # we need to pass
        nonlocal account_address

        if account_address and account_address != account:
            raise ValueError(f'Fixture already was called for {account_address}')
        elif not account_address:

            for block_i in range(block_count):
                timestamp = TIMESTAMP + block_i
                block = block_factory.create(timestamp=timestamp)
                for i in range(0, tx_in_block):
                    kwargs = {'transaction_index': i}
                    kwargs.update({'from_': account})

                    new_txs = transaction_factory.create_for_block(block=block, **kwargs)
                    for internal_tx_index in range(1, internal_tx_in_block + 1):
                        internal_transaction_factory.create_for_tx(
                            tx=new_txs[0],
                            transaction_index=internal_tx_index,
                        )

            account_address = account

        else:
            logger.info(f'Skip txs creation for {account}')

    return create_env


URL = '/v1/accounts/address/internal_transactions?{params}'


def get_index(tx: Dict[str, Any]):
    return tx['blockNumber'], tx['parentTxIndex'], tx['transactionIndex']


TIMESTAMP = int(time.time())


@pytest.mark.parametrize(
    "url, txs_on_page, next_link, link",
    [
        (
                URL.format(params=''),
                list(itertools.product(range(0, 5), [0, 1], [1, 2]))[::-1],
                None,
                URL.format(params=urlencode({
                    'block_number': 4,
                    'parent_transaction_index': 1,
                    'transaction_index': 2,
                    'limit': '20',
                    'order': 'desc'
                })),
        ),
        (
                URL.format(params=urlencode({'limit': 3})),
                [(4, 1, 2), (4, 1, 1), (4, 0, 2)],
                URL.format(params=urlencode({
                    'block_number': 4,
                    'parent_transaction_index': 0,
                    'transaction_index': 1,
                    'limit': 3,
                    'order': 'desc'
                })),
                URL.format(params=urlencode({
                    'block_number': 4,
                    'parent_transaction_index': 1,
                    'transaction_index': 2,
                    'limit': 3,
                    'order': 'desc'
                })),
        ),
        (
                URL.format(params=urlencode({'timestamp': TIMESTAMP, 'limit': 5, 'order': 'asc'})),
                [(0, 0, 1), (0, 0, 2), (0, 1, 1), (0, 1, 2), (1, 0, 1)],
                URL.format(params=urlencode({
                    'timestamp': TIMESTAMP + 1,
                    'parent_transaction_index': 0,
                    'transaction_index': 2,
                    'limit': 5,
                    'order': 'asc'
                })),
                URL.format(params=urlencode({
                    'timestamp': TIMESTAMP,
                    'parent_transaction_index': 0,
                    'transaction_index': 1,
                    'limit': 5,
                    'order': 'asc'
                })),
        ),
        (
                URL.format(params=urlencode({'order': 'asc', 'limit': 3})),
                [(0, 0, 1), (0, 0, 2), (0, 1, 1)],
                URL.format(params=urlencode({
                    'block_number': 0,
                    'parent_transaction_index': 1,
                    'transaction_index': 2,
                    'limit': 3,
                    'order': 'asc'
                })),
                URL.format(params=urlencode({
                    'block_number': 0,
                    'parent_transaction_index': 0,
                    'transaction_index': 1,
                    'limit': 3,
                    'order': 'asc'
                })),
        ),
        (
                URL.format(params=urlencode({'block_number': 3, 'limit': 3})),
                [(3, 1, 2), (3, 1, 1), (3, 0, 2)],
                URL.format(params=urlencode({
                    'block_number': 3,
                    'parent_transaction_index': 0,
                    'transaction_index': 1,
                    'limit': 3,
                    'order': 'desc'
                })),
                URL.format(params=urlencode({
                    'block_number': 3,
                    'parent_transaction_index': 1,
                    'transaction_index': 2,
                    'limit': 3,
                    'order': 'desc'
                })),
        ),
        (
                URL.format(params=urlencode({
                    'block_number': 3,
                    'parent_transaction_index': 0,
                    'transaction_index': 2,
                    'limit': 3
                })),
                [(3, 0, 2), (3, 0, 1), (2, 1, 2)],
                URL.format(params=urlencode({
                    'block_number': 2,
                    'parent_transaction_index': 1,
                    'transaction_index': 1,
                    'limit': 3,
                    'order': 'desc'
                })),
                URL.format(params=urlencode({
                    'block_number': 3,
                    'parent_transaction_index': 0,
                    'transaction_index': 2,
                    'limit': 3,
                    'order': 'desc'
                })),
        ),
        (
                URL.format(params=urlencode({
                    'block_number': 3,
                    'parent_transaction_index': 0,
                    'transaction_index': 1,
                    'limit': 3
                })),
                [(3, 0, 1), (2, 1, 2), (2, 1, 1)],
                URL.format(params=urlencode({
                    'block_number': 2,
                    'parent_transaction_index': 0,
                    'transaction_index': 2,
                    'limit': 3,
                    'order': 'desc'
                })),
                URL.format(params=urlencode({
                    'block_number': 3,
                    'parent_transaction_index': 0,
                    'transaction_index': 1,
                    'limit': 3,
                    'order': 'desc'
                })),
        ),
        (
                URL.format(params=urlencode({'block_number': 'latest', 'limit': 3})),
                [(4, 1, 2), (4, 1, 1), (4, 0, 2)],
                URL.format(params=urlencode({
                    'block_number': 4,
                    'parent_transaction_index': 0,
                    'transaction_index': 1,
                    'limit': 3,
                    'order': 'desc'
                })),
                URL.format(params=urlencode({
                    'block_number': 4,
                    'parent_transaction_index': 1,
                    'transaction_index': 2,
                    'limit': 3,
                    'order': 'desc'
                })),
        ),
        (
                URL.format(params=urlencode({'timestamp': 'latest', 'limit': 3})),
                [(4, 1, 2), (4, 1, 1), (4, 0, 2)],
                URL.format(params=urlencode({
                    'timestamp': TIMESTAMP + 4,
                    'parent_transaction_index': 0,
                    'transaction_index': 1,
                    'limit': 3,
                    'order': 'desc'
                })),
                URL.format(params=urlencode({
                    'timestamp': TIMESTAMP + 4,
                    'parent_transaction_index': 1,
                    'transaction_index': 2,
                    'limit': 3,
                    'order': 'desc'
                })),
        ),
    ],
    ids=[
        'no_params',
        URL.format(params=urlencode({'limit': 3})),
        URL.format(params=urlencode({'timestamp': TIMESTAMP, 'limit': 3, 'order': 'asc'})),
        URL.format(params=urlencode({'order': 'asc', 'limit': 3})),
        URL.format(params=urlencode({'block_number': 3, 'limit': 3})),
        URL.format(params=urlencode({
            'block_number': 3,
            'parent_transaction_index': 0,
            'transaction_index': 2,
            'limit': 3
        })),
        URL.format(params=urlencode({
            'block_number': 3,
            'parent_transaction_index': 0,
            'transaction_index': 1,
            'limit': 3
        })),
        URL.format(params=urlencode({'block_number': 'latest', 'limit': 3})),
        URL.format(params=urlencode({'timestamp': 'latest', 'limit': 3})),
    ]
)
async def test_get_account_internal_transactions(cli,
                                                 account_factory,
                                                 create_account_internal_txs,
                                                 url: str,
                                                 txs_on_page: List[int],
                                                 next_link: str,
                                                 link: str) -> None:
    # given
    account = account_factory.create()
    await create_account_internal_txs(account.address)

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

    assert [get_index(tx) for tx in resp_json['data']] == txs_on_page


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
        (URL.format(params=urlencode({'block_number': 3, 'transaction_index': 1, 'limit': 3})), [
            {
                "field": "__all__",
                "message": "Filter `transaction_index` requires `parent_transaction_index` value.",
                "code": "VALIDATION_ERROR"
            }
        ]),
        (URL.format(params=urlencode({'parent_transaction_index': 1, 'limit': 3})), [
            {
                "field": "__all__",
                "message": "Filter `parent_transaction_index` requires `block_number` or `timestamp` value.",
                "code": "VALIDATION_ERROR"
            }
        ]),
    ],
    ids=[
        "invalid_tag",
        "invalid_timestamp",
        "either_number_or_timestamp",
        "invalid_order",
        "transaction_index_requires_parent_transaction_index",
        "parent_transaction_index_requires_block_number",
    ]
)
async def test_get_account_internal_transactions_errors(
        cli,
        account_factory,
        create_account_internal_txs,
        url,
        errors
):
    # given
    account = account_factory.create()
    await create_account_internal_txs(account.address)

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
async def test_get_accounts_internal_txs_limits(
        cli: TestClient,
        account_factory: AccountFactory,
        create_account_internal_txs,
        target_limit: Optional[int],
        expected_items_count: int,
        expected_errors: List[Dict[str, str]],
):
    # given
    account = account_factory.create()
    await create_account_internal_txs(account.address, internal_tx_in_block=3)  # ...therefore more than 20 TXs created.

    # when
    reqv_params = {'block_number': 'latest'}

    if target_limit is not None:
        reqv_params['limit'] = target_limit

    resp = await cli.get(URL.replace('/address/', f'/{account.address}/').format(params=urlencode(reqv_params)))
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
async def test_get_account_internal_txs_filter_by_big_value(
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
