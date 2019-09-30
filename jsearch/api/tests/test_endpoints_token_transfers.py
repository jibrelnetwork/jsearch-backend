import logging
from urllib.parse import urlencode

import pytest
import time
from aiohttp.test_utils import TestClient
from typing import List, Dict, Any, Callable, Optional

from jsearch.api.tests.utils import parse_url
from jsearch.tests.plugins.databases.factories.accounts import AccountFactory
from jsearch.tests.plugins.databases.factories.common import generate_address
from jsearch.typing import AnyCoroutine

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.usefixtures('disable_metrics_setup')


@pytest.fixture()
def create_account_transfers(
        block_factory,
        transaction_factory,
        log_factory,
        transfer_factory
) -> Callable[[str], AnyCoroutine]:
    async def create_env(token_address: str,
                         block_count=5,
                         tx_in_block=2,
                         logs_in_tx=2) -> None:

        transfers = []
        for block_i in range(block_count):
            timestamp = TIMESTAMP + block_i
            block = block_factory.create(timestamp=timestamp)
            for i in range(0, tx_in_block):
                kwargs = {
                    'transaction_index': i,
                }

                new_txs = transaction_factory.create_for_block(block=block, **kwargs)
                tx = new_txs[0]

                for log_index in range(logs_in_tx):
                    log = log_factory.create_for_tx(tx, log_index=log_index)
                    transfers.extend(
                        transfer_factory.create_for_log(
                            tx=tx,
                            block=block,
                            log=log,
                            token_address=token_address
                        )
                    )

    return create_env


URL = '/v1/tokens/address/transfers?{params}'


def get_index(tx: Dict[str, Any]):
    return tx['blockNumber'], tx['transactionIndex'], tx['logIndex']


TIMESTAMP = int(time.time())


@pytest.mark.parametrize(
    "url, txs_on_page, next_link, link",
    [
        (
                URL.format(params=urlencode({'limit': 3})),
                [(4, 1, 1), (4, 1, 0), (4, 0, 1)],
                URL.format(params=urlencode({
                    'block_number': 4,
                    'transaction_index': 0,
                    'log_index': 0,
                    'limit': 3,
                    'order': 'desc'
                })),
                URL.format(params=urlencode({
                    'block_number': 4,
                    'transaction_index': 1,
                    'log_index': 1,
                    'limit': 3,
                    'order': 'desc'
                })),
        ),
        (
                URL.format(params=urlencode({'timestamp': TIMESTAMP, 'limit': 5, 'order': 'asc'})),
                [(0, 0, 0), (0, 0, 1), (0, 1, 0), (0, 1, 1), (1, 0, 0)],
                URL.format(params=urlencode({
                    'timestamp': TIMESTAMP + 1,
                    'transaction_index': 0,
                    'log_index': 1,
                    'limit': 5,
                    'order': 'asc'
                })),
                URL.format(params=urlencode({
                    'timestamp': TIMESTAMP,
                    'transaction_index': 0,
                    'log_index': 0,
                    'limit': 5,
                    'order': 'asc'
                })),
        ),
        (
                URL.format(params=urlencode({'order': 'asc', 'limit': 3})),
                [(0, 0, 0), (0, 0, 1), (0, 1, 0)],
                URL.format(params=urlencode({
                    'block_number': 0,
                    'transaction_index': 1,
                    'log_index': 1,
                    'limit': 3,
                    'order': 'asc'
                })),
                URL.format(params=urlencode({
                    'block_number': 0,
                    'transaction_index': 0,
                    'log_index': 0,
                    'limit': 3,
                    'order': 'asc'
                })),
        ),
        (
                URL.format(params=urlencode({'block_number': 3, 'limit': 3})),
                [(3, 1, 1), (3, 1, 0), (3, 0, 1)],
                URL.format(params=urlencode({
                    'block_number': 3,
                    'transaction_index': 0,
                    'log_index': 0,
                    'limit': 3,
                    'order': 'desc'
                })),
                URL.format(params=urlencode({
                    'block_number': 3,
                    'transaction_index': 1,
                    'log_index': 1,
                    'limit': 3,
                    'order': 'desc'
                })),
        ),
        (
                URL.format(params=urlencode({
                    'block_number': 3,
                    'transaction_index': 0,
                    'log_index': 1,
                    'limit': 3
                })),
                [(3, 0, 1), (3, 0, 0), (2, 1, 1)],
                URL.format(params=urlencode({
                    'block_number': 2,
                    'transaction_index': 1,
                    'log_index': 0,
                    'limit': 3,
                    'order': 'desc'
                })),
                URL.format(params=urlencode({
                    'block_number': 3,
                    'transaction_index': 0,
                    'log_index': 1,
                    'limit': 3,
                    'order': 'desc'
                })),
        ),
        (
                URL.format(params=urlencode({
                    'block_number': 3,
                    'transaction_index': 40,
                    'log_index': 1,
                    'limit': 3
                })),
                [(3, 1, 1), (3, 1, 0), (3, 0, 1)],
                URL.format(params=urlencode({
                    'block_number': 3,
                    'transaction_index': 0,
                    'log_index': 0,
                    'limit': 3,
                    'order': 'desc'
                })),
                URL.format(params=urlencode({
                    'block_number': 3,
                    'transaction_index': 1,
                    'log_index': 1,
                    'limit': 3,
                    'order': 'desc'
                })),
        ),
        (
                URL.format(params=urlencode({'block_number': 'latest', 'limit': 3})),
                [(4, 1, 1), (4, 1, 0), (4, 0, 1)],
                URL.format(params=urlencode({
                    'block_number': 4,
                    'transaction_index': 0,
                    'log_index': 0,
                    'limit': 3,
                    'order': 'desc'
                })),
                URL.format(params=urlencode({
                    'block_number': 4,
                    'transaction_index': 1,
                    'log_index': 1,
                    'limit': 3,
                    'order': 'desc'
                })),
        ),
    ],
    ids=[
        URL.format(params=urlencode({'limit': 3})),
        URL.format(params=urlencode({'timestamp': TIMESTAMP, 'limit': 3, 'order': 'asc'})),
        URL.format(params=urlencode({'order': 'asc', 'limit': 3})),
        URL.format(params=urlencode({'block_number': 3, 'limit': 3})),
        URL.format(params=urlencode({
            'block_number': 3,
            'transaction_index': 0,
            'log_index': 1,
            'limit': 3
        })),
        URL.format(params=urlencode({
            'block_number': 3,
            'transaction_index': 40,
            'log_index': 1,
            'limit': 3
        })),
        URL.format(params=urlencode({'block_number': 'latest', 'limit': 3})),
    ]
)
async def test_get_token_transfers_pagination(cli,
                                              account_factory,
                                              create_account_transfers,
                                              url: str,
                                              txs_on_page: List[int],
                                              next_link: str,
                                              link: str) -> None:
    """
    Each transaction is described as tuple of index elements:
        - block_number
        - transaction_number
        - log_index
    """
    # given
    token = generate_address()
    await create_account_transfers(token)

    # when
    resp = await cli.get(url.replace('/address/', f'/{token}/'))
    resp_json = await resp.json()

    # then
    assert resp.status == 200
    assert resp_json['status']['success']

    link = link and link.replace('/address/', f'/{token}/')
    next_link = next_link and next_link.replace('/address/', f'/{token}/')

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
        (URL.format(params=urlencode({'block_number': 3, 'log_index': 1, 'limit': 3})), [
            {
                "field": "__all__",
                "message": "Filter `log_index` requires `transaction_index` value.",
                "code": "VALIDATION_ERROR"
            }
        ]),
        (URL.format(params=urlencode({'transaction_index': 1, 'limit': 3})), [
            {
                "field": "__all__",
                "message": "Filter `transaction_index` requires `block_number` or `timestamp` value.",
                "code": "VALIDATION_ERROR"
            }
        ]),
    ],
    ids=[
        "invalid_tag",
        "invalid_timestamp",
        "either_number_or_timestamp",
        "invalid_limit",
        "invalid_order",
        "transaction_index_requires_parent_transaction_index",
        "parent_transaction_index_requires_block_number",
    ]
)
async def test_get_token_transfers_errors(
        cli,
        account_factory,
        create_account_transfers,
        url,
        errors
):
    # given
    account = account_factory.create()
    await create_account_transfers(account.address)

    # when
    resp = await cli.get(url)
    resp_json = await resp.json()

    # then
    assert resp.status == 400
    assert not resp_json['status']['success']
    assert resp_json['status']['errors'] == errors


async def test_get_token_transfers_noparams(cli, block_factory, transaction_factory, log_factory, transfer_factory):
    token = generate_address()
    block = block_factory.create()
    tx = transaction_factory.create_for_block(block)[0]
    log = log_factory.create_for_tx(tx)

    transfer = transfer_factory.create_for_log(block, tx, log, token_address=token)[0]

    resp = await cli.get(URL.format(params="").replace("/address/", f"/{token}/"))
    resp_json = await resp.json()

    assert resp.status == 200
    assert resp_json['data'] == [
        {
            'amount': f'{int(transfer.token_value)}',
            'blockNumber': transfer.block_number,
            'contractAddress': transfer.token_address,
            'decimals': transfer.token_decimals,
            'from': transfer.from_address,
            'timestamp': transfer.timestamp,
            'to': transfer.to_address,
            'transactionHash': transfer.transaction_hash,
            'transactionIndex': transfer.transaction_index,
            'logIndex': transfer.log_index,
        }
    ]


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
async def test_get_token_transfers_limits(
        cli: TestClient,
        account_factory: AccountFactory,
        create_account_transfers,
        target_limit: Optional[int],
        expected_items_count: int,
        expected_errors: List[Dict[str, str]],
):
    # given
    account = account_factory.create()
    await create_account_transfers(account.address)

    # when
    reqv_params = 'block_number=latest'

    if target_limit is not None:
        reqv_params += f'&limit={target_limit}'

    resp = await cli.get(f'/v1/tokens/{account.address}/transfers?{reqv_params}')
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
async def test_get_token_transfers_filter_by_big_value(
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
