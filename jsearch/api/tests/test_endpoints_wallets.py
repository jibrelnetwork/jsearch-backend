import logging
from urllib.parse import urlencode

import pytest
import time
from aiohttp.test_utils import TestClient
from typing import Callable, Tuple, List, Dict, Any, Optional

from jsearch.api.tests.utils import parse_url
from jsearch.common.tables import assets_summary_t
from jsearch.common.wallet_events import make_event_index
from jsearch.tests.plugins.databases.factories.accounts import AccountFactory
from jsearch.tests.plugins.databases.factories.assets_summary import AssetsSummaryFactory
from jsearch.tests.plugins.databases.factories.common import generate_address
from jsearch.typing import AnyCoroutine

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.usefixtures('disable_metrics_setup')


def get_indexes(data: Dict[str, Any]) -> int:
    indexes = []
    for item in data:
        events = item['events']
        for event in events:
            indexes.append(event['eventIndex'])
    return indexes


TIMESTAMP = int(time.time())


@pytest.fixture()
def create_wallet_events(
        block_factory,
        transaction_factory,
        internal_transaction_factory,
        wallet_events_factory
) -> Callable[[str], AnyCoroutine]:
    async def create_env(account: str,
                         block_count=5,
                         tx_in_block=2,
                         internal_tx_in_block=2) -> None:
        for block_i in range(block_count):
            timestamp = TIMESTAMP + block_i
            block = block_factory.create(timestamp=timestamp)
            for i in range(0, tx_in_block):
                kwargs = {'transaction_index': i}
                kwargs.update({'from_': account})

                new_txs = transaction_factory.create_for_block(block=block, **kwargs)
                for internal_tx_index in range(1, internal_tx_in_block + 1):
                    internal_tx = internal_transaction_factory.create_for_tx(
                        tx=new_txs[0],
                        transaction_index=internal_tx_index,
                    )
                    wallet_events_factory.create_event_from_internal_tx(internal_tx, new_txs[0], block)

    return create_env


URL = '/v1/wallet/events?{params}'


@pytest.mark.parametrize(
    'url, events_on_page, next_link, link',
    [
        (
                URL.format(params=urlencode({'limit': 3})),
                [
                    (4, 1, 2),
                    (4, 1, 1),
                    (4, 0, 2),
                ],
                URL.format(params=urlencode({
                    'block_number': 4,
                    'event_index': make_event_index(4, 0, 1),
                    'limit': 3,
                    'order': 'desc'
                })),
                URL.format(params=urlencode({
                    'block_number': 4,
                    'event_index': make_event_index(4, 1, 2),
                    'limit': 3,
                    'order': 'desc'
                })),
        ),
        (
                URL.format(params=urlencode({'timestamp': TIMESTAMP, 'limit': 5, 'order': 'asc'})),
                [(0, 0, 1), (0, 0, 2), (0, 1, 1), (0, 1, 2), (1, 0, 1)],
                URL.format(params=urlencode({
                    'timestamp': TIMESTAMP + 1,
                    'event_index': make_event_index(1, 0, 2),
                    'limit': 5,
                    'order': 'asc'
                })),
                URL.format(params=urlencode({
                    'timestamp': TIMESTAMP,
                    'event_index': make_event_index(0, 0, 1),
                    'limit': 5,
                    'order': 'asc'
                })),
        ),
        (
                URL.format(params=urlencode({'order': 'asc', 'limit': 3})),
                [(0, 0, 1), (0, 0, 2), (0, 1, 1)],
                URL.format(params=urlencode({
                    'block_number': 0,
                    'event_index': make_event_index(0, 1, 2),
                    'limit': 3,
                    'order': 'asc'
                })),
                URL.format(params=urlencode({
                    'block_number': 0,
                    'event_index': make_event_index(0, 0, 1),
                    'limit': 3,
                    'order': 'asc'
                })),
        ),
        (
                URL.format(params=urlencode({'block_number': 3, 'limit': 3})),
                [(3, 1, 2), (3, 1, 1), (3, 0, 2)],
                URL.format(params=urlencode({
                    'block_number': 3,
                    'event_index': make_event_index(3, 0, 1),
                    'limit': 3,
                    'order': 'desc'
                })),
                URL.format(params=urlencode({
                    'block_number': 3,
                    'event_index': make_event_index(3, 1, 2),
                    'limit': 3,
                    'order': 'desc'
                })),
        ),
        (
                URL.format(params=urlencode({
                    'block_number': 3,
                    'event_index': make_event_index(3, 0, 2),
                    'limit': 3
                })),
                [(3, 0, 2), (3, 0, 1), (2, 1, 2)],
                URL.format(params=urlencode({
                    'block_number': 2,
                    'event_index': make_event_index(2, 1, 1),
                    'limit': 3,
                    'order': 'desc'
                })),
                URL.format(params=urlencode({
                    'event_index': make_event_index(3, 0, 2),
                    'block_number': 3,
                    'limit': 3,
                    'order': 'desc'
                })),
        ),
        (
                URL.format(params=urlencode({'block_number': 'latest', 'limit': 3})),
                [(4, 1, 2), (4, 1, 1), (4, 0, 2)],
                URL.format(params=urlencode({
                    'block_number': 4,
                    'event_index': make_event_index(4, 0, 1),
                    'limit': 3,
                    'order': 'desc'
                })),
                URL.format(params=urlencode({
                    'block_number': 4,
                    'event_index': make_event_index(4, 1, 2),
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
        URL.format(params=urlencode({
            'block_number': 3,
            'event_index': make_event_index(3, 0, 1),
            'limit': 3
        })),
        URL.format(params=urlencode({'block_number': 'latest', 'limit': 3})),
    ]
)
async def test_get_wallet_events_pagination(
        cli,
        account_factory,
        create_wallet_events,
        url,
        events_on_page: List[Tuple[int, int, int]],
        next_link: str,
        link: str
):
    # given
    account = account_factory.create()
    await create_wallet_events(account.address)

    # when
    resp = await cli.get(f"{url}&blockchain_address={account.address}")
    resp_json = await resp.json()

    # then
    assert resp.status == 200
    assert resp_json['status']['success']

    assert parse_url(resp_json['paging']['next']) == parse_url(f"{next_link}&blockchain_address={account.address}")
    assert parse_url(resp_json['paging']['link']) == parse_url(f"{link}&blockchain_address={account.address}")

    assert get_indexes(resp_json['data']['events']) == [make_event_index(*pnt) for pnt in events_on_page]


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
        "invalid_order",
    ]
)
async def test_get_wallet_events_errors(
        cli,
        account_factory,
        create_wallet_events,
        url,
        errors
):
    # given
    account = account_factory.create()
    await create_wallet_events(account.address)

    # when
    resp = await cli.get(f"{url}&blockchain_address={account.address}")
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
async def test_get_events_limits(
        cli: TestClient,
        account_factory: AccountFactory,
        create_wallet_events,
        target_limit: Optional[int],
        expected_items_count: int,
        expected_errors: List[Dict[str, str]],
):
    # given
    account = account_factory.create()

    # Making more than 20 events.
    await create_wallet_events(account.address, tx_in_block=5, internal_tx_in_block=1)

    # when
    reqv_params = f'block_number=latest&blockchain_address={account.address}'

    if target_limit is not None:
        reqv_params += f'&limit={target_limit}'

    resp = await cli.get(f'/v1/wallet/events?{reqv_params}')
    resp_json = await resp.json()

    # then
    observed_errors = resp_json['status']['errors']
    # If 400 is raised -> resp_json['data'] is empty.
    if resp_json['data'] is None:
        observed_items_count = None
    else:
        observed_items_count = len(resp_json['data']['events'])

    assert (observed_errors, observed_items_count) == (expected_errors, expected_items_count)


async def test_get_wallet_events_200_response(cli, block_factory, wallet_events_factory, transaction_factory):
    # given
    block = block_factory.create(number=100)
    tx, _ = transaction_factory.create_for_block(block=block, )
    event = wallet_events_factory.create_token_transfer(tx=tx, block=block)

    url = 'v1/wallet/events?{params}'.format(
        params=urlencode({
            'blockchain_address': event.address,
            'blockchain_tip': block.hash,
        })
    )

    # when
    response = await cli.get(url)
    response_json = await response.json()

    # then
    assert response.status == 200
    assert response_json == {
        'status': {
            'success': True,
            'errors': []
        },
        'meta': {
            'blockchainTipStatus': {
                'blockHash': block.hash,
                'blockNumber': block.number,
                'isOrphaned': False,
                'lastUnchangedBlock': None
            },
            'currentBlockchainTip': {
                'blockHash': block.hash,
                'blockNumber': block.number
            }
        },
        'paging': {
            'link': (
                f'/v1/wallet/events?'
                f'block_number={block.number}&'
                f'event_index={event.event_index}'
                f'&order=desc&'
                f'limit=20&'
                f'blockchain_address={event.address}'
            ),
            'next': None
        },
        'data': {
            'pendingEvents': [],
            'events': [
                {
                    'events': [
                        {
                            'eventData': [
                                {'fieldName': key, 'fieldValue': value} for key, value in event.event_data.items()
                            ],
                            'eventIndex': event.event_index,
                            'eventType': event.type,
                            'eventDirection': 'out'
                        }
                    ],
                    'transaction': {
                        'blockHash': tx.block_hash,
                        'blockNumber': tx.block_number,
                        'timestamp': tx.timestamp,
                        'from': getattr(tx, 'from'),
                        'gas': tx.gas,
                        'gasPrice': tx.gas_price,
                        'hash': tx.hash,
                        'input': tx.input,
                        'nonce': tx.nonce,
                        'status': True,
                        'r': tx.r,
                        's': tx.s,
                        'to': tx.to,
                        'transactionIndex': tx.transaction_index,
                        'v': tx.v,
                        'value': tx.value
                    }
                }
            ],
        }
    }


async def test_get_wallet_events_pending_txs(cli,
                                             block_factory,
                                             pending_transaction_factory):
    # given
    block = block_factory.create()
    pending_tx = pending_transaction_factory.create_eth_transfer()

    url = URL.format(
        params=urlencode({
            'blockchain_address': pending_tx.to,
            'blockchain_tip': block.hash,
            'include_pending_txs': 1
        })
    )

    # when
    response = await cli.get(url)
    response_json = await response.json()

    # then

    assert response_json['data']['pendingEvents'] == [
        {
            'events': [{
                'eventData': [
                    {
                        'fieldName': 'sender',
                        'fieldValue': getattr(pending_tx, 'from')},
                    {
                        'fieldName': 'recipient',
                        'fieldValue': pending_tx.to},
                    {
                        'fieldName': 'amount',
                        'fieldValue': f'{int(pending_tx.value, 16)}'
                    }
                ],
                'eventIndex': 0,
                'eventType': 'eth-transfer',
                'eventDirection': 'in'
            }],
            'transaction': {
                'from': getattr(pending_tx, 'from'),
                'gas': str(pending_tx.gas),
                'gasPrice': str(pending_tx.gas_price),
                'hash': pending_tx.hash,
                'input': pending_tx.input,
                'nonce': str(pending_tx.nonce),
                'removed': False,
                'r': pending_tx.r,
                's': pending_tx.s,
                'to': pending_tx.to,
                'v': pending_tx.v,
                'status': pending_tx.status,
                'value': pending_tx.value,
            }
        }
    ]


async def test_get_wallet_events_pending_txs_limit(cli,
                                                   block_factory,
                                                   wallet_events_factory,
                                                   transaction_factory,
                                                   pending_transaction_factory):
    # given
    block = block_factory.create()
    tx, _ = transaction_factory.create_for_block(block)
    event = wallet_events_factory.create_token_transfer(tx=tx, block=block)
    for i in range(0, 110):
        pending_transaction_factory.create_eth_transfer(to=event.address)

    url = URL.format(
        params=urlencode({
            'blockchain_address': event.address,
            'blockchain_tip': block.hash,
            'include_pending_txs': 1
        })
    )

    # when
    response = await cli.get(url)
    response_json = await response.json()

    # then

    assert len(response_json['data']['pendingEvents']) == 100


async def test_get_wallet_assets_summary(cli, db, transfer_factory, transaction_factory):
    assets = [
        {
            'address': 'a1',
            'asset_address': 'c100',
            'value': 0,
            'decimals': 0,
            'tx_number': 1,
            'nonce': 10,
            'block_number': 100
        },
        {
            'address': 'a1',
            'asset_address': 'c1',
            'value': 100,
            'decimals': 0,
            'tx_number': 1,
            'nonce': 10,
            'block_number': 100
        },
        {
            'address': 'a1',
            'asset_address': 'c2',
            'value': 20000,
            'decimals': 2,
            'tx_number': 2,
            'nonce': 10,
            'block_number': 100
        },
        {
            'address': 'a1',
            'asset_address': '',
            'value': 300,
            'decimals': 0,
            'tx_number': 3,
            'nonce': 10,
            'block_number': 100
        },
        {
            'address': 'a2',
            'asset_address': 'c1',
            'value': 1000,
            'decimals': 1,
            'tx_number': 1,
            'nonce': 5,
            'block_number': 100
        },
    ]

    for a in assets:
        db.execute(assets_summary_t.insert().values(**a))

    transfer_factory.create(address='a1', token_address='c100')
    transfer_factory.create(address='a1', token_address='c1')
    transfer_factory.create(address='a1', token_address='c1')
    transfer_factory.create(address='a1', token_address='c2')
    transfer_factory.create(address='a2', token_address='c1')
    transfer_factory.create(address='a2', token_address='c1')
    transfer_factory.create(address='a2', token_address='c1')
    transfer_factory.create(address='a2', token_address='c1', is_forked=True)

    transaction_factory.create(address='a1')
    transaction_factory.create(address='a1')
    transaction_factory.create(address='a1')
    transaction_factory.create(address='a1', is_forked=True)

    resp = await cli.get(f'/v1/wallet/assets_summary?addresses=a1,a2')
    assert resp.status == 200
    res = (await resp.json())['data']
    assert res == [
        {
            'address': 'a1',
            'assetsSummary': [
                {'address': '', 'balance': "300", 'decimals': "0", 'transfersNumber': 3},
                {'address': 'c1', 'balance': "100", 'decimals': "0", 'transfersNumber': 2},
                {'address': 'c2', 'balance': "20000", 'decimals': "2", 'transfersNumber': 1}
            ],
            'outgoingTransactionsNumber': "10"},
        {
            'address': 'a2',
            'assetsSummary': [
                {'address': 'c1', 'balance': "1000", 'decimals': "1", 'transfersNumber': 3}
            ],
            'outgoingTransactionsNumber': "5"
        }
    ]

    resp = await cli.get(f'/v1/wallet/assets_summary?addresses=a1&assets=c2')
    assert resp.status == 200
    res = (await resp.json())['data']
    assert res == [{'address': 'a1',
                    'assetsSummary': [{'address': 'c2', 'balance': "20000", "decimals": "2", 'transfersNumber': 1}],
                    'outgoingTransactionsNumber': "10"},
                   ]


@pytest.mark.parametrize("url_with_asset", [(True,), (False,)], ids=('without_assets', 'with_assets'))
async def test_get_assets_summary_from_history(cli, assets_summary_factory: AssetsSummaryFactory, url_with_asset,
                                               transfer_factory):
    # given
    token = generate_address()
    token2 = generate_address()
    account = generate_address()

    data = {
        'address': account,
        'asset_address': token,
    }

    # balances
    legacy_balance = assets_summary_factory.create(**data)
    current_balance = assets_summary_factory.create(**{**data, 'block_number': legacy_balance.block_number + 1})

    transfer_factory.create(address=account, token_address=token2)
    transfer_factory.create(address=account, token_address=token)
    transfer_factory.create(address=account, token_address=token)
    transfer_factory.create(address=account, token_address=token, is_forked=True)

    url = f'/v1/wallet/assets_summary?addresses={account}'
    if url_with_asset:
        url = f'{url}&assets={token}'

    # when
    resp = await cli.get(url)
    resp_json = await resp.json()

    # then
    assert resp.status == 200
    assert resp_json['data'] == [{
        'address': account,
        'assetsSummary': [
            {
                'address': token,
                'balance': f'{current_balance.value}',
                'decimals': f'{current_balance.decimals}',
                'transfersNumber': 2
            }
        ],
        'outgoingTransactionsNumber': '1'
    }]


@pytest.mark.parametrize(
    "parameter, value, status",
    (
            ('block_number', 2 ** 128, 400),
            ('block_number', 2 ** 8, 200),
            ('timestamp', 2 ** 128, 400),
            ('timestamp', 2 ** 8, 200),
    ),
    ids=(
            "block_number_with_too_big_value",
            "block_number_with_normal_value",
            "timestamp_with_too_big_value",
            "timestamp_with_normal_value"
    )
)
async def test_get_wallet_events_filter_by_big_value(
        cli: TestClient,
        parameter: str,
        value: int,
        status: int
):
    # given
    params = urlencode({
        parameter: value,
        'blockchain_address': generate_address()
    })
    url = URL.format(params=params)

    # when
    resp = await cli.get(url)

    # then
    assert status == resp.status
