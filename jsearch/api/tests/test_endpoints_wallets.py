import logging
from urllib.parse import urlencode

import pytest
import time
from typing import Callable, Tuple, List, Dict, Any

from jsearch.api.tests.utils import parse_url
from jsearch.common.wallet_events import make_event_index
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


URL = '/v1/wallet/get_events?{params}'


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
                [(4, 0, 1), (4, 0, 2), (4, 1, 1)],
                URL.format(params=urlencode({
                    'block_number': 4,
                    'event_index': make_event_index(4, 1, 2),
                    'limit': 3,
                    'order': 'asc'
                })),
                URL.format(params=urlencode({
                    'block_number': 4,
                    'event_index': make_event_index(4, 0, 1),
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


async def test_get_wallet_events_200_response(cli, block_factory, wallet_events_factory, transaction_factory):
    # given
    block = block_factory.create(number=100)
    tx, _ = transaction_factory.create_for_block(block=block,)
    event = wallet_events_factory.create_token_transfer(tx=tx, block=block)

    url = 'v1/wallet/get_events?{params}'.format(
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
                f'/v1/wallet/get_events?'
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
                            'eventType': event.type
                        }
                    ],
                    'rootTxData': {
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
                'eventType': 'eth-transfer'
            }],
            'rootTxData': {
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
