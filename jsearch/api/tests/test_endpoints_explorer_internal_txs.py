import pytest
from aiohttp.test_utils import TestClient

from jsearch.tests.plugins.databases.factories.internal_transactions import InternalTransactionFactory
from jsearch.tests.plugins.databases.factories.transactions import TransactionFactory


async def test_get_internal_transactions(cli, internal_transaction_factory):
    internal_transaction_data = {
        'block_number': 42,
        'block_hash': '0xa47a6185aa22e64647207caedd0ce8b2b1ae419added75fc3b7843c72b6386bd',
        'timestamp': 1550000000,
        'parent_tx_hash': '0xae334d3879824f8ece42b16f161caaa77417787f779a05534b122de0aabe3f7e',
        'op': 'suicide',
        'call_depth': 3,
        'from_': '0x3e20a5fe4eb128156c51e310f0391799beccf0c1',
        'to': '0x70137010922f2fc2964b3792907f79fbb75febe8',
        'value': 1000,
        'gas_limit': 2000,
        'payload': '0x',
        'status': 'success',
        'transaction_index': NotImplemented,
    }

    internal_transaction_factory.create(
        **{
            **internal_transaction_data,
            **{
                'parent_tx_index': 1,
                'transaction_index': 42
            },
        }
    )
    internal_transaction_factory.create(
        **{
            **internal_transaction_data,
            **{
                'parent_tx_index': 1,
                'transaction_index': 43
            },
        }
    )

    resp = await cli.get(
        f'v1/transactions/0xae334d3879824f8ece42b16f161caaa77417787f779a05534b122de0aabe3f7e/internal_transactions'
    )
    resp_json = await resp.json()

    assert resp.status == 200
    assert resp_json == {
        'status': {
            'success': True,
            'errors': [],
        },
        'data': [
            {
                'blockNumber': 42,
                'blockHash': '0xa47a6185aa22e64647207caedd0ce8b2b1ae419added75fc3b7843c72b6386bd',
                'timestamp': 1550000000,
                'parentTxHash': '0xae334d3879824f8ece42b16f161caaa77417787f779a05534b122de0aabe3f7e',
                'parentTxIndex': 1,
                'op': 'suicide',
                'callDepth': 3,
                'from': '0x3e20a5fe4eb128156c51e310f0391799beccf0c1',
                'to': '0x70137010922f2fc2964b3792907f79fbb75febe8',
                'value': '1000',
                'gasLimit': '2000',
                'input': '0x',
                'status': 'success',
                'transactionIndex': 43,
            },
            {
                'blockNumber': 42,
                'blockHash': '0xa47a6185aa22e64647207caedd0ce8b2b1ae419added75fc3b7843c72b6386bd',
                'timestamp': 1550000000,
                'parentTxHash': '0xae334d3879824f8ece42b16f161caaa77417787f779a05534b122de0aabe3f7e',
                'parentTxIndex': 1,
                'op': 'suicide',
                'callDepth': 3,
                'from': '0x3e20a5fe4eb128156c51e310f0391799beccf0c1',
                'to': '0x70137010922f2fc2964b3792907f79fbb75febe8',
                'value': '1000',
                'gasLimit': '2000',
                'input': '0x',
                'status': 'success',
                'transactionIndex': 42,
            }
        ]
    }


@pytest.mark.parametrize(
    'limit, offset',
    (
        ('10', '0'),
        ('0', '10'),
        ('10', '10'),
        ('aaa', 'bbb'),
    ),
    ids=[
        'with limit without offset',
        'without limit with offset',
        'with limit and offset',
        'invalid limit and offset',
    ]
)
async def test_get_internal_transactions_does_not_care_about_limit_and_offset(
        cli: TestClient,
        internal_transaction_factory: InternalTransactionFactory,
        limit: str,
        offset: str
) -> None:

    tx_hash = '0xae334d3879824f8ece42b16f161caaa77417787f779a05534b122de0aabe3f7e'
    internal_transaction_factory.create_batch(21, parent_tx_hash=tx_hash)

    resp = await cli.get(f'v1/transactions/{tx_hash}/internal_transactions?limit={limit}&offset={offset}')
    resp_json = await resp.json()

    assert len(resp_json['data']) == 21


@pytest.mark.parametrize(
    'order, expected_indexes',
    (
        ('asc', [0, 1, 2, 3, 4]),
        ('desc', [4, 3, 2, 1, 0]),
    ),
    ids=['asc', 'desc']
)
async def test_get_internal_transactions_with_ordering(
        cli: TestClient,
        transaction_factory: TransactionFactory,
        internal_transaction_factory: InternalTransactionFactory,
        order: str,
        expected_indexes: str
) -> None:

    tx_hash = '0xae334d3879824f8ece42b16f161caaa77417787f779a05534b122de0aabe3f7e'
    tx = transaction_factory.create(hash=tx_hash)

    for index in [0, 1, 2, 3, 4]:
        internal_transaction_factory.create_for_tx(tx, transaction_index=index)

    resp = await cli.get(f'v1/transactions/{tx_hash}/internal_transactions?order={order}')
    resp_json = await resp.json()

    assert [x['transactionIndex'] for x in resp_json['data']] == expected_indexes


async def test_get_internal_transactions_with_invalid_ordering_complains_about_queryparam(
        cli: TestClient,
        internal_transaction_factory: InternalTransactionFactory,
) -> None:

    tx_hash = '0xae334d3879824f8ece42b16f161caaa77417787f779a05534b122de0aabe3f7e'
    internal_transaction_factory.create(parent_tx_hash=tx_hash)

    resp = await cli.get(f'v1/transactions/{tx_hash}/internal_transactions?order=ascending')
    resp_json = await resp.json()

    assert (resp.status, resp_json['status']['errors']) == (
        400,
        [
            {
                "field": "order",
                "message": 'Ordering can be either "asc" or "desc".',
                "code": "INVALID_ORDER_VALUE"
            }
        ],
    )
