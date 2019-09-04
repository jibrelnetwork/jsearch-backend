import logging
from urllib.parse import urlencode

import pytest
from aiohttp.test_utils import TestClient
from typing import Optional, List, Dict

from jsearch.api.tests.utils import assert_not_404_response
from jsearch.common.wallet_events import WalletEventType
from jsearch.tests.plugins.databases.factories.blocks import BlockFactory
from jsearch.tests.plugins.databases.factories.wallet_events import WalletEventsFactory

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.usefixtures('disable_metrics_setup')


async def test_get_account_404(cli):
    resp = await cli.get('/v1/accounts/x')
    await assert_not_404_response(resp)


async def test_get_account(cli, main_db_data):
    resp = await cli.get('/v1/accounts/' + main_db_data['accounts_state'][0]['address'])
    assert resp.status == 200
    account_state = main_db_data['accounts_state'][-1]
    account_base = main_db_data['accounts_base'][0]
    rdata = await resp.json()
    assert rdata['data'] == {'address': account_state['address'],
                             'balance': str(account_state['balance']),
                             'blockHash': account_state['block_hash'],
                             'blockNumber': account_state['block_number'],
                             'code': '0x' + account_base['code'],
                             'codeHash': '0x' + account_base['code_hash'],
                             'nonce': account_state['nonce']}


async def test_get_account_block_number(cli, main_db_data):
    resp = await cli.get('/v1/accounts/{}?tag={}'.format(
        main_db_data['accounts_state'][0]['address'],
        4,
    ))
    assert resp.status == 200
    account_state = main_db_data['accounts_state'][8]
    account_base = main_db_data['accounts_base'][0]
    rdata = await resp.json()
    assert rdata['data'] == {'address': account_state['address'],
                             'balance': str(account_state['balance']),
                             'blockHash': account_state['block_hash'],
                             'blockNumber': account_state['block_number'],
                             'code': '0x' + account_base['code'],
                             'codeHash': '0x' + account_base['code_hash'],
                             'nonce': account_state['nonce']}


async def test_get_account_block_hash(cli, main_db_data):
    resp = await cli.get('/v1/accounts/{}?tag={}'.format(
        main_db_data['accounts_state'][0]['address'],
        main_db_data['blocks'][3]['hash'],
    ))
    assert resp.status == 200
    account_state = main_db_data['accounts_state'][8]
    account_base = main_db_data['accounts_base'][0]
    rdata = await resp.json()
    assert rdata['data'] == {'address': account_state['address'],
                             'balance': str(account_state['balance']),
                             'blockHash': account_state['block_hash'],
                             'blockNumber': account_state['block_number'],
                             'code': '0x' + account_base['code'],
                             'codeHash': '0x' + account_base['code_hash'],
                             'nonce': account_state['nonce']}


async def test_account_get_mined_blocks(cli, main_db_data):
    a1 = main_db_data['accounts_base'][0]
    a2 = main_db_data['accounts_base'][1]

    resp = await cli.get(f'/v1/accounts/{a1["address"]}/mined_blocks')
    assert resp.status == 200

    res = (await resp.json())['data']
    assert len(res) == len(main_db_data['blocks'])

    resp = await cli.get(f'/v1/accounts/{a2["address"]}/mined_blocks')
    res = (await resp.json())['data']

    assert len(res) == 0


async def test_get_account_internal_transactions(cli, block_factory, transaction_factory, internal_transaction_factory):
    block = block_factory.create(timestamp=1550000000)

    tx = transaction_factory.create_for_block(
        block=block,
        **{
            'hash': '0xae334d3879824f8ece42b16f161caaa77417787f779a05534b122de0aabe3f7e',
            'address': '0x3e20a5fe4eb128156c51e310f0391799beccf0c1',
            'from_': '0x3e20a5fe4eb128156c51e310f0391799beccf0c1',
            'to': '0x70137010922f2fc2964b3792907f79fbb75febe8',
        }
    )[0]

    internal_transaction_data = {
        'op': 'suicide',
        'call_depth': NotImplemented,
        'from_': NotImplemented,
        'to': NotImplemented,
        'value': 1000,
        'gas_limit': 2000,
        'payload': '0x',
        'status': 'success',
        'transaction_index': NotImplemented,
    }

    internal_transaction_factory.create_for_tx(
        tx=tx,
        **{
            **internal_transaction_data,
            **{
                'call_depth': 1,
                'from_': '0x1111111111111111111111111111111111111111',
                'to': '0x2222222222222222222222222222222222222222',
                'transaction_index': 7,
            }
        }
    )
    internal_transaction_factory.create_for_tx(
        tx=tx,
        **{
            **internal_transaction_data,
            **{
                'call_depth': 2,
                'from_': '0x2222222222222222222222222222222222222222',
                'to': '0x3333333333333333333333333333333333333333',
                'transaction_index': 8,
            }
        }
    )

    resp = await cli.get(
        f'v1/accounts/0x3e20a5fe4eb128156c51e310f0391799beccf0c1/'
        f'internal_transactions?timestamp=1550000000')
    resp_json = await resp.json()

    assert resp.status == 200
    assert resp_json['status'] == {
        'success': True,
        'errors': [],
    }
    assert resp_json['data'] == [
        {
            'blockNumber': tx.block_number,
            'blockHash': tx.block_hash,
            'timestamp': tx.timestamp,
            'parentTxHash': tx.hash,
            'parentTxIndex': tx.transaction_index,
            'op': 'suicide',
            'callDepth': 2,
            'from': '0x2222222222222222222222222222222222222222',
            'to': '0x3333333333333333333333333333333333333333',
            'value': '1000',
            'gasLimit': '2000',
            'input': '0x',
            'status': 'success',
            'transactionIndex': 8,
        },
        {
            'blockNumber': tx.block_number,
            'blockHash': tx.block_hash,
            'timestamp': tx.timestamp,
            'parentTxHash': tx.hash,
            'parentTxIndex': tx.transaction_index,
            'op': 'suicide',
            'callDepth': 1,
            'from': '0x1111111111111111111111111111111111111111',
            'to': '0x2222222222222222222222222222222222222222',
            'value': '1000',
            'gasLimit': '2000',
            'input': '0x',
            'status': 'success',
            'transactionIndex': 7,
        }
    ]


async def test_get_account_token_balances_multi_ok(cli, token_holder_factory):
    token_holder_factory.create(
        account_address='0x1111111111111111111111111111111111111111',
        token_address='0x1111111111111111111111111111111111111112',
        balance=2000,
        decimals=1
    )
    token_holder_factory.create(
        account_address='0x1111111111111111111111111111111111111111',
        token_address='0x1111111111111111111111111111111111111113',
        balance=30000,
        decimals=3
    )
    token_holder_factory.create(
        account_address='0x1111111111111111111111111111111111111111',
        token_address='0x1111111111111111111111111111111111111114',
        balance=30000,
        decimals=3
    )
    token_holder_factory.create(
        account_address='0x1111111111111111111111111111111111111112',
        token_address='0x1111111111111111111111111111111111111113',
        balance=30000,
        decimals=3
    )

    params = f'contract_addresses=0x1111111111111111111111111111111111111112,0x1111111111111111111111111111111111111113'
    resp = await cli.get(f'v1/accounts/0x1111111111111111111111111111111111111111/token_balances?{params}')
    assert resp.status == 200
    resp_json = await resp.json()
    assert len(resp_json['data']) == 2
    assert resp_json['data'] == [{'balance': '2000',
                                  'decimals': 1,
                                  'contractAddress': '0x1111111111111111111111111111111111111112'},
                                 {'balance': '30000',
                                  'decimals': 3,
                                  'contractAddress': '0x1111111111111111111111111111111111111113'}]
    assert 'meta' not in resp_json


async def test_get_account_token_balances_multi_no_addresses(cli, token_holder_factory):
    token_holder_factory.create(
        account_address='0x1111111111111111111111111111111111111111',
        token_address='0x1111111111111111111111111111111111111112',
        balance=2000
    )
    token_holder_factory.create(
        account_address='0x1111111111111111111111111111111111111111',
        token_address='0x1111111111111111111111111111111111111113',
        balance=30000
    )
    params = ''
    resp = await cli.get(f'v1/accounts/0x1111111111111111111111111111111111111111/token_balances?{params}')
    assert resp.status == 200
    resp_json = await resp.json()
    assert len(resp_json['data']) == 0
    assert resp_json['data'] == []


async def test_get_account_token_balances_multi_too_many_addresses(cli, token_holder_factory):
    token_holder_factory.create(
        account_address='0x1111111111111111111111111111111111111111',
        token_address='0x1111111111111111111111111111111111111112',
        balance=2000
    )
    token_holder_factory.create(
        account_address='0x1111111111111111111111111111111111111111',
        token_address='0x1111111111111111111111111111111111111113',
        balance=30000
    )

    addresses = ','.join(['0x11111111111111111111111111111111111111{}'.format(n) for n in range(12, 38)])
    params = f'contract_addresses={addresses}'
    resp = await cli.get(f'v1/accounts/0x1111111111111111111111111111111111111111/token_balances?{params}')
    assert resp.status == 400
    resp_json = await resp.json()
    assert resp_json == {'data': None,
                         'status': {'errors': [{'error_code': 'TOO_MANY_ITEMS',
                                                'error_message': 'Too many addresses requested',
                                                'field': 'contract_addresses'}],
                                    'success': False}}


async def test_get_account_transaction_count_w_pending(cli, account_state_factory, pending_transaction_factory):
    account_state_factory.create(
        address='0x1111111111111111111111111111111111111111',
        nonce=5,
        block_number=5,
        is_forked=False,
    )
    account_state_factory.create(
        address='0x1111111111111111111111111111111111111111',
        nonce=6,
        block_number=7,
        is_forked=False,
    )
    account_state_factory.create(
        address='0x1111111111111111111111111111111111111111',
        nonce=7,
        block_number=9,
        is_forked=True,
    )
    pending_transaction_factory.create(
        from_='0x1111111111111111111111111111111111111111',
        removed=False,
        last_synced_id=123,
    )

    resp = await cli.get(f'v1/accounts/0x1111111111111111111111111111111111111111/transaction_count')
    assert resp.status == 200
    resp_json = await resp.json()
    assert resp_json['data'] == 7


async def test_get_account_eth_transfers(cli, wallet_events_factory):
    address = '0x9cef2704a5ec2073bbba030906e24235ff2fde2f'
    wallet_events_factory.create(
        address=address,
        type="eth-transfer",
        tx_hash='0xa682d04025637c3b249586be9a7578e68af6d7b3060941ccd91202186251ba76',
        block_hash='0x9c6eba83e130251df94da89b87758d074a04a6859d9403d8dd8358bfb436aefe',
        block_number=8345641,
        event_index='83456410000000',
        is_forked=False,
        tx_data={
            "r": "0xf19b497d0ec5d040f72e8fb1b3705a1540386416b5bdc0cb555d35884bcd4e3",
            "s": "0x4a9aacab1dbe461e569b3826d8a64d124c96839613d0a3d6436d542f900caa1e",
            "v": "0x1b",
            "to": "0x3c2f261fc6d26c27c49a9574defe469b53c09d0d",
            "gas": "0x5208",
            "from": "0x9cef2704a5ec2073bbba030906e24235ff2fde2f",
            "hash": "0xa682d04025637c3b249586be9a7578e68af6d7b3060941ccd91202186251ba76",
            "input": "0x",
            "nonce": "0x27",
            "value": "0x58c1821b6439000",
            "gas_price": "0x1000000000",
            "transaction_index": 0,
            "block_hash": "0x9c6eba83e130251df94da89b87758d074a04a6859d9403d8dd8358bfb436aefe",
            "block_number": 8345641,
            "is_forked": False,
            "timestamp": 1565745636,
            "address": "0x9cef2704a5ec2073bbba030906e24235ff2fde2f",
            "status": 1
        },
        event_data={
            "sender": "0x9cef2704a5ec2073bbba030906e24235ff2fde2f",
            "recipient": "0x3c2f261fc6d26c27c49a9574defe469b53c09d0d",
            "amount": "399721000000000000",
            "status": 1,
        },
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


async def test_get_account_eth_transfers_does_not_throws_500_if_there_s_no_timestamp_in_tx(cli, wallet_events_factory):
    event = wallet_events_factory.create(
        type=WalletEventType.ETH_TRANSFER,
        tx_data={},
        event_data={'amount': '', 'sender': '', 'recipient': ''}
    )

    resp = await cli.get(f'v1/accounts/{event.address}/eth_transfers')
    resp_json = await resp.json()

    assert len(resp_json['data']) == 1


async def test_get_account_eth_transfers_ok(cli, wallet_events_factory):
    address = '0x1111111111111111111111111111111111111111'
    t1 = wallet_events_factory.create(
        address=address,
        type=WalletEventType.ETH_TRANSFER,
        is_forked=False,
        event_data={'amount': '1000', 'sender': address, 'recipient': '0xa1'},
        tx_data={'timestamp': 100},
    )
    wallet_events_factory.create(
        address='0xbb',
        type=WalletEventType.ETH_TRANSFER,
        is_forked=False,
        event_data={'amount': '2000', 'sender': '0xbb', 'recipient': '0xa1'},
        tx_data={'timestamp': 101},
    )
    t3 = wallet_events_factory.create(
        address=address,
        type=WalletEventType.ETH_TRANSFER,
        is_forked=False,
        event_data={'amount': '3000', 'sender': '0xaaa', 'recipient': address},
        tx_data={'timestamp': 102},
    )
    resp = await cli.get(f'v1/accounts/{address}/eth_transfers')
    assert resp.status == 200
    resp_json = await resp.json()
    assert resp_json['data'] == [
        {'amount': '3000',
         'from': '0xaaa',
         'timestamp': 102,
         'to': '0x1111111111111111111111111111111111111111',
         'transactionHash': t3.tx_hash},
        {'amount': '1000',
         'from': '0x1111111111111111111111111111111111111111',
         'timestamp': 100,
         'to': '0xa1',
         'transactionHash': t1.tx_hash},
    ]
    assert resp_json['paging'] == {
        'link': f'/v1/accounts/{address}/eth_transfers?block_number=2&event_index=2&order=desc&limit=20',
        'next': None
    }

    resp = await cli.get(f'v1/accounts/{address}/eth_transfers?limit=1')
    resp_json = await resp.json()
    assert resp_json['data'] == [
        {'amount': '3000',
         'from': '0xaaa',
         'timestamp': 102,
         'to': '0x1111111111111111111111111111111111111111',
         'transactionHash': t3.tx_hash},
    ]
    assert resp_json['paging'] == {
        'link': f'/v1/accounts/{address}/eth_transfers?block_number=2&event_index=2&order=desc&limit=1',
        'next': f'/v1/accounts/{address}/eth_transfers?block_number=0&event_index=0&order=desc&limit=1'
    }

    resp = await cli.get(resp_json['paging']['next'])
    resp_json = await resp.json()
    assert resp_json['data'] == [
        {'amount': '1000',
         'from': '0x1111111111111111111111111111111111111111',
         'timestamp': 100,
         'to': '0xa1',
         'transactionHash': t1.tx_hash},
    ]


async def test_get_account_eth_transfers_page2(cli, wallet_events_factory, block_factory):
    address = '0x1111111111111111111111111111111111111111'
    block_factory.create(number=12, timestamp=102)
    wallet_events_factory.create(
        block_number=10,
        event_index=1000,
        address=address,
        type=WalletEventType.ETH_TRANSFER,
        is_forked=False,
        event_data={'amount': '1000', 'sender': address, 'recipient': '0xa1'},
        tx_data={'timestamp': 100},
    )
    wallet_events_factory.create(
        block_number=10,
        event_index=1001,
        address=address,
        type=WalletEventType.ETH_TRANSFER,
        is_forked=False,
        event_data={'amount': '2000', 'sender': '0xbb', 'recipient': '0xa1'},
        tx_data={'timestamp': 100},
    )
    t3 = wallet_events_factory.create(
        block_number=12,
        event_index=1200,
        address=address,
        type=WalletEventType.ETH_TRANSFER,
        is_forked=False,
        event_data={'amount': '3000', 'sender': '0xaaa', 'recipient': address},
        tx_data={'timestamp': 102},
    )
    t4 = wallet_events_factory.create(
        block_number=12,
        event_index=1201,
        address=address,
        type=WalletEventType.ETH_TRANSFER,
        is_forked=False,
        event_data={'amount': '4000', 'sender': '0xaaa', 'recipient': address},
        tx_data={'timestamp': 102},
    )
    wallet_events_factory.create(
        block_number=13,
        event_index=1300,
        address=address,
        type=WalletEventType.ETH_TRANSFER,
        is_forked=False,
        event_data={'amount': '5000', 'sender': '0xaaa', 'recipient': address},
        tx_data={'timestamp': 103},
    )
    wallet_events_factory.create(
        block_number=13,
        event_index=1301,
        address=address,
        type=WalletEventType.ETH_TRANSFER,
        is_forked=False,
        event_data={'amount': '6000', 'sender': '0xaaa', 'recipient': address},
        tx_data={'timestamp': 103},
    )
    resp = await cli.get(f'v1/accounts/{address}/eth_transfers?block_number=12&event_index=1201&limit=2')
    assert resp.status == 200
    resp_json = await resp.json()
    assert resp_json['data'] == [{'amount': '4000',
                                  'from': '0xaaa',
                                  'timestamp': 102,
                                  'to': '0x1111111111111111111111111111111111111111',
                                  'transactionHash': t4.tx_hash},
                                 {'amount': '3000',
                                  'from': '0xaaa',
                                  'timestamp': 102,
                                  'to': '0x1111111111111111111111111111111111111111',
                                  'transactionHash': t3.tx_hash}
                                 ]
    assert resp_json['paging'] == {
        'link': f'/v1/accounts/{address}/eth_transfers?block_number=12&event_index=1201&order=desc&limit=2',
        'next': f'/v1/accounts/{address}/eth_transfers?block_number=10&event_index=1001&order=desc&limit=2'}

    resp = await cli.get(f'v1/accounts/{address}/eth_transfers?block_number=12&event_index=1200&order=asc&limit=2')
    assert resp.status == 200
    resp_json = await resp.json()
    assert resp_json['data'] == [
        {'amount': '3000',
         'from': '0xaaa',
         'timestamp': 102,
         'to': '0x1111111111111111111111111111111111111111',
         'transactionHash': t3.tx_hash},
        {'amount': '4000',
         'from': '0xaaa',
         'timestamp': 102,
         'to': '0x1111111111111111111111111111111111111111',
         'transactionHash': t4.tx_hash},
    ]
    assert resp_json['paging'] == {
        'link': f'/v1/accounts/{address}/eth_transfers?block_number=12&event_index=1200&order=asc&limit=2',
        'next': f'/v1/accounts/{address}/eth_transfers?block_number=13&event_index=1300&order=asc&limit=2'}

    resp = await cli.get(f'v1/accounts/{address}/eth_transfers?timestamp=102&order=asc&limit=2')
    assert resp.status == 200
    resp_json = await resp.json()
    assert resp_json['data'] == [
        {'amount': '3000',
         'from': '0xaaa',
         'timestamp': 102,
         'to': '0x1111111111111111111111111111111111111111',
         'transactionHash': t3.tx_hash},
        {'amount': '4000',
         'from': '0xaaa',
         'timestamp': 102,
         'to': '0x1111111111111111111111111111111111111111',
         'transactionHash': t4.tx_hash},
    ]
    assert resp_json['paging'] == {
        'link': f'/v1/accounts/{address}/eth_transfers?timestamp=102&event_index=1200&order=asc&limit=2',
        'next': f'/v1/accounts/{address}/eth_transfers?timestamp=103&event_index=1300&order=asc&limit=2'}


@pytest.mark.parametrize(
    "target_limit, expected_items_count, expected_errors",
    (
            (None, 20, []),
            (19, 19, []),
            (20, 20, []),
            (21, 0, [
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
async def test_get_accounts_eth_transfers(
        cli: TestClient,
        block_factory: BlockFactory,
        wallet_events_factory: WalletEventsFactory,
        target_limit: Optional[int],
        expected_items_count: int,
        expected_errors: List[Dict[str, str]],
):
    # given
    address = '0xcd424c53f5dc7d22cdff536309c24ad87a97e6af'

    block_factory.create(number=1)
    wallet_events_factory.create_batch(
        25,
        block_number=1,
        address=address,
        type=WalletEventType.ETH_TRANSFER,
        event_data={'amount': '6000', 'sender': '0xaaa', 'recipient': address},
        tx_data={'timestamp': 100}
    )

    # when
    reqv_params = 'block_number=latest'

    if target_limit is not None:
        reqv_params += f'&limit={target_limit}'

    resp = await cli.get(f'/v1/accounts/{address}/eth_transfers?{reqv_params}')
    resp_json = await resp.json()

    # then
    observed_errors = resp_json['status']['errors']
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
