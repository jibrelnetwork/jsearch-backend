import logging

import pytest

from jsearch import settings
from jsearch.api.tests.utils import assert_not_404_response
from jsearch.common.wallet_events import WalletEventType

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


async def test_get_account_balances(cli, main_db_data):
    a1 = main_db_data['accounts_base'][0]
    a2 = main_db_data['accounts_base'][1]
    resp = await cli.get('/v1/accounts/balances?addresses={},{}'.format(a1['address'], a2['address']))
    assert resp.status == 200
    res = (await resp.json())['data']
    assert res == [{'address': a1['address'],
                    'balance': str(main_db_data['accounts_state'][10]['balance'])},
                   {'address': a2['address'],
                    'balance': str(main_db_data['accounts_state'][6]['balance'])}]


async def test_get_account_balances_invalid_addresses_all(cli):
    resp = await cli.get('/v1/accounts/balances?addresses=foobar')
    assert resp.status == 200
    res = (await resp.json())['data']
    assert res == []


async def test_get_account_balances_addresses_have_spaces(cli, main_db_data):
    a1 = main_db_data['accounts_base'][0]
    a2 = main_db_data['accounts_base'][1]
    resp = await cli.get('/v1/accounts/balances?addresses={}, {}'.format(a1['address'], a2['address']))
    res = (await resp.json())['data']
    assert resp.status == 200
    assert len(res) == 2


async def test_get_account_balances_invalid_addresses(cli: object, main_db_data: object) -> object:
    a1 = main_db_data['accounts_base'][0]
    resp = await cli.get('/v1/accounts/balances?addresses={},{},{}'.format('foo', a1['address'], 'bar'))
    assert resp.status == 200
    res = (await resp.json())['data']
    assert res == [{'address': a1['address'],
                    'balance': str(main_db_data['accounts_state'][10]['balance'])}]


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


async def test_get_account_token_balance(cli, main_db_data):
    resp = await cli.get(f'/v1/accounts/a1/token_balance/t1')
    assert resp.status == 200
    res = (await resp.json())['data']
    assert res == {'accountAddress': 'a1', 'decimals': 2, 'balance': 1000, 'contractAddress': 't1'}

    resp = await cli.get(f'/v1/accounts/a3/token_balance/t3')
    assert resp.status == 200
    res = (await resp.json())['data']
    assert res == {'accountAddress': 'a3', 'decimals': 2, 'balance': 5000, 'contractAddress': 't3'}

    resp = await cli.get(f'/v1/accounts/a3/token_balance/tX')
    await assert_not_404_response(resp)

    resp = await cli.get(f'/v1/accounts/aX/token_balance/t1')
    await assert_not_404_response(resp)


async def test_get_accounts_balances_does_not_complain_on_addresses_count_less_than_limit(cli):
    addresses = [f'a{x}' for x in range(settings.API_QUERY_ARRAY_MAX_LENGTH)]
    addresses_str = ','.join(addresses)

    resp = await cli.get(f'/v1/accounts/balances?addresses={addresses_str}')

    assert resp.status == 200


async def test_get_accounts_balances_complains_on_addresses_count_more_than_limit(cli):
    addresses = [f'a{x}' for x in range(settings.API_QUERY_ARRAY_MAX_LENGTH + 1)]
    addresses_str = ','.join(addresses)

    resp = await cli.get(f'/v1/accounts/balances?addresses={addresses_str}')
    resp_json = await resp.json()

    assert resp.status == 400
    assert resp_json['status']['errors'] == [
        {
            'field': 'addresses',
            'error_code': 'TOO_MANY_ITEMS',
            'error_message': 'Too many addresses requested'
        }
    ]


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

    params = f'tokens_addresses=0x1111111111111111111111111111111111111112,0x1111111111111111111111111111111111111113'
    resp = await cli.get(f'v1/accounts/0x1111111111111111111111111111111111111111/token_balances?{params}')
    assert resp.status == 200
    resp_json = await resp.json()
    assert len(resp_json['data']) == 2
    assert resp_json['data'] == [{'accountAddress': '0x1111111111111111111111111111111111111111',
                                  'balance': 2000,
                                  'decimals': 1,
                                  'contractAddress': '0x1111111111111111111111111111111111111112'},
                                 {'accountAddress': '0x1111111111111111111111111111111111111111',
                                  'balance': 30000,
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
    params = f'tokens_addresses={addresses}'
    resp = await cli.get(f'v1/accounts/0x1111111111111111111111111111111111111111/token_balances?{params}')
    assert resp.status == 400
    resp_json = await resp.json()
    assert resp_json == {'data': None,
                         'status': {'errors': [{'error_code': 'TOO_MANY_ITEMS',
                                                'error_message': 'Too many addresses requested',
                                                'field': 'tokens_addresses'}],
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


async def test_get_account_eth_transfers_ok(cli, wallet_events_factory):
    address = '0x1111111111111111111111111111111111111111'
    t1 = wallet_events_factory.create(
        address=address,
        type=WalletEventType.ETH_TRANSFER,
        is_forked=False,
        event_data={'amount': '1000', 'sender': address, 'recepient': '0xa1'},
        timestamp=100,
    )
    wallet_events_factory.create(
        address='0xbb',
        type=WalletEventType.ETH_TRANSFER,
        is_forked=False,
        event_data={'amount': '2000', 'sender': '0xbb', 'recepient': '0xa1'},
        timestamp=101,
    )
    t3 = wallet_events_factory.create(
        address=address,
        type=WalletEventType.ETH_TRANSFER,
        is_forked=False,
        event_data={'amount': '3000', 'sender': '0xaaa', 'recepient': address},
        timestamp=102,
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


async def test_get_account_eth_transfers_page2(cli, wallet_events_factory):
    address = '0x1111111111111111111111111111111111111111'
    wallet_events_factory.create(
        block_number=10,
        event_index=1000,
        address=address,
        type=WalletEventType.ETH_TRANSFER,
        is_forked=False,
        event_data={'amount': '1000', 'sender': address, 'recepient': '0xa1'},
        timestamp=100,
    )
    wallet_events_factory.create(
        block_number=10,
        event_index=1001,
        address=address,
        type=WalletEventType.ETH_TRANSFER,
        is_forked=False,
        event_data={'amount': '2000', 'sender': '0xbb', 'recepient': '0xa1'},
        timestamp=100,
    )
    t3 = wallet_events_factory.create(
        block_number=12,
        event_index=1200,
        address=address,
        type=WalletEventType.ETH_TRANSFER,
        is_forked=False,
        event_data={'amount': '3000', 'sender': '0xaaa', 'recepient': address},
        timestamp=102,
    )
    t4 = wallet_events_factory.create(
        block_number=12,
        event_index=1201,
        address=address,
        type=WalletEventType.ETH_TRANSFER,
        is_forked=False,
        event_data={'amount': '4000', 'sender': '0xaaa', 'recepient': address},
        timestamp=102,
    )
    wallet_events_factory.create(
        block_number=13,
        event_index=1300,
        address=address,
        type=WalletEventType.ETH_TRANSFER,
        is_forked=False,
        event_data={'amount': '5000', 'sender': '0xaaa', 'recepient': address},
        timestamp=103,
    )
    wallet_events_factory.create(
        block_number=13,
        event_index=1301,
        address=address,
        type=WalletEventType.ETH_TRANSFER,
        is_forked=False,
        event_data={'amount': '6000', 'sender': '0xaaa', 'recepient': address},
        timestamp=103,
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
