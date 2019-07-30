import logging

import pytest

from jsearch import settings
from jsearch.api.tests.utils import assert_not_404_response

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.usefixtures('disable_metrics_setup')

_account_1_transfers = [
    {
        'from': 'a3',
        'timestamp': 1529159847,
        'to': 'a1',
        'contractAddress': 'c2',
        'decimals': 2,
        'amount': '500',
        'transactionHash': 't2'
    },
    {
        'from': 'a1',
        'timestamp': 1529159847,
        'to': 'a3',
        'contractAddress': 'c1',
        'decimals': 2,
        'amount': '300',
        'transactionHash': 't1'
    },
    {
        'from': 'a2',
        'timestamp': 1529159847,
        'to': 'a1',
        'contractAddress': 'c2',
        'decimals': 2,
        'amount': '200',
        'transactionHash': 't1'
    },
    {
        'from': 'a2',
        'timestamp': 1529159847,
        'to': 'a1',
        'contractAddress': 'c1',
        'decimals': 2,
        'amount': '100',
        'transactionHash': 't1'
    }
]


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
                             'balance': hex(account_state['balance']),
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
                             'balance': hex(account_state['balance']),
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
                             'balance': hex(account_state['balance']),
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
                    'balance': hex(main_db_data['accounts_state'][10]['balance'])},
                   {'address': a2['address'],
                    'balance': hex(main_db_data['accounts_state'][6]['balance'])}]


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
                    'balance': hex(main_db_data['accounts_state'][10]['balance'])}]


async def test_get_account_token_transfers(cli, main_db_data):
    resp = await cli.get(f'/v1/accounts/a1/token_transfers')
    assert resp.status == 200
    assert (await resp.json())['data'] == _account_1_transfers[:]


async def test_get_account_token_transfers_asc(cli, main_db_data):
    resp = await cli.get(f'/v1/accounts/a1/token_transfers?order=asc')
    assert resp.status == 200
    assert (await resp.json())['data'] == _account_1_transfers[::-1]


async def test_get_account_token_transfers_limit(cli, main_db_data):
    resp = await cli.get(f'/v1/accounts/a1/token_transfers?limit=1')
    assert resp.status == 200
    assert (await resp.json())['data'] == _account_1_transfers[:1]


async def test_get_account_token_transfers_offset(cli, main_db_data):
    resp = await cli.get(f'/v1/accounts/a1/token_transfers?offset=1')
    assert resp.status == 200
    assert (await resp.json())['data'] == _account_1_transfers[1:]


async def test_get_account_token_transfers_a2(cli, main_db_data):
    resp = await cli.get(f'/v1/accounts/a2/token_transfers')
    assert resp.status == 200
    assert (await resp.json())['data'] == [{'from': 'a2',
                                            'timestamp': 1529159847,
                                            'to': 'a1',
                                            'contractAddress': 'c2',
                                            'decimals': 2,
                                            'amount': '200',
                                            'transactionHash': 't1'},
                                           {'from': 'a2',
                                            'timestamp': 1529159847,
                                            'to': 'a1',
                                            'contractAddress': 'c1',
                                            'decimals': 2,
                                            'amount': '100',
                                            'transactionHash': 't1'}]


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


async def test_get_token_holders(cli, main_db_data):
    resp = await cli.get(f'/v1/tokens/t1/holders')
    assert resp.status == 200
    res = (await resp.json())['data']
    assert res == [{'accountAddress': 'a3', 'decimals': 2, 'balance': 3000, 'contractAddress': 't1'},
                   {'accountAddress': 'a2', 'decimals': 2, 'balance': 2000, 'contractAddress': 't1'},
                   {'accountAddress': 'a1', 'decimals': 2, 'balance': 1000, 'contractAddress': 't1'}]

    resp = await cli.get(f'/v1/tokens/t1/holders?order=asc')
    assert resp.status == 200
    res = (await resp.json())['data']
    assert res == [{'accountAddress': 'a1', 'decimals': 2, 'balance': 1000, 'contractAddress': 't1'},
                   {'accountAddress': 'a2', 'decimals': 2, 'balance': 2000, 'contractAddress': 't1'},
                   {'accountAddress': 'a3', 'decimals': 2, 'balance': 3000, 'contractAddress': 't1'}]

    resp = await cli.get(f'/v1/tokens/t3/holders?order=asc&limit=2&offset=1')
    assert resp.status == 200
    res = (await resp.json())['data']
    assert res == [{'accountAddress': 'a3', 'decimals': 2, 'balance': 5000, 'contractAddress': 't3'},
                   {'accountAddress': 'a4', 'decimals': 2, 'balance': 6000, 'contractAddress': 't3'}]


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


async def test_get_account_logs(cli, db, main_db_data):
    from jsearch.api import models

    address = "0xbb4af59aeaf2e83684567982af5ca21e9ac8419a"
    logs = [models.Log(**item).to_dict() for item in main_db_data['logs'] if item['address'] == address]

    resp = await cli.get(f'/v1/accounts/{address}/logs?order=asc')
    resp_json = await resp.json()

    assert resp_json['data'] == logs
    assert resp_json == {'data': logs, 'status': {'errors': [], 'success': True}}


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
    assert resp_json == {
        'status': {
            'success': True,
            'errors': [],
        },
        'data': [
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
    }


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
        last_synced_id=123
    )

    resp = await cli.get(f'v1/accounts/0x1111111111111111111111111111111111111111/transaction_count')
    assert resp.status == 200
    resp_json = await resp.json()
    assert resp_json['data'] == 7
