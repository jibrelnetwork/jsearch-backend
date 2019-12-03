import functools
import logging

import pytest
from aiohttp.test_utils import TestClient

from jsearch.api.tests.utils import assert_not_404_response
from jsearch.tests.plugins.databases.factories.accounts import AccountFactory, AccountStateFactory
from jsearch.tests.plugins.databases.factories.blocks import BlockFactory

logger = logging.getLogger(__name__)


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


@pytest.mark.parametrize('tag', ('latest', 'forked-hash', '14'))
async def test_get_account_does_not_return_data_from_fork(
        cli: TestClient,
        account_factory: AccountFactory,
        account_state_factory: AccountStateFactory,
        block_factory: BlockFactory,
        tag: str,
) -> None:
    canonical = block_factory.create(number=12, is_forked=False)
    forked = block_factory.create(number=14, is_forked=True)

    account = account_factory.create()

    account_state_maker = functools.partial(account_state_factory.create, address=account.address)
    account_state_maker(block_hash=forked.hash, block_number=forked.number, is_forked=True)
    account_state_maker(block_hash=canonical.hash, block_number=canonical.number, is_forked=False)

    tag = forked.hash if tag == 'forked-hash' else tag
    resp = await cli.get(f'/v1/accounts/{account.address}?tag={tag}')
    resp_json = await resp.json()

    assert resp_json["data"]["blockHash"] == canonical.hash
