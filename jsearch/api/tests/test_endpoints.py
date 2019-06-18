import logging
from unittest import mock
from urllib.parse import urlencode

import pytest
from aiohttp import ClientResponse
from asynctest import CoroutineMock
from typing import Optional, Set, Union

from sqlalchemy import select

from jsearch import settings
from jsearch.api.error_code import ErrorCode
from jsearch.common.tables import (
    assets_transfers_t,
    transactions_t,
    assets_summary_t,
    accounts_state_t,
)
from jsearch.tests.entities import (
    TransactionFromDumpWrapper,
    BlockFromDumpWrapper,
)

pytest_plugins = [
    'jsearch.tests.plugins.databases.main_db',
    'jsearch.tests.plugins.databases.dumps',
    'jsearch.tests.plugins.databases.factories.blocks',
    'jsearch.tests.plugins.databases.factories.transactions',
    'jsearch.tests.plugins.databases.factories.internal_transactions',
    'jsearch.tests.plugins.databases.factories.pending_transactions',
    'jsearch.tests.plugins.databases.factories.wallet_events',
    'jsearch.tests.plugins.databases.factories.chain_splits',
    'jsearch.tests.plugins.databases.factories.reorgs',
]

logger = logging.getLogger(__name__)


async def assert_not_404_response(response: ClientResponse) -> None:
    from jsearch.api.error_code import ErrorCode

    assert response.status == 404

    data = await response.json()

    assert data['status']['success'] is False
    assert data['status']['errors'] == [
        {
            'code': ErrorCode.RESOURCE_NOT_FOUND,
            'message': 'Resource not found'
        }
    ]


async def test_get_block_404(cli):
    resp = await cli.get('/v1/blocks/1')
    await assert_not_404_response(resp)


async def test_get_block_by_number(cli, block_factory, transaction_factory):
    # given
    block = block_factory.create()
    from_tx, to_tx = transaction_factory.create_for_block(block)

    # when
    resp = await cli.get(f'/v1/blocks/{block.number}')
    assert resp.status == 200

    # then
    payload = await resp.json()
    assert payload['status'] == {'success': True, 'errors': []}
    assert payload['data'] == {
        'difficulty': str(block.difficulty),
        'extraData': block.extra_data,
        'gasLimit': str(block.gas_limit),
        'gasUsed': str(block.gas_used),
        'hash': block.hash,
        'logsBloom': block.logs_bloom,
        'miner': block.miner,
        'mixHash': block.mix_hash,
        'nonce': block.nonce,
        'number': block.number,
        'parentHash': block.parent_hash,
        'receiptsRoot': block.receipts_root,
        'sha3Uncles': block.sha3_uncles,
        'stateRoot': block.state_root,
        'timestamp': block.timestamp,
        'transactions': [from_tx.hash],
        'transactionsRoot': block.transactions_root,
        'staticReward': str(hex(int(block.static_reward))),
        'txFees': str(hex(int(block.tx_fees))),
        'uncleInclusionReward': str(hex(int(block.uncle_inclusion_reward))),
        'uncles': []
    }


async def test_get_block_with_empty_uncles_list(cli, main_db_data):
    # given
    block_number = 1
    block = BlockFromDumpWrapper.from_dump(
        dump=main_db_data,
        filters={'number': block_number},
    )
    # then
    resp = await cli.get(f'/v1/blocks/{block_number}')
    assert resp.status == 200

    payload = await resp.json()
    assert payload['status'] == {'success': True, 'errors': []}
    assert payload['data'] == block.as_dict()

    assert payload['data']['uncles'] == []
    assert isinstance(payload['data']['uncles'], list)


async def test_get_block_with_uncles(cli, main_db_data, uncles):
    # given
    block_hash = '0x691d775caa1979538e2c3e68678d149567e3398480a6c4389585b16a312635f5'
    uncle_hashes = [uncle['hash'] for uncle in uncles if uncle['block_hash'] == block_hash]
    txs = TransactionFromDumpWrapper.from_dump(
        main_db_data,
        filters={"block_hash": block_hash},
        bulk=True
    )
    block = BlockFromDumpWrapper.from_dump(
        dump=main_db_data,
        filters={'hash': block_hash},
        uncles=uncle_hashes,
        transactions=[tx.entity.hash for tx in txs]
    )
    # then
    resp = await cli.get(f'/v1/blocks/{block_hash}')
    assert resp.status == 200

    payload = await resp.json()
    assert payload['status'] == {'success': True, 'errors': []}
    assert payload['data'] == block.as_dict()
    assert payload['data']['uncles'] == uncle_hashes


async def test_get_block_by_number_no_forked(cli, db):
    # given
    db.execute('INSERT INTO blocks (number, hash, is_forked, static_reward, uncle_inclusion_reward, tx_fees)'
               'values (%s, %s, %s, %s, %s, %s)', [
                   (1, 'aa', False, 0, 0, 0),
                   (2, 'ab', False, 0, 0, 0),
                   (2, 'ax', True, 0, 0, 0),
                   (3, 'ac', False, 0, 0, 0),
               ])
    # then
    resp = await cli.get('/v1/blocks/2')
    assert resp.status == 200
    b = await resp.json()
    assert b['data']['hash'] == 'ab'
    assert b['data']['number'] == 2


async def test_get_block_by_hash_forked_404(cli, db):
    # given
    db.execute('INSERT INTO blocks (number, hash, is_forked, static_reward, uncle_inclusion_reward, tx_fees)'
               'values (%s, %s, %s, %s, %s, %s)', [
                   (1, 'aa', False, 0, 0, 0),
                   (2, 'ab', False, 0, 0, 0),
                   (2, 'ax', True, 0, 0, 0),
                   (3, 'ac', False, 0, 0, 0),
               ])
    # then
    resp = await cli.get('/v1/blocks/ax')
    await assert_not_404_response(resp)


async def test_get_block_by_hash(cli, main_db_data):
    resp = await cli.get('/v1/blocks/' + main_db_data['blocks'][0]['hash'])
    assert resp.status == 200
    b = main_db_data['blocks'][0]
    rdata = await resp.json()
    assert rdata['data'] == {
        'difficulty': str(b['difficulty']),
        'extraData': b['extra_data'],
        'gasLimit': str(b['gas_limit']),
        'gasUsed': str(b['gas_used']),
        'hash': b['hash'],
        'logsBloom': b['logs_bloom'],
        'miner': b['miner'],
        'mixHash': b['mix_hash'],
        'nonce': b['nonce'],
        'number': b['number'],
        'parentHash': b['parent_hash'],
        'receiptsRoot': b['receipts_root'],
        'sha3Uncles': b['sha3_uncles'],
        'stateRoot': b['state_root'],
        'timestamp': b['timestamp'],
        'transactions': [],
        'transactionsRoot': b['transactions_root'],
        'staticReward': hex(b['static_reward']),
        'txFees': hex(b['tx_fees']),
        'uncleInclusionReward': hex(b['uncle_inclusion_reward']),
        'uncles': []
    }


async def test_get_block_latest(cli, main_db_data):
    resp = await cli.get('/v1/blocks/latest')
    assert resp.status == 200
    b = main_db_data['blocks'][-1]
    assert (await resp.json())['data'] == {
        'difficulty': str(b['difficulty']),
        'extraData': b['extra_data'],
        'gasLimit': str(b['gas_limit']),
        'gasUsed': str(b['gas_used']),
        'hash': b['hash'],
        'logsBloom': b['logs_bloom'],
        'miner': b['miner'],
        'mixHash': b['mix_hash'],
        'nonce': b['nonce'],
        'number': b['number'],
        'parentHash': b['parent_hash'],
        'receiptsRoot': b['receipts_root'],
        'sha3Uncles': b['sha3_uncles'],
        'stateRoot': b['state_root'],
        'timestamp': b['timestamp'],
        'transactions': [main_db_data['transactions'][-1]['hash']],
        'transactionsRoot': b['transactions_root'],
        'staticReward': hex(b['static_reward']),
        'txFees': hex(b['tx_fees']),
        'uncleInclusionReward': hex(b['uncle_inclusion_reward']),
        'uncles': []
    }


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
                             'code': account_base['code'],
                             'codeHash': account_base['code_hash'],
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
                             'code': account_base['code'],
                             'codeHash': account_base['code_hash'],
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
                             'code': account_base['code'],
                             'codeHash': account_base['code_hash'],
                             'nonce': account_state['nonce']}


async def test_get_account_transactions(cli, main_db_data):
    address = main_db_data['accounts_state'][0]['address']

    resp = await cli.get(f'/v1/accounts/{address}/transactions')
    assert resp.status == 200
    txs = main_db_data['transactions']
    res = (await resp.json())['data']
    assert len(res) == 4
    assert res[0]['hash'] == txs[0]['hash']
    assert res[1]['hash'] == txs[1]['hash']
    assert res[2]['hash'] == txs[2]['hash']
    assert res[3]['hash'] == txs[4]['hash']
    assert res[0] == {
        'blockHash': txs[0]['block_hash'],
        'blockNumber': txs[0]['block_number'],
        'from': txs[0]['from'],
        'gas': txs[0]['gas'],
        'gasPrice': txs[0]['gas_price'],
        'hash': txs[0]['hash'],
        'input': txs[0]['input'],
        'nonce': txs[0]['nonce'],
        'r': txs[0]['r'],
        's': txs[0]['s'],
        'to': txs[0]['to'],
        'status': False,
        'transactionIndex': txs[0]['transaction_index'],
        'v': txs[0]['v'],
        'value': txs[0]['value'],
    }


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


async def test_get_block_transactions(cli, block_factory, transaction_factory):
    # given
    block = block_factory.create()
    txs = transaction_factory.create_for_block(block)
    tx = txs[0]

    # when
    resp = await cli.get(f'/v1/blocks/{block.hash}/transactions')
    assert resp.status == 200

    # then
    res = (await resp.json())['data']
    assert len(res) == 1
    assert res == [
        {
            'blockHash': tx.block_hash,
            'blockNumber': tx.block_number,
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
            'value': tx.value,
        }
    ]


async def test_get_block_transactions_forked(cli, db):
    # given
    db.execute('INSERT INTO transactions (block_number, block_hash, hash, is_forked, transaction_index)'
               'values (%s, %s, %s, %s, %s)', [
                   (1, 'aa', 'tx1', False, 1),
                   (2, 'ab', 'tx2', False, 1),
                   (2, 'ax', 'tx3', True, 1),
                   (3, 'ac', 'tx3', False, 1),
               ])
    # then
    resp = await cli.get('/v1/blocks/2/transactions')
    assert resp.status == 200
    txs = (await resp.json())['data']
    assert len(txs) == 1
    assert txs[0]['hash'] == 'tx2'

    resp = await cli.get('/v1/blocks/ab/transactions')
    assert resp.status == 200
    txs = (await resp.json())['data']
    assert len(txs) == 1
    assert txs[0]['hash'] == 'tx2'

    resp = await cli.get('/v1/blocks/ax/transactions')
    assert resp.status == 200
    txs = (await resp.json())['data']
    assert len(txs) == 0


async def test_get_block_transactions_by_number(cli, block_factory, transaction_factory):
    # given
    block = block_factory.create()
    from_tx, to_tx = transaction_factory.create_for_block(block)

    # when
    response = await cli.get(f'/v1/blocks/{block.hash}/transactions')
    response_json = await response.json()

    # then
    assert response.status == 200

    txs = response_json['data']
    assert len(txs) == 1
    assert txs[0] == {
        'blockHash': from_tx.block_hash,
        'blockNumber': from_tx.block_number,
        'from': getattr(from_tx, 'from'),
        'gas': from_tx.gas,
        'gasPrice': from_tx.gas_price,
        'hash': from_tx.hash,
        'input': from_tx.input,
        'nonce': from_tx.nonce,
        'status': True,
        'r': from_tx.r,
        's': from_tx.s,
        'to': from_tx.to,
        'transactionIndex': from_tx.transaction_index,
        'v': from_tx.v,
        'value': from_tx.value,
    }


@pytest.mark.usefixtures('uncles')
async def test_get_block_uncles(cli, main_db_data):
    resp = await cli.get('/v1/blocks/' + main_db_data['blocks'][1]['hash'] + '/uncles')
    assert resp.status == 200
    assert (await resp.json())['data'] == [
        {
            'difficulty': "17578564779",
            'blockNumber': 2,
            'extraData': '0x476574682f76312e302e302f6c696e75782f676f312e342e32',
            'gasLimit': "5000",
            'gasUsed': "0",
            'hash': '0x7852fb223883cd9af4cd9d448998c879a1f93a02954952666075df696c61a2cc',
            'logsBloom': '0x0',
            'miner': '0x0193d941b50d91be6567c7ee1c0fe7af498b4137',
            'mixHash': '0x94a09bb3ef9208bf434855efdb1089f80d07334d91930387a1f3150494e806cb',
            'nonce': '0x32de6ee381be0179',
            'number': 61,
            'parentHash': '0x3cd0324c7ba14ba7cf6e4b664dea0360681458d76bd25dfc0d2207ce4e9abed4',
            'receiptsRoot': '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421',
            'sha3Uncles': '0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347',
            'stateRoot': '0x1f4f1cf07f087191901752fe3da8ca195946366db6565f17afec5c04b3d75fd8',
            'timestamp': 1438270332,
            'reward': hex(3750000000000000000),
            'transactionsRoot': '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421'
        }
    ]


async def test_get_transaction(cli, main_db_data):
    tx = main_db_data['transactions'][0]
    resp = await cli.get('/v1/transactions/' + tx['hash'])
    assert resp.status == 200
    assert (await resp.json())['data'] == {
        'blockHash': tx['block_hash'],
        'blockNumber': tx['block_number'],
        'from': tx['from'],
        'gas': tx['gas'],
        'gasPrice': tx['gas_price'],
        'hash': tx['hash'],
        'input': tx['input'],
        'nonce': tx['nonce'],
        'r': tx['r'],
        's': tx['s'],
        'to': tx['to'],
        'status': False,
        'transactionIndex': tx['transaction_index'],
        'v': tx['v'],
        'value': tx['value'],
    }


async def test_get_receipt(cli, main_db_data):
    r = main_db_data['receipts'][0]
    resp = await cli.get('/v1/receipts/' + r['transaction_hash'])
    assert resp.status == 200
    res = (await resp.json())['data']
    assert res == {
        'blockHash': r['block_hash'],
        'blockNumber': r['block_number'],
        'contractAddress': r['contract_address'],
        'cumulativeGasUsed': str(r['cumulative_gas_used']),
        'from': r['from'],
        'gasUsed': str(r['gas_used']),
        'logs': [
            {'address': main_db_data['logs'][0]['address'],
             'blockHash': main_db_data['logs'][0]['block_hash'],
             'blockNumber': main_db_data['logs'][0]['block_number'],
             'data': main_db_data['logs'][0]['data'],
             'logIndex': main_db_data['logs'][0]['log_index'],
             'removed': main_db_data['logs'][0]['removed'],
             'topics': main_db_data['logs'][0]['topics'],
             'transactionHash': main_db_data['logs'][0]['transaction_hash'],
             'transactionIndex': main_db_data['logs'][0]['transaction_index'],
             }],
        'logsBloom': r['logs_bloom'],
        'root': r['root'],
        'status': r['status'],
        'to': r['to'],
        'transactionHash': r['transaction_hash'],
        'transactionIndex': r['transaction_index'],
    }


@pytest.mark.parametrize(
    "block_hash, txs",
    [
        (
                '0x4c99d417111714a2118b9f3e336c097c4acbdc45289ba7b3a02d078d00658a22',
                []
        ),
        (
                '0x691d775caa1979538e2c3e68678d149567e3398480a6c4389585b16a312635f5',
                [
                    '0x8b6450741b7d1d5d5b37354e6b966dfff807346cdc575c7f0a10eeb3cd7717ba',
                    '0x125fa8f17c970ff63db176860b94ea3d004ec42c1c446b99553e8bbfc2d2a892'
                ]
        )
    ],
    ids=[
        "txs=not_transactions",
        "txs=2_transactions"
    ]
)
async def test_get_blocks_check_txs_hashes_list(cli, main_db_data, block_hash, txs):
    resp = await cli.get('/v1/blocks')
    assert resp.status == 200

    data = await resp.json()
    blocks = {block['hash']: block for block in data['data']}

    assert blocks[block_hash]
    assert isinstance(blocks[block_hash]['transactions'], list)
    assert sorted(blocks[block_hash]['transactions']) == sorted(txs)


@pytest.mark.parametrize(
    "block_hash,uncles_hashes",
    [
        (
                '0x4c99d417111714a2118b9f3e336c097c4acbdc45289ba7b3a02d078d00658a22',
                []
        ),
        (
                '0x691d775caa1979538e2c3e68678d149567e3398480a6c4389585b16a312635f5',
                [
                    '0x7852fb223883cd9af4cd9d448998c879a1f93a02954952666075df696c61a2cc',
                ]
        ),
    ],
    ids=[
        "uncles=no_uncles",
        "uncles=1_uncles"
    ]
)
@pytest.mark.usefixtures('uncles')
async def test_get_blocks_check_uncles_hashes_list(cli, main_db_data, block_hash, uncles_hashes):
    resp = await cli.get('/v1/blocks')
    assert resp.status == 200

    data = await resp.json()
    blocks = {block['hash']: block for block in data['data']}

    assert blocks[block_hash]
    assert isinstance(blocks[block_hash]['uncles'], list)
    assert sorted(blocks[block_hash]['uncles']) == sorted(uncles_hashes)


async def test_get_blocks_def(cli, main_db_data):
    b = main_db_data['blocks']
    resp = await cli.get('/v1/blocks')
    assert resp.status == 200
    res = (await resp.json())['data']
    assert res[0]['hash'] == b[-1]['hash']
    assert res[1]['hash'] == b[-2]['hash']


async def test_get_blocks_ask(cli, main_db_data):
    resp = await cli.get('/v1/blocks?order=asc')
    b = main_db_data['blocks']
    assert resp.status == 200
    res = (await resp.json())['data']
    assert res[0]['hash'] == b[0]['hash']
    assert res[1]['hash'] == b[1]['hash']


@pytest.mark.usefixtures('main_db_data')
async def test_get_blocks_limit_offset(cli):
    resp = await cli.get('/v1/blocks?limit=1')
    assert resp.status == 200
    result = (await resp.json())['data']
    assert len(result) == 1
    assert result[0]['number'] == 5

    resp = await cli.get('/v1/blocks?limit=1&offset=1')
    assert resp.status == 200
    result = (await resp.json())['data']
    assert len(result) == 1
    assert result[0]['number'] == 4


@pytest.mark.usefixtures('uncles')
async def test_get_uncles(cli, main_db_data):
    resp = await cli.get('/v1/uncles')
    assert resp.status == 200
    assert (await resp.json())['data'] == [
        {'blockNumber': main_db_data['blocks'][2]['number'],
         'difficulty': "18180751616",
         'extraData': '0x476574682f76312e302e302d30636463373634372f6c696e75782f676f312e34',
         'gasLimit': "5000",
         'gasUsed': "0",
         'hash': '0x6a5a801b12b94e1fb24e531b087719d699882a4f948564ba58706934bc5a19ff',
         'logsBloom': '0x0',
         'miner': '0x70137010922f2fc2964b3792907f79fbb75febe8',
         'mixHash': '0x48b762afc38197f6962c31851fd54ebbdff137bae3c64fff414eaa14ec243dbf',
         'nonce': '0x5283f7dfcd4a29ec',
         'number': 62,
         'parentHash': '0x5656b852baa80ce4db00c60998f5cf6e7a8d76f0339d3cf97955d933f731fecf',
         'receiptsRoot': '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421',
         'sha3Uncles': '0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347',
         'stateRoot': '0x901a42ee6ef09d68712df93609a8adbce98b314118d69a3dd07497615aa7b37b',
         'timestamp': 1438270505,
         'reward': hex(3750000000000000000),
         'transactionsRoot': '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421'},
        {'blockNumber': main_db_data['blocks'][1]['number'],
         'difficulty': "17578564779",
         'extraData': '0x476574682f76312e302e302f6c696e75782f676f312e342e32',
         'gasLimit': "5000",
         'gasUsed': "0",
         'hash': '0x7852fb223883cd9af4cd9d448998c879a1f93a02954952666075df696c61a2cc',
         'logsBloom': '0x0',
         'miner': '0x0193d941b50d91be6567c7ee1c0fe7af498b4137',
         'mixHash': '0x94a09bb3ef9208bf434855efdb1089f80d07334d91930387a1f3150494e806cb',
         'nonce': '0x32de6ee381be0179',
         'number': 61,
         'parentHash': '0x3cd0324c7ba14ba7cf6e4b664dea0360681458d76bd25dfc0d2207ce4e9abed4',
         'receiptsRoot': '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421',
         'sha3Uncles': '0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347',
         'stateRoot': '0x1f4f1cf07f087191901752fe3da8ca195946366db6565f17afec5c04b3d75fd8',
         'timestamp': 1438270332,
         'reward': hex(3750000000000000000),
         'transactionsRoot': '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421'}
    ]


@pytest.mark.usefixtures('uncles')
async def test_get_uncles_asc(cli):
    resp = await cli.get('/v1/uncles?order=asc')
    assert resp.status == 200
    uncles = (await resp.json())['data']
    assert uncles[0]['number'] == 61
    assert uncles[1]['number'] == 62


@pytest.mark.usefixtures('uncles')
async def test_get_uncles_offset_limit(cli):
    resp = await cli.get('/v1/uncles?offset=1&limit=1')
    assert resp.status == 200
    uncles = (await resp.json())['data']
    assert len(uncles) == 1
    assert uncles[0]['number'] == 61


@pytest.mark.usefixtures('uncles')
async def test_get_uncle_404(cli):
    resp = await cli.get('/v1/uncles/111')
    await assert_not_404_response(resp)

    resp = await cli.get('/v1/uncles/0x6a')
    await assert_not_404_response(resp)


@pytest.mark.usefixtures('uncles')
async def test_get_uncle_by_hash(cli, main_db_data):
    resp = await cli.get('/v1/uncles/0x6a5a801b12b94e1fb24e531b087719d699882a4f948564ba58706934bc5a19ff')
    assert resp.status == 200
    assert (await resp.json())['data'] == {
        'blockNumber': main_db_data['blocks'][2]['number'],
        'difficulty': "18180751616",
        'extraData': '0x476574682f76312e302e302d30636463373634372f6c696e75782f676f312e34',
        'gasLimit': "5000",
        'gasUsed': "0",
        'hash': '0x6a5a801b12b94e1fb24e531b087719d699882a4f948564ba58706934bc5a19ff',
        'logsBloom': '0x0',
        'miner': '0x70137010922f2fc2964b3792907f79fbb75febe8',
        'mixHash': '0x48b762afc38197f6962c31851fd54ebbdff137bae3c64fff414eaa14ec243dbf',
        'nonce': '0x5283f7dfcd4a29ec',
        'number': 62,
        'parentHash': '0x5656b852baa80ce4db00c60998f5cf6e7a8d76f0339d3cf97955d933f731fecf',
        'receiptsRoot': '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421',
        'sha3Uncles': '0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347',
        'stateRoot': '0x901a42ee6ef09d68712df93609a8adbce98b314118d69a3dd07497615aa7b37b',
        'timestamp': 1438270505,
        'reward': hex(3750000000000000000),
        'transactionsRoot': '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421'
    }


@pytest.mark.usefixtures('uncles')
async def test_get_uncle_by_number(cli, main_db_data):
    resp = await cli.get('/v1/uncles/62')
    assert resp.status == 200
    assert (await resp.json())['data'] == {
        'blockNumber': main_db_data['blocks'][2]['number'],
        'difficulty': "18180751616",
        'extraData': '0x476574682f76312e302e302d30636463373634372f6c696e75782f676f312e34',
        'gasLimit': "5000",
        'gasUsed': "0",
        'hash': '0x6a5a801b12b94e1fb24e531b087719d699882a4f948564ba58706934bc5a19ff',
        'logsBloom': '0x0',
        'miner': '0x70137010922f2fc2964b3792907f79fbb75febe8',
        'mixHash': '0x48b762afc38197f6962c31851fd54ebbdff137bae3c64fff414eaa14ec243dbf',
        'nonce': '0x5283f7dfcd4a29ec',
        'number': 62,
        'parentHash': '0x5656b852baa80ce4db00c60998f5cf6e7a8d76f0339d3cf97955d933f731fecf',
        'receiptsRoot': '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421',
        'sha3Uncles': '0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347',
        'stateRoot': '0x901a42ee6ef09d68712df93609a8adbce98b314118d69a3dd07497615aa7b37b',
        'timestamp': 1438270505,
        'reward': hex(3750000000000000000),
        'transactionsRoot': '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421'
    }


class AsyncContextManagerMock(mock.Mock):
    async def __aenter__(self):
        return self.aenter

    async def __aexit__(self, *args):
        pass


async def test_verify_contract_ok(db, cli, main_db_data, here, fuck_token):
    contract_data = {
        'address': main_db_data['accounts_state'][2]['address'],
        'contract_name': 'FucksToken',
        'compiler': 'v0.4.18+commit.9cf6e910',
        'optimization_enabled': True,
        'constructor_args': None,
        'source_code': fuck_token.sources
    }

    with mock.patch('jsearch.api.handlers.contracts.aiohttp.request', new=AsyncContextManagerMock()) as m:
        m.return_value.aenter.json = CoroutineMock(side_effect=[
            {
                'bin': fuck_token.bin,
                'abi': fuck_token.abi_as_dict(),
            },
            {}
        ])

        resp = await cli.post('/v1/verify_contract', json=contract_data)
    assert resp.status == 200
    assert (await resp.json())['data'] == {'verification_passed': True}

    # assert m.has_call()
    m.assert_called_with(
        'POST',
        mock.ANY,
        json={
            'abi': fuck_token.abi_as_dict(),
            'address': contract_data['address'],
            'compiler': 'v0.4.18+commit.9cf6e910',
            'constructor_args': '',
            'contract_creation_code': mock.ANY,
            'contract_name': 'FucksToken',
            'is_erc20_token': True,
            'mhash': '4c3e25afac0b2393e51b49944bdfca9d02ac0c064fb7dccd895eaf7c59f55155',
            'optimization_enabled': True,
            'source_code': contract_data['source_code']
        }
    )


_token_1_transfers = [{'from': 'a1',
                       'timestamp': 1529159847,
                       'to': 'a3',
                       'tokenAddress': 'c1',
                       'tokenDecimals': 2,
                       'tokenName': 'A Token',
                       'tokenSymbol': 'TKN',
                       'amount': '300',
                       'transactionHash': 't1'},
                      {'from': 'a2',
                       'timestamp': 1529159847,
                       'to': 'a1',
                       'tokenAddress': 'c1',
                       'tokenDecimals': 2,
                       'tokenName': 'A Token',
                       'tokenSymbol': 'TKN',
                       'amount': '100',
                       'transactionHash': 't1'}]


async def test_get_token_transfers(cli, main_db_data):
    resp = await cli.get(f'/v1/tokens/c1/transfers')
    assert resp.status == 200
    assert (await resp.json())['data'] == _token_1_transfers[:]


async def test_get_token_transfers_asc(cli, main_db_data):
    resp = await cli.get(f'/v1/tokens/c1/transfers?order=asc')
    assert resp.status == 200
    assert (await resp.json())['data'] == _token_1_transfers[::-1]


async def test_get_token_transfers_limit(cli, main_db_data):
    resp = await cli.get(f'/v1/tokens/c1/transfers?limit=1')
    assert resp.status == 200
    assert (await resp.json())['data'] == _token_1_transfers[:1]


async def test_get_token_transfers_offset(cli, main_db_data):
    resp = await cli.get(f'/v1/tokens/c1/transfers?offset=1')
    assert resp.status == 200
    assert (await resp.json())['data'] == _token_1_transfers[1:]


_account_1_transfers = [{'from': 'a3',
                         'timestamp': 1529159847,
                         'to': 'a1',
                         'tokenAddress': 'c2',
                         'tokenDecimals': 2,
                         'tokenName': 'A Token 2',
                         'tokenSymbol': 'TKN2',
                         'amount': '500',
                         'transactionHash': 't2'},
                        {'from': 'a1',
                         'timestamp': 1529159847,
                         'to': 'a3',
                         'tokenAddress': 'c1',
                         'tokenDecimals': 2,
                         'tokenName': 'A Token',
                         'tokenSymbol': 'TKN',
                         'amount': '300',
                         'transactionHash': 't1'},
                        {'from': 'a2',
                         'timestamp': 1529159847,
                         'to': 'a1',
                         'tokenAddress': 'c2',
                         'tokenDecimals': 2,
                         'tokenName': 'A Token 2',
                         'tokenSymbol': 'TKN2',
                         'amount': '200',
                         'transactionHash': 't1'},
                        {'from': 'a2',
                         'timestamp': 1529159847,
                         'to': 'a1',
                         'tokenAddress': 'c1',
                         'tokenDecimals': 2,
                         'tokenName': 'A Token',
                         'tokenSymbol': 'TKN',
                         'amount': '100',
                         'transactionHash': 't1'}]


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
                                            'tokenAddress': 'c2',
                                            'tokenDecimals': 2,
                                            'tokenName': 'A Token 2',
                                            'tokenSymbol': 'TKN2',
                                            'amount': '200',
                                            'transactionHash': 't1'},
                                           {'from': 'a2',
                                            'timestamp': 1529159847,
                                            'to': 'a1',
                                            'tokenAddress': 'c1',
                                            'tokenDecimals': 2,
                                            'tokenName': 'A Token',
                                            'tokenSymbol': 'TKN',
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
    assert res == [{'accountAddress': 'a3', 'decimals': 2, 'balance': 3000, 'tokenAddress': 't1'},
                   {'accountAddress': 'a2', 'decimals': 2, 'balance': 2000, 'tokenAddress': 't1'},
                   {'accountAddress': 'a1', 'decimals': 2, 'balance': 1000, 'tokenAddress': 't1'}]

    resp = await cli.get(f'/v1/tokens/t1/holders?order=asc')
    assert resp.status == 200
    res = (await resp.json())['data']
    assert res == [{'accountAddress': 'a1', 'decimals': 2, 'balance': 1000, 'tokenAddress': 't1'},
                   {'accountAddress': 'a2', 'decimals': 2, 'balance': 2000, 'tokenAddress': 't1'},
                   {'accountAddress': 'a3', 'decimals': 2, 'balance': 3000, 'tokenAddress': 't1'}]

    resp = await cli.get(f'/v1/tokens/t3/holders?order=asc&limit=2&offset=1')
    assert resp.status == 200
    res = (await resp.json())['data']
    assert res == [{'accountAddress': 'a3', 'decimals': 2, 'balance': 5000, 'tokenAddress': 't3'},
                   {'accountAddress': 'a4', 'decimals': 2, 'balance': 6000, 'tokenAddress': 't3'}]


async def test_get_account_token_balance(cli, main_db_data):
    resp = await cli.get(f'/v1/accounts/a1/token_balance/t1')
    assert resp.status == 200
    res = (await resp.json())['data']
    assert res == {'accountAddress': 'a1', 'decimals': 2, 'balance': 1000, 'tokenAddress': 't1'}

    resp = await cli.get(f'/v1/accounts/a3/token_balance/t3')
    assert resp.status == 200
    res = (await resp.json())['data']
    assert res == {'accountAddress': 'a3', 'decimals': 2, 'balance': 5000, 'tokenAddress': 't3'}

    resp = await cli.get(f'/v1/accounts/a3/token_balance/tX')
    await assert_not_404_response(resp)

    resp = await cli.get(f'/v1/accounts/aX/token_balance/t1')
    await assert_not_404_response(resp)


async def test_get_blockchain_tip(cli, block_factory):
    block_factory.create()
    last_block = block_factory.create()

    response = await cli.get(f'/v1/wallet/blockchain_tip?tip=aa')
    response_json = await response.json()

    assert response_json == {
        'status': {
            'success': True,
            'errors': []
        },
        'data': {
            'blockHash': last_block.hash,
            'blockNumber': last_block.number
        }
    }


async def test_get_wallet_transfers_no_addresses(cli, db):
    resp = await cli.get(f'/v1/wallet/transfers')
    assert resp.status == 200
    res = (await resp.json())['data']
    assert res == []


async def test_get_wallet_transfers(cli, db):
    transfers = [
        {'address': 'a1',
         'type': 'erc20-transfer',
         'from': 'a1',
         'to': 'a2',
         'asset_address': 'ca1',
         'value': 1000,
         'decimals': 1,
         'tx_data': {'hash': 'f1'},
         'is_forked': False,
         'block_number': 100,
         'block_hash': 'b1',
         'ordering': 2},
        {'address': 'a1',
         'type': 'eth-transfer',
         'from': 'a3',
         'to': 'a1',
         'asset_address': '',
         'value': 200,
         'decimals': 0,
         'tx_data': {'hash': 'f2'},
         'is_forked': False,
         'block_number': 101,
         'block_hash': 'b1',
         'ordering': 1},
        {'address': 'a2',
         'type': 'erc20-transfer',
         'from': 'a1',
         'to': 'a2',
         'asset_address': 'ca1',
         'value': 1000,
         'decimals': 1,
         'tx_data': {'hash': 'f1'},
         'is_forked': False,
         'block_number': 100,
         'block_hash': 'b1',
         'ordering': 1},
        {'address': 'a2',
         'type': 'eth-transfer',
         'from': 'a3',
         'to': 'a1',
         'asset_address': '',
         'value': 100,
         'decimals': 0,
         'tx_data': {'hash': 'f2'},
         'is_forked': True,
         'block_number': 100,
         'block_hash': 'b1',
         'ordering': 1},

    ]
    for t in transfers:
        db.execute(assets_transfers_t.insert().values(**t))

    resp = await cli.get(f'/v1/wallet/transfers?addresses=a1,a2')
    assert resp.status == 200
    res = (await resp.json())['data']
    assert res == [{'amount': '100',
                    'assetAddress': 'ca1',
                    'from': 'a1',
                    'to': 'a2',
                    'txData': {"hash": "f1"},
                    'type': 'erc20-transfer'},
                   {'amount': '200',
                    'assetAddress': '',
                    'from': 'a3',
                    'to': 'a1',
                    'txData': {"hash": "f2"},
                    'type': 'eth-transfer'},
                   {'amount': '100',
                    'assetAddress': 'ca1',
                    'from': 'a1',
                    'to': 'a2',
                    'txData': {"hash": "f1"},
                    'type': 'erc20-transfer'}]

    resp = await cli.get(f'/v1/wallet/transfers?addresses=a1&assets=ca1')
    assert resp.status == 200
    res = (await resp.json())['data']
    assert res == [{'amount': '100',
                    'assetAddress': 'ca1',
                    'from': 'a1',
                    'to': 'a2',
                    'txData': {"hash": "f1"},
                    'type': 'erc20-transfer'}
                   ]


async def test_get_wallet_transactions(cli, db):
    txs = [
        {
            'address': 'a1',
            'hash': '0xt1',
            'block_number': 1,
            'block_hash': '0xb1',
            'transaction_index': 0,
            'from': '0xa1',
            'to': '0xa2',
            'gas': '0xf4240',
            'gas_price': '0x430e23400',
            'input': '0x6060',
            'nonce': '0x0',
            'value': '0x0',
            'is_forked': False,
            'contract_call_description': None
        },
        {
            'address': 'a2',
            'hash': '0xt1',
            'block_number': 1,
            'block_hash': '0xb1',
            'transaction_index': 0,
            'from': '0xa1',
            'to': '0xa2',
            'gas': '0xf4240',
            'gas_price': '0x430e23400',
            'input': '0x6060',
            'nonce': '0x0',
            'value': '0x0',
            'is_forked': False,
            'contract_call_description': None
        },
        {
            'address': 'a1',
            'hash': '0xt2',
            'block_number': 1,
            'block_hash': '0xb1',
            'transaction_index': 0,
            'from': 'a1',
            'to': 'a2',
            'gas': '0xf4240',
            'gas_price': '0x430e23400',
            'input': '0x6060',
            'nonce': '0x0',
            'value': '0x0',
            'is_forked': False,
            'contract_call_description': None
        },
        {
            'address': 'a3',
            'hash': '0xt2',
            'block_number': 1,
            'block_hash': '0xb1',
            'transaction_index': 0,
            'from': 'a1',
            'to': 'a3',
            'gas': '0xf4240',
            'gas_price': '0x430e23400',
            'input': '0x6060',
            'nonce': '0x0',
            'value': '0x0',
            'is_forked': False,
            'contract_call_description': None
        },
    ]
    for t in txs:
        db.execute(transactions_t.insert().values(**t))

    db.execute(accounts_state_t.insert().values(address='a1', nonce=1, block_number=1, block_hash='0xb1'))

    resp = await cli.get(f'/v1/wallet/transactions?address=a1')
    assert resp.status == 200
    res = (await resp.json())['data']
    assert res == {
        'transactions': [
            {
                'blockHash': '0xb1',
                'blockNumber': 1,
                'from': '0xa1',
                'gas': '0xf4240',
                'gasPrice': '0x430e23400',
                'hash': '0xt1',
                'input': '0x6060',
                'nonce': '0x0',
                'r': None,
                's': None,
                'to': '0xa2',
                'transactionIndex': 0,
                'v': None,
                'status': False,
                'value': '0x0'
            },
            {
                'blockHash': '0xb1',
                'blockNumber': 1,
                'from': 'a1',
                'gas': '0xf4240',
                'gasPrice': '0x430e23400',
                'hash': '0xt2',
                'input': '0x6060',
                'nonce': '0x0',
                'r': None,
                's': None,
                'to': 'a2',
                'transactionIndex': 0,
                'v': None,
                'status': False,
                'value': '0x0'
            }
        ],
        'pendingTransactions': [

        ],
        'outgoingTransactionsNumber': 1
    }


async def test_get_wallet_assets_summary(cli, db):
    assets = [
        {
            'address': 'a1',
            'asset_address': 'c1',
            'value': 100,
            'decimals': 0,
            'tx_number': 1,
            'nonce': 10,
        },
        {
            'address': 'a1',
            'asset_address': 'c2',
            'value': 20000,
            'decimals': 2,
            'tx_number': 2,
            'nonce': 10,
        },
        {
            'address': 'a1',
            'asset_address': '',
            'value': 300,
            'decimals': 0,
            'tx_number': 3,
            'nonce': 10,
        },
        {
            'address': 'a2',
            'asset_address': 'c1',
            'value': 1000,
            'decimals': 1,
            'tx_number': 1,
            'nonce': 5,
        },
    ]
    for a in assets:
        db.execute(assets_summary_t.insert().values(**a))
    resp = await cli.get(f'/v1/wallet/assets_summary?addresses=a1,a2')
    assert resp.status == 200
    res = (await resp.json())['data']
    assert res == [{'address': 'a1',
                    'assetsSummary': [{'address': '', 'balance': "300", 'decimals': "0", 'transfersNumber': 3},
                                      {'address': 'c1', 'balance': "100", 'decimals': "0", 'transfersNumber': 1},
                                      {'address': 'c2', 'balance': "20000", 'decimals': "2", 'transfersNumber': 2}],
                    'outgoingTransactionsNumber': "10"},
                   {'address': 'a2',
                    'assetsSummary': [{'address': 'c1', 'balance': "1000", 'decimals': "1", 'transfersNumber': 1}],
                    'outgoingTransactionsNumber': "5"}]

    resp = await cli.get(f'/v1/wallet/assets_summary?addresses=a1&assets=c2')
    assert resp.status == 200
    res = (await resp.json())['data']
    assert res == [{'address': 'a1',
                    'assetsSummary': [{'address': 'c2', 'balance': "20000", "decimals": "2", 'transfersNumber': 2}],
                    'outgoingTransactionsNumber': "10"},
                   ]


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


@pytest.mark.parametrize(
    "direction, expected_order",
    (
            (
                    'asc',
                    [
                        (7400000, 0),
                        (7400000, 1),
                        (7400000, 2),
                        (7400000, 3),
                        (7400000, 4),
                        (7500000, 0),
                        (7500000, 1),
                        (7500000, 2),
                        (7500000, 3),
                        (7500000, 4),
                    ]
            ),
            (
                    'desc',
                    [
                        (7500000, 4),
                        (7500000, 3),
                        (7500000, 2),
                        (7500000, 1),
                        (7500000, 0),
                        (7400000, 4),
                        (7400000, 3),
                        (7400000, 2),
                        (7400000, 1),
                        (7400000, 0),
                    ]
            ),
    ),
    ids=[
        "direction=asc",
        "direction=desc"
    ]
)
async def test_get_account_transactions_ordering(cli, db, direction, expected_order, transaction_factory):
    address = '0x3e20a5fe4eb128156c51e310f0391799beccf0c1'

    # given - unordered transactions
    for block_number in ('7400000', '7500000'):
        for transaction_index in range(5):
            transaction_factory.create(
                **{
                    'transaction_index': str(transaction_index),
                    'block_number': block_number,
                    'address': address
                },
            )

    # when - get transactions with default order
    resp = await cli.get(f'/v1/accounts/{address}/transactions?order={direction}')
    resp_json = await resp.json()
    resp_order_indicators = [(entry['blockNumber'], entry['transactionIndex']) for entry in resp_json['data']]

    # then - check order
    assert resp.status == 200
    assert resp_order_indicators == expected_order


async def test_get_account_internal_transactions(cli, transaction_factory, internal_transaction_factory):
    transaction_factory.create(
        hash='0xae334d3879824f8ece42b16f161caaa77417787f779a05534b122de0aabe3f7e',
        address='0x3e20a5fe4eb128156c51e310f0391799beccf0c1',
        from_='0x3e20a5fe4eb128156c51e310f0391799beccf0c1',
        to='0x70137010922f2fc2964b3792907f79fbb75febe8',
    )

    internal_transaction_data = {
        'block_number': 42,
        'block_hash': '0xa47a6185aa22e64647207caedd0ce8b2b1ae419added75fc3b7843c72b6386bd',
        'parent_tx_hash': '0xae334d3879824f8ece42b16f161caaa77417787f779a05534b122de0aabe3f7e',
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

    internal_transaction_factory.create(
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
    internal_transaction_factory.create(
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

    resp = await cli.get(f'v1/accounts/0x3e20a5fe4eb128156c51e310f0391799beccf0c1/internal_transactions')
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
                'parentTxHash': '0xae334d3879824f8ece42b16f161caaa77417787f779a05534b122de0aabe3f7e',
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
                'blockNumber': 42,
                'blockHash': '0xa47a6185aa22e64647207caedd0ce8b2b1ae419added75fc3b7843c72b6386bd',
                'parentTxHash': '0xae334d3879824f8ece42b16f161caaa77417787f779a05534b122de0aabe3f7e',
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
    }


async def test_get_internal_transactions(cli, internal_transaction_factory):
    internal_transaction_data = {
        'block_number': 42,
        'block_hash': '0xa47a6185aa22e64647207caedd0ce8b2b1ae419added75fc3b7843c72b6386bd',
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
            **{'transaction_index': 42},
        }
    )
    internal_transaction_factory.create(
        **{
            **internal_transaction_data,
            **{'transaction_index': 43},
        }
    )

    resp = await cli.get(f'v1/transactions/internal/0xae334d3879824f8ece42b16f161caaa77417787f779a05534b122de0aabe3f7e')
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
                'parentTxHash': '0xae334d3879824f8ece42b16f161caaa77417787f779a05534b122de0aabe3f7e',
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
                'parentTxHash': '0xae334d3879824f8ece42b16f161caaa77417787f779a05534b122de0aabe3f7e',
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


async def test_get_wallet_events(cli, block_factory, wallet_events_factory, transaction_factory):
    # given
    block = block_factory.create()
    tx, _ = transaction_factory.create_for_block(block=block)
    event = wallet_events_factory.create_token_transfer(tx=tx, block=block)

    params = urlencode({
        'blockchain_address': event.address,
        'blockchain_tip': block.hash,
        'block_range_start': block.number,
    })
    url = f'v1/wallet/get_events?{params}'

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
        'data': {
            'blockchainTip': {
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
            'pending_events': [],
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


class PaginationCase:
    # block pagination params
    start: Union[str, int]
    until: Optional[Union[str, int]]
    count: Optional[int]

    # events pagination
    limit: Optional[int]
    offset: Optional[int]

    # expectation
    txs_count: int
    events_count: int
    blocks: Set[int]

    # mis
    ordering: Optional[str]
    id: str

    def __init__(self,
                 start=None,
                 until=None,
                 count=None,
                 limit=None,
                 offset=None,
                 txs_count=None,
                 events_count=None,
                 blocks=None,
                 ordering=None):
        self.start = start
        self.until = until
        self.count = count
        self.limit = limit
        self.offset = offset
        self.txs_count = txs_count
        self.events_count = events_count
        self.blocks = blocks
        self.ordering = ordering

    def to_dict(self):
        data = {
            'block_range_start': self.start,
            'block_range_end': self.until,
            'block_range_count': self.count,
            'order': self.ordering,
            'limit': self.limit,
            'offset': self.offset,
        }
        return {key: value for key, value in data.items() if value is not None}

    @property
    def name(self):
        keys = ('start', 'until', 'count', 'limit', 'offset', 'ordering')
        params = [(key, getattr(self, key, None)) for key in keys]
        return ",".join([f"{key}={value}" for key, value in params if value is not None])


# 5 blocks
# 3 transaction per block
# 2 events per transaction or 6 events per block
# Tip is previous before latest (blocks[-2])
parameters = [
    PaginationCase('latest', until=0, count=1, txs_count=3, events_count=6, blocks={4}, ordering='desc'),

    PaginationCase(0, until='latest', count=1, txs_count=3, events_count=6, blocks={0}),
    PaginationCase(0, until='latest', count=2, limit=1, txs_count=1, events_count=1, blocks={0}),
    PaginationCase(0, until='latest', count=1, txs_count=3, events_count=6, blocks={4}, ordering='desc'),
    PaginationCase(0, until='latest', count=2, offset=11, txs_count=1, events_count=1, blocks={3}, ordering='desc'),

    PaginationCase(0, until='tip', count=1, txs_count=3, events_count=6, blocks={3}, ordering='desc'),
    PaginationCase(0, until='tip', count=1, txs_count=3, events_count=6, blocks={0}),

    PaginationCase(0, count=1, txs_count=3, events_count=6, blocks={0}),
    PaginationCase(0, until='tip', count=1, txs_count=3, events_count=6, blocks={0}),
    PaginationCase(0, until='latest', count=1, txs_count=3, events_count=6, blocks={0}),

    PaginationCase(0, until='tip', txs_count=3 * 4, events_count=6 * 4, blocks={0, 1, 2, 3}),
    PaginationCase(0, until='latest', txs_count=3 * 5, events_count=6 * 5, blocks={0, 1, 2, 3, 4}),

    PaginationCase(0, count=1, limit=1, txs_count=1, events_count=1, blocks={0}),
    PaginationCase(0, count=1, offset=5, txs_count=1, events_count=1, blocks={0}),
    PaginationCase(0, count=1, txs_count=3, events_count=6, blocks={0}),

    PaginationCase(0, count=2, limit=1, txs_count=1, events_count=1, blocks={0}, ordering='desc'),
    PaginationCase(0, count=2, offset=5, txs_count=1, events_count=1, blocks={0}, ordering='desc'),

    PaginationCase(0, count=2, limit=1, txs_count=1, events_count=1, blocks={0}),
    PaginationCase(0, count=2, offset=11, txs_count=1, events_count=1, blocks={1}, ordering='asc'),

    PaginationCase('latest', count=1, txs_count=3, events_count=6, blocks={4}),
    PaginationCase('latest', count=2, txs_count=3, events_count=6, blocks={4}),
    PaginationCase('latest', count=2, txs_count=6, events_count=12, blocks={3, 4}, ordering='desc'),
    PaginationCase('latest', count=1, limit=1, events_count=1, txs_count=1, blocks={4}, ordering='desc'),
    PaginationCase('latest', count=2, offset=11, txs_count=1, events_count=1, blocks={3}, ordering='desc'),

    PaginationCase('tip', count=1, txs_count=3, events_count=6, blocks={3}),
    PaginationCase('tip', count=2, txs_count=6, events_count=12, blocks={3, 4}),
    PaginationCase('tip', until='latest', txs_count=6, events_count=12, blocks={3, 4}),
]


@pytest.mark.parametrize('case', parameters, ids=[p.name for p in parameters])
async def test_get_wallet_events_pagination(
        cli,
        account_factory,
        block_factory,
        wallet_events_factory,
        transaction_factory,
        case
):
    # given
    account = account_factory.create()

    # create 2 events per transaction
    # create 3 txs per blocks
    blocks = []
    events = []
    txs = []
    for _ in range(0, 5):
        block = block_factory.create()
        blocks.append(block)
        for _ in range(0, 3):
            tx = transaction_factory.create(block_hash=block.hash, block_number=block.number)
            txs.append(tx)
            for event_index in range(0, 2):
                event = wallet_events_factory.create_token_transfer(
                    tx=tx,
                    block=block,
                    address=account.address,
                    event_index=event_index,
                )
                events.append(event)

    tip_block = blocks[-2]

    params = urlencode({
        'blockchain_address': account.address,
        'blockchain_tip': tip_block.hash,
        **case.to_dict()
    })
    url = f'v1/wallet/get_events?{params}'

    # when
    response = await cli.get(url)
    response_json = await response.json()

    # then
    assert response.status == 200

    txs = response_json['data']['events']
    events = sum([tx['events'] for tx in txs], [])
    blocks = {tx['rootTxData']['blockNumber'] for tx in txs}

    assert len(txs) == case.txs_count
    assert len(events) == case.events_count
    assert blocks == case.blocks


async def test_get_wallet_events_tip_in_fork(cli,
                                             block_factory,
                                             wallet_events_factory,
                                             reorg_factory,
                                             transaction_factory,
                                             chain_split_factory):
    # given
    block = block_factory.create()
    forked_block = block_factory.create(number=1)

    chain_splits = chain_split_factory.create()
    reorg = reorg_factory.create(
        block_hash=forked_block.hash,
        block_number=forked_block.number,
        split_id=chain_splits.id
    )

    tx, _ = transaction_factory.create_for_block(block=block)
    wallet_events_factory.create_token_transfer(tx=tx)

    tx_in_fork, _ = transaction_factory.create_for_block(block=forked_block)
    event = wallet_events_factory.create_token_transfer(tx=tx)

    params = urlencode({
        'blockchain_address': event.address,
        'blockchain_tip': reorg.block_hash,
        'block_range_start': forked_block.number,
    })
    url = f'v1/wallet/get_events?{params}'

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
        'data': {
            'blockchainTip': {
                'blockchainTipStatus': {
                    'blockHash': forked_block.hash,
                    'blockNumber': forked_block.number,
                    'isOrphaned': True,
                    'lastUnchangedBlock': 0
                },
                'currentBlockchainTip': {
                    'blockHash': forked_block.hash,
                    'blockNumber': forked_block.number
                }
            },
            'events': [],
            'pending_events': []
        }
    }


async def test_get_wallet_events_tip(cli,
                                     block_factory,
                                     chain_split_factory,
                                     reorg_factory,
                                     transaction_factory,
                                     wallet_events_factory):
    block = block_factory.create(hash='aa', number=100)
    block_factory.create(hash='ab', number=101)
    block_factory.create(hash='abf', number=101, is_forked=True)
    block_factory.create(hash='ac', number=102)
    block_factory.create(hash='acf', number=102, is_forked=True)

    chain_split_factory.create(id=1, common_block_number=100)

    reorg_factory.create(id=1, split_id=1, block_hash='abf', block_number=101, reinserted=False)
    reorg_factory.create(id=2, split_id=1, block_hash='acf', block_number=102, reinserted=False)

    tx, _ = transaction_factory.create_for_block(block=block)
    event = wallet_events_factory.create_token_transfer(tx=tx, block=block)

    def get_url(tip):
        return f'v1/wallet/get_events?' \
               f'blockchain_address={event.address}&' \
               f'blockchain_tip={tip}&' \
               f'block_range_start={0}'

    response = await cli.get(get_url(tip='aa'))
    response_json = await response.json()
    assert response.status == 200
    assert response_json['data']['blockchainTip'] == {
        'blockchainTipStatus': {
            'blockHash': 'aa',
            'blockNumber': 100,
            'isOrphaned': False,
            'lastUnchangedBlock': None
        },
        'currentBlockchainTip': {
            'blockHash': 'ac',
            'blockNumber': 102
        }
    }

    response = await cli.get(get_url(tip='ac'))
    response_json = await response.json()
    assert response.status == 200
    assert response_json['data']['blockchainTip'] == {
        'blockchainTipStatus': {
            'blockHash': 'ac',
            'blockNumber': 102,
            'isOrphaned': False,
            'lastUnchangedBlock': None
        },
        'currentBlockchainTip': {
            'blockHash': 'ac',
            'blockNumber': 102
        }
    }

    response = await cli.get(get_url('abf'))
    response_json = await response.json()
    assert response.status == 200
    assert response_json['data']['blockchainTip'] == {
        'blockchainTipStatus': {
            'blockHash': 'abf',
            'blockNumber': 101,
            'isOrphaned': True,
            'lastUnchangedBlock': 100
        },
        'currentBlockchainTip': {
            'blockHash': 'ac',
            'blockNumber': 102
        }
    }

    response = await cli.get(get_url('acf'))
    response_json = await response.json()
    assert response.status == 200
    assert response_json['data']['blockchainTip'] == {
        'blockchainTipStatus': {
            'blockHash': 'acf',
            'blockNumber': 102,
            'isOrphaned': True,
            'lastUnchangedBlock': 100
        },
        'currentBlockchainTip': {
            'blockHash': 'ac',
            'blockNumber': 102
        }
    }


async def test_get_wallet_events_tip_in_fork_but_events_not_affected(cli,
                                                                     block_factory,
                                                                     wallet_events_factory,
                                                                     transaction_factory,
                                                                     reorg_factory,
                                                                     chain_split_factory):
    # given
    not_affected_block = block_factory.create()
    block = block_factory.create()

    chain_splits = chain_split_factory.create(
        common_block_number=not_affected_block.number,
        common_block_hash=not_affected_block.hash
    )
    reorg = reorg_factory.create(block_hash=block.hash, block_number=block.number, split_id=chain_splits.id)

    tx, _ = transaction_factory.create_for_block(block=not_affected_block)
    event = wallet_events_factory.create_token_transfer(tx=tx, block=not_affected_block)

    params = urlencode({
        'blockchain_address': event.address,
        'blockchain_tip': reorg.block_hash,
        'block_range_start': 0,
        'block_range_count': 1,
    })
    url = f'v1/wallet/get_events?{params}'

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
        'data': {
            'blockchainTip': {
                'blockchainTipStatus': {
                    'blockHash': block.hash,
                    'blockNumber': block.number,
                    'isOrphaned': True,
                    'lastUnchangedBlock': 0
                },
                'currentBlockchainTip': {
                    'blockHash': block.hash,
                    'blockNumber': block.number
                }
            },
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
            'pending_events': [],
        }
    }


async def test_get_wallet_events_tip_does_not_exist(cli,
                                                    block_factory,
                                                    transaction_factory,
                                                    wallet_events_factory):
    # given
    block = block_factory.create()
    tx, _ = transaction_factory.create_for_block(block=block)
    event = wallet_events_factory.create_token_transfer(tx=tx, block=block)

    unsaved_block = block_factory.build()

    url = f'v1/wallet/get_events?' \
          f'blockchain_address={event.address}&' \
          f'blockchain_tip={unsaved_block.hash}&' \
          f'block_range_start={block.number}'

    # when
    response = await cli.get(url)
    response_json = await response.json()

    # then
    assert response.status == 404

    assert response_json == {
        'data': {},
        'status': {
            'errors': [
                {
                    'error_code': ErrorCode.BLOCK_NOT_FOUND,
                    'error_message': f'Block with hash {unsaved_block.hash} not found',
                    'field': 'tip'
                }
            ],
            'success': False
        }
    }


async def test_get_wallet_events_query_param_started_from_is_required(cli,
                                                                      block_factory,
                                                                      wallet_events_factory,
                                                                      reorg_factory,
                                                                      transaction_factory,
                                                                      chain_split_factory):
    # given
    block = block_factory.create()
    chain_splits = chain_split_factory.create()
    reorg = reorg_factory.create(block_hash=block.hash, block_number=block.number, split_id=chain_splits.id)
    tx, _ = transaction_factory.create_for_block(block=block)
    event = wallet_events_factory.create_token_transfer(tx, block=block)

    url = f'v1/wallet/get_events?' \
          f'blockchain_address={event.address}&' \
          f'blockchain_tip={reorg.block_hash}'

    # when
    response = await cli.get(url)
    response_json = await response.json()

    # then
    assert response.status == 400
    assert response_json == {
        'status': {
            'success': False,
            'errors': [
                {
                    'field': 'block_range_start',
                    'error_code': ErrorCode.VALIDATION_ERROR,
                    'error_message': f'Query param `block_range_start` is required'
                },
            ]
        },
        'data': {},
    }


async def test_get_wallet_events_query_param_started_from_is_uknown_tag(cli,
                                                                        block_factory,
                                                                        wallet_events_factory,
                                                                        reorg_factory,
                                                                        transaction_factory,
                                                                        chain_split_factory):
    # given
    block = block_factory.create()
    tx, _ = transaction_factory.create_for_block(block=block)
    event = wallet_events_factory.create_token_transfer(tx, block=block)

    params = urlencode({
        'blockchain_address': event.address,
        'blockchain_tip': block.hash,
        'block_range_start': 'unknown_tag',
    })
    url = f'v1/wallet/get_events?{params}'

    # when
    response = await cli.get(url)
    response_json = await response.json()

    # then
    assert response.status == 400
    assert response_json == {
        'status': {
            'success': False,
            'errors': [
                {
                    'field': 'block_range_start',
                    'error_code': ErrorCode.VALIDATION_ERROR,
                    'error_message': 'Parameter `block_range_start` must be '
                                     'either positive integer or tag (latest, tip).'
                },
            ]
        },
        'data': {},
    }


async def test_get_wallet_events_query_param_address_is_required(cli,
                                                                 block_factory,
                                                                 reorg_factory,
                                                                 chain_split_factory):
    # given
    block = block_factory.create()
    chain_splits = chain_split_factory.create()
    reorg = reorg_factory.create(block_hash=block.hash, block_number=block.number, split_id=chain_splits.id)

    url = f'v1/wallet/get_events?' \
          f'blockchain_tip={reorg.block_hash}' \
          f'block_range_start={block.number}'

    # when
    response = await cli.get(url)
    response_json = await response.json()

    # then
    assert response.status == 400
    assert response_json == {
        'status': {
            'success': False,
            'errors': [
                {
                    'param': 'blockchain_address',
                    'error_code': ErrorCode.VALIDATION_ERROR,
                    'error_message': 'Query param `blockchain_address` is required'
                },
            ]
        },
        'data': {},
    }


async def test_get_wallet_events_query_param_tip_is_required(cli,
                                                             block_factory,
                                                             transaction_factory,
                                                             wallet_events_factory):
    # given
    block = block_factory.create()
    tx, _ = transaction_factory.create_for_block(block=block)
    event = wallet_events_factory.create_token_transfer(tx=tx, block=block)
    url = f'v1/wallet/get_events?' \
          f'blockchain_address={event.address}&' \
          f'block_range_start={block.number}'

    # when
    response = await cli.get(url)
    response_json = await response.json()

    # then
    assert response.status == 400
    assert response_json == {
        'status': {
            'success': False,
            'errors': [
                {
                    'param': 'blockchain_tip',
                    'error_code': ErrorCode.VALIDATION_ERROR,
                    'error_message': f'Query param `blockchain_tip` is required'
                },
            ]
        },
        'data': {},
    }


async def test_get_wallet_events_pending_txs(cli,
                                             block_factory,
                                             pending_transaction_factory):
    # given
    block = block_factory.create()
    pending_tx = pending_transaction_factory.create_eth_transfer()

    url = f'v1/wallet/get_events?' \
          f'blockchain_address={pending_tx.to}&' \
          f'blockchain_tip={block.hash}&' \
          f'block_range_start={block.number}&' \
          f'include_pending_events=1'

    # when
    response = await cli.get(url)
    response_json = await response.json()

    # then

    assert response_json == {
        'status': {
            'success': True,
            'errors': []
        },
        'data': {
            'blockchainTip': {
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
            'events': [],
            'pending_events': [
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
        }
    }


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

    url = f'v1/wallet/get_events?' \
          f'blockchain_address={event.address}&' \
          f'blockchain_tip={block.hash}&' \
          f'block_range_start={block.number}&' \
          f'include_pending_events=1'

    # when
    response = await cli.get(url)
    response_json = await response.json()

    # then

    assert len(response_json['data']['pending_events']) == 100


async def test_get_blocks_transactions_preserves_order(cli,
                                                       db,
                                                       block_factory,
                                                       transaction_factory):
    def _ordered_txs_for_block(number):
        return db.execute(
            select(
                columns=[
                    transactions_t.c.hash,
                    transactions_t.c.block_hash,
                    transactions_t.c.transaction_index,
                ],
                whereclause=transactions_t.c.block_number == number,
            ).order_by(transactions_t.c.block_hash, transactions_t.c.transaction_index).distinct()
        ).fetchall()

    # given
    for block_number in ('7400000', '7500000'):
        block = block_factory.create(number=block_number)

        for transaction_index in range(0, 5):
            transaction_factory.create_for_block(block, transaction_index=transaction_index)

    # when
    response = await cli.get('v1/blocks')
    response_json = await response.json()

    block_one = response_json['data'][0]
    block_two = response_json['data'][1]

    # then
    assert block_one['transactions'] == [t['hash'] for t in _ordered_txs_for_block(7500000)]
    assert block_two['transactions'] == [t['hash'] for t in _ordered_txs_for_block(7400000)]
