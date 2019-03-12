from unittest import mock

import pytest
from asynctest import CoroutineMock

from jsearch.common.tables import (
    blocks_t,
    reorgs_t,
    chain_splits_t,
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
]


async def test_get_block_404(cli):
    resp = await cli.get('/v1/blocks/1')
    assert resp.status == 404


async def test_get_block_by_number(cli, main_db_data):
    # given
    txs = TransactionFromDumpWrapper.from_dump(
        main_db_data,
        filters={"block_number": 2},
        bulk=True
    )
    block = BlockFromDumpWrapper.from_dump(
        dump=main_db_data,
        filters={'number': 2},
        transactions=[tx.entity.hash for tx in txs]
    )
    # then
    resp = await cli.get('/v1/blocks/2')
    assert resp.status == 200
    rdata = await resp.json()
    assert rdata['status'] == {'success': True, 'errors': []}
    assert rdata['data'] == block.as_dict()


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
    assert resp.status == 404


async def test_get_block_by_hash(cli, main_db_data):
    resp = await cli.get('/v1/blocks/' + main_db_data['blocks'][0]['hash'])
    assert resp.status == 200
    b = main_db_data['blocks'][0]
    rdata = await resp.json()
    assert rdata['data'] == {
        'difficulty': b['difficulty'],
        'extraData': b['extra_data'],
        'gasLimit': b['gas_limit'],
        'gasUsed': b['gas_used'],
        'hash': b['hash'],
        'logsBloom': b['logs_bloom'],
        'miner': b['miner'],
        'mixHash': b['mix_hash'],
        'nonce': b['nonce'],
        'number': b['number'],
        'parentHash': b['parent_hash'],
        'receiptsRoot': b['receipts_root'],
        'sha3Uncles': b['sha3_uncles'],
        'size': b['size'],
        'stateRoot': b['state_root'],
        'timestamp': b['timestamp'],
        'totalDifficulty': b['total_difficulty'],
        'transactions': [],
        'transactionsRoot': b['transactions_root'],
        'staticReward': hex(b['static_reward']),
        'txFees': hex(b['tx_fees']),
        'uncleInclusionReward': hex(b['uncle_inclusion_reward']),
        'uncles': None
    }


async def test_get_block_latest(cli, main_db_data):
    resp = await cli.get('/v1/blocks/latest')
    assert resp.status == 200
    b = main_db_data['blocks'][-1]
    assert (await resp.json())['data'] == {
        'difficulty': b['difficulty'],
        'extraData': b['extra_data'],
        'gasLimit': b['gas_limit'],
        'gasUsed': b['gas_used'],
        'hash': b['hash'],
        'logsBloom': b['logs_bloom'],
        'miner': b['miner'],
        'mixHash': b['mix_hash'],
        'nonce': b['nonce'],
        'number': b['number'],
        'parentHash': b['parent_hash'],
        'receiptsRoot': b['receipts_root'],
        'sha3Uncles': b['sha3_uncles'],
        'size': b['size'],
        'stateRoot': b['state_root'],
        'timestamp': b['timestamp'],
        'totalDifficulty': b['total_difficulty'],
        'transactions': [main_db_data['transactions'][-1]['hash']],
        'transactionsRoot': b['transactions_root'],
        'staticReward': hex(b['static_reward']),
        'txFees': hex(b['tx_fees']),
        'uncleInclusionReward': hex(b['uncle_inclusion_reward']),
        'uncles': None
    }


async def test_get_account_404(cli):
    resp = await cli.get('/v1/accounts/x')
    assert resp.status == 404


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
    resp = await cli.get('/v1/accounts/' + main_db_data['accounts_state'][0]['address'] + '/transactions')
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


async def test_get_account_balances_invalid_addresses(cli: object, main_db_data: object) -> object:
    a1 = main_db_data['accounts_base'][0]
    resp = await cli.get('/v1/accounts/balances?addresses={},{},{}'.format('foo', a1['address'], 'bar'))
    assert resp.status == 200
    res = (await resp.json())['data']
    assert res == [{'address': a1['address'],
                    'balance': hex(main_db_data['accounts_state'][10]['balance'])}]


async def test_get_block_transactions(cli, main_db_data):
    resp = await cli.get('/v1/blocks/' + main_db_data['blocks'][1]['hash'] + '/transactions')
    assert resp.status == 200
    res = (await resp.json())['data']
    txs = main_db_data['transactions']
    assert len(res) == 2
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
        'transactionIndex': txs[0]['transaction_index'],
        'v': txs[0]['v'],
        'value': txs[0]['value'],
    }


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


async def test_get_block_transactions_by_number(cli, main_db_data):
    resp = await cli.get('/v1/blocks/{}/transactions'.format(main_db_data['blocks'][1]['number']))
    assert resp.status == 200
    res = (await resp.json())['data']
    txs = main_db_data['transactions']
    assert len(res) == 2
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
        'transactionIndex': txs[0]['transaction_index'],
        'v': txs[0]['v'],
        'value': txs[0]['value'],
    }


@pytest.mark.usefixtures('uncles')
async def test_get_block_uncles(cli, main_db_data):
    resp = await cli.get('/v1/blocks/' + main_db_data['blocks'][1]['hash'] + '/uncles')
    assert resp.status == 200
    assert (await resp.json())['data'] == [
        {
            'difficulty': 17578564779,
            'blockNumber': 2,
            'extraData': '0x476574682f76312e302e302f6c696e75782f676f312e342e32',
            'gasLimit': 5000,
            'gasUsed': 0,
            'hash': '0x7852fb223883cd9af4cd9d448998c879a1f93a02954952666075df696c61a2cc',
            'logsBloom': '0x0',
            'miner': '0x0193d941b50d91be6567c7ee1c0fe7af498b4137',
            'mixHash': '0x94a09bb3ef9208bf434855efdb1089f80d07334d91930387a1f3150494e806cb',
            'nonce': '0x32de6ee381be0179',
            'number': 61,
            'parentHash': '0x3cd0324c7ba14ba7cf6e4b664dea0360681458d76bd25dfc0d2207ce4e9abed4',
            'receiptsRoot': '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421',
            'sha3Uncles': '0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347',
            'size': None,
            'stateRoot': '0x1f4f1cf07f087191901752fe3da8ca195946366db6565f17afec5c04b3d75fd8',
            'timestamp': 1438270332,
            'totalDifficulty': None,
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
        'cumulativeGasUsed': r['cumulative_gas_used'],
        'from': r['from'],
        'gasUsed': r['gas_used'],
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
         'difficulty': 18180751616,
         'extraData': '0x476574682f76312e302e302d30636463373634372f6c696e75782f676f312e34',
         'gasLimit': 5000,
         'gasUsed': 0,
         'hash': '0x6a5a801b12b94e1fb24e531b087719d699882a4f948564ba58706934bc5a19ff',
         'logsBloom': '0x0',
         'miner': '0x70137010922f2fc2964b3792907f79fbb75febe8',
         'mixHash': '0x48b762afc38197f6962c31851fd54ebbdff137bae3c64fff414eaa14ec243dbf',
         'nonce': '0x5283f7dfcd4a29ec',
         'number': 62,
         'parentHash': '0x5656b852baa80ce4db00c60998f5cf6e7a8d76f0339d3cf97955d933f731fecf',
         'receiptsRoot': '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421',
         'sha3Uncles': '0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347',
         'size': None,
         'stateRoot': '0x901a42ee6ef09d68712df93609a8adbce98b314118d69a3dd07497615aa7b37b',
         'timestamp': 1438270505,
         'totalDifficulty': None,
         'reward': hex(3750000000000000000),
         'transactionsRoot': '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421'},
        {'blockNumber': main_db_data['blocks'][1]['number'],
         'difficulty': 17578564779,
         'extraData': '0x476574682f76312e302e302f6c696e75782f676f312e342e32',
         'gasLimit': 5000,
         'gasUsed': 0,
         'hash': '0x7852fb223883cd9af4cd9d448998c879a1f93a02954952666075df696c61a2cc',
         'logsBloom': '0x0',
         'miner': '0x0193d941b50d91be6567c7ee1c0fe7af498b4137',
         'mixHash': '0x94a09bb3ef9208bf434855efdb1089f80d07334d91930387a1f3150494e806cb',
         'nonce': '0x32de6ee381be0179',
         'number': 61,
         'parentHash': '0x3cd0324c7ba14ba7cf6e4b664dea0360681458d76bd25dfc0d2207ce4e9abed4',
         'receiptsRoot': '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421',
         'sha3Uncles': '0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347',
         'size': None,
         'stateRoot': '0x1f4f1cf07f087191901752fe3da8ca195946366db6565f17afec5c04b3d75fd8',
         'timestamp': 1438270332,
         'totalDifficulty': None,
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
    assert resp.status == 404
    resp = await cli.get('/v1/uncles/0x6a')
    assert resp.status == 404


@pytest.mark.usefixtures('uncles')
async def test_get_uncle_by_hash(cli, main_db_data):
    resp = await cli.get('/v1/uncles/0x6a5a801b12b94e1fb24e531b087719d699882a4f948564ba58706934bc5a19ff')
    assert resp.status == 200
    assert (await resp.json())['data'] == {
        'blockNumber': main_db_data['blocks'][2]['number'],
        'difficulty': 18180751616,
        'extraData': '0x476574682f76312e302e302d30636463373634372f6c696e75782f676f312e34',
        'gasLimit': 5000,
        'gasUsed': 0,
        'hash': '0x6a5a801b12b94e1fb24e531b087719d699882a4f948564ba58706934bc5a19ff',
        'logsBloom': '0x0',
        'miner': '0x70137010922f2fc2964b3792907f79fbb75febe8',
        'mixHash': '0x48b762afc38197f6962c31851fd54ebbdff137bae3c64fff414eaa14ec243dbf',
        'nonce': '0x5283f7dfcd4a29ec',
        'number': 62,
        'parentHash': '0x5656b852baa80ce4db00c60998f5cf6e7a8d76f0339d3cf97955d933f731fecf',
        'receiptsRoot': '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421',
        'sha3Uncles': '0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347',
        'size': None,
        'stateRoot': '0x901a42ee6ef09d68712df93609a8adbce98b314118d69a3dd07497615aa7b37b',
        'timestamp': 1438270505,
        'totalDifficulty': None,
        'reward': hex(3750000000000000000),
        'transactionsRoot': '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421'
    }


@pytest.mark.usefixtures('uncles')
async def test_get_uncle_by_number(cli, main_db_data):
    resp = await cli.get('/v1/uncles/62')
    assert resp.status == 200
    assert (await resp.json())['data'] == {
        'blockNumber': main_db_data['blocks'][2]['number'],
        'difficulty': 18180751616,
        'extraData': '0x476574682f76312e302e302d30636463373634372f6c696e75782f676f312e34',
        'gasLimit': 5000,
        'gasUsed': 0,
        'hash': '0x6a5a801b12b94e1fb24e531b087719d699882a4f948564ba58706934bc5a19ff',
        'logsBloom': '0x0',
        'miner': '0x70137010922f2fc2964b3792907f79fbb75febe8',
        'mixHash': '0x48b762afc38197f6962c31851fd54ebbdff137bae3c64fff414eaa14ec243dbf',
        'nonce': '0x5283f7dfcd4a29ec',
        'number': 62,
        'parentHash': '0x5656b852baa80ce4db00c60998f5cf6e7a8d76f0339d3cf97955d933f731fecf',
        'receiptsRoot': '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421',
        'sha3Uncles': '0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347',
        'size': None,
        'stateRoot': '0x901a42ee6ef09d68712df93609a8adbce98b314118d69a3dd07497615aa7b37b',
        'timestamp': 1438270505,
        'totalDifficulty': None,
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

    with mock.patch('jsearch.api.handlers.aiohttp.request', new=AsyncContextManagerMock()) as m:
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
                       'amount': 300,
                       'transactionHash': 't1'},
                      {'from': 'a2',
                       'timestamp': 1529159847,
                       'to': 'a1',
                       'tokenAddress': 'c1',
                       'tokenDecimals': 2,
                       'tokenName': 'A Token',
                       'tokenSymbol': 'TKN',
                       'amount': 100,
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
                         'amount': 500,
                         'transactionHash': 't2'},
                        {'from': 'a1',
                         'timestamp': 1529159847,
                         'to': 'a3',
                         'tokenAddress': 'c1',
                         'tokenDecimals': 2,
                         'tokenName': 'A Token',
                         'tokenSymbol': 'TKN',
                         'amount': 300,
                         'transactionHash': 't1'},
                        {'from': 'a2',
                         'timestamp': 1529159847,
                         'to': 'a1',
                         'tokenAddress': 'c2',
                         'tokenDecimals': 2,
                         'tokenName': 'A Token 2',
                         'tokenSymbol': 'TKN2',
                         'amount': 200,
                         'transactionHash': 't1'},
                        {'from': 'a2',
                         'timestamp': 1529159847,
                         'to': 'a1',
                         'tokenAddress': 'c1',
                         'tokenDecimals': 2,
                         'tokenName': 'A Token',
                         'tokenSymbol': 'TKN',
                         'amount': 100,
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
                                            'amount': 200,
                                            'transactionHash': 't1'},
                                           {'from': 'a2',
                                            'timestamp': 1529159847,
                                            'to': 'a1',
                                            'tokenAddress': 'c1',
                                            'tokenDecimals': 2,
                                            'tokenName': 'A Token',
                                            'tokenSymbol': 'TKN',
                                            'amount': 100,
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


async def test_on_new_contracts_added(cli, mocker):
    m = mocker.patch('jsearch.api.handlers.tasks.on_new_contracts_added_task')
    resp = await cli.post('/_on_new_contracts_added', json={'address': 'abc', 'abi': 'ABI'})
    assert resp.status == 200
    m.delay.assert_called_with('abc')


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
    assert resp.status == 404

    resp = await cli.get(f'/v1/accounts/aX/token_balance/t1')
    assert resp.status == 404


async def test_get_blockchain_tip(cli, db):
    db.execute(blocks_t.insert().values(hash='aa', number=100))
    db.execute(blocks_t.insert().values(hash='ab', number=101))
    db.execute(blocks_t.insert().values(hash='abf', number=101))
    db.execute(blocks_t.insert().values(hash='ac', number=102))
    db.execute(blocks_t.insert().values(hash='acf', number=102))

    db.execute(chain_splits_t.insert().values(id=1, common_block_number=100))

    db.execute(
        reorgs_t.insert().values(id=1, split_id=1, block_hash='abf', block_number=101, reinserted=False, node_id='a'))
    db.execute(
        reorgs_t.insert().values(id=2, split_id=1, block_hash='acf', block_number=102, reinserted=False, node_id='a'))

    resp = await cli.get(f'/v1/wallet/blockchain_tip?tip=aa')
    assert resp.status == 200
    res = (await resp.json())['data']
    assert res == {'blockchainTip': {'blockHash': 'aa', 'blockNumber': 100},
                   'forkData': {'isInFork': False, 'lastUnchangedBlock': 100}}

    resp = await cli.get(f'/v1/wallet/blockchain_tip?tip=ac')
    assert resp.status == 200
    res = (await resp.json())['data']
    assert res == {'blockchainTip': {'blockHash': 'ac', 'blockNumber': 102},
                   'forkData': {'isInFork': False, 'lastUnchangedBlock': 102}}

    resp = await cli.get(f'/v1/wallet/blockchain_tip?tip=abf')
    assert resp.status == 200
    res = (await resp.json())['data']
    assert res == {'blockchainTip': {'blockHash': 'abf', 'blockNumber': 101},
                   'forkData': {'isInFork': True, 'lastUnchangedBlock': 100}}

    resp = await cli.get(f'/v1/wallet/blockchain_tip?tip=acf')
    assert resp.status == 200
    res = (await resp.json())['data']
    assert res == {'blockchainTip': {'blockHash': 'acf', 'blockNumber': 102},
                   'forkData': {'isInFork': True, 'lastUnchangedBlock': 100}}


async def test_get_blockchain_tip_no_block(cli):
    resp = await cli.get(f'/v1/wallet/blockchain_tip?tip=aa')
    assert resp.status == 404
    assert (await resp.json()) == {'status': {'success': False, 'errors': [
        {'field': 'tip', 'error_code': 'BLOCK_NOT_FOUND', 'error_message': 'Block with hash aa not found'}]},
                                   'data': None}


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
         'amount': 100,
         'tx_data': {'hash': 'f1'},
         'is_forked': False,
         'block_number': 100,
         'block_hash': 'b1',
         'ordering': 2},
        {'address': 'a1',
         'type': 'eth',
         'from': 'a3',
         'to': 'a1',
         'asset_address': '',
         'amount': 200,
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
         'amount': 100,
         'tx_data': {'hash': 'f1'},
         'is_forked': False,
         'block_number': 100,
         'block_hash': 'b1',
         'ordering': 1},
        {'address': 'a2',
         'type': 'eth',
         'from': 'a3',
         'to': 'a1',
         'asset_address': '',
         'amount': 300,
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
    assert res == [{'amount': 100,
                    'assetAddress': 'ca1',
                    'from': 'a1',
                    'to': 'a2',
                    'txData': {"hash": "f1"},
                    'type': 'erc20-transfer'},
                   {'amount': 200,
                    'assetAddress': '',
                    'from': 'a3',
                    'to': 'a1',
                    'txData': {"hash": "f2"},
                    'type': 'eth'},
                   {'amount': 100,
                    'assetAddress': 'ca1',
                    'from': 'a1',
                    'to': 'a2',
                    'txData': {"hash": "f1"},
                    'type': 'erc20-transfer'}]

    resp = await cli.get(f'/v1/wallet/transfers?addresses=a1&assets=ca1')
    assert resp.status == 200
    res = (await resp.json())['data']
    assert res == [{'amount': 100,
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
            'balance': 100,
            'tx_number': 1,
            'nonce': 10,
        },
        {
            'address': 'a1',
            'asset_address': 'c2',
            'balance': 200,
            'tx_number': 2,
            'nonce': 10,
        },
        {
            'address': 'a1',
            'asset_address': '',
            'balance': 300,
            'tx_number': 3,
            'nonce': 10,
        },
        {
            'address': 'a2',
            'asset_address': 'c1',
            'balance': 100,
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
                    'assetsSummary': [{'address': '', 'balance': 300.0, 'transfersNumber': 3},
                                      {'address': 'c1', 'balance': 100.0, 'transfersNumber': 1},
                                      {'address': 'c2', 'balance': 200.0, 'transfersNumber': 2}],
                    'outgoingTransactionsNumber': 10},
                   {'address': 'a2',
                    'assetsSummary': [{'address': 'c1', 'balance': 100.0, 'transfersNumber': 1}],
                    'outgoingTransactionsNumber': 5}]

    resp = await cli.get(f'/v1/wallet/assets_summary?addresses=a1&assets=c2')
    assert resp.status == 200
    res = (await resp.json())['data']
    assert res == [{'address': 'a1',
                    'assetsSummary': [{'address': 'c2', 'balance': 200.0, 'transfersNumber': 2}],
                    'outgoingTransactionsNumber': 10},
                   ]
