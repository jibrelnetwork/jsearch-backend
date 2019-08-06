import logging
from unittest import mock

import pytest
from asynctest import CoroutineMock

from jsearch import settings
from jsearch.api.tests.utils import assert_not_404_response
from jsearch.common.tables import (
    assets_transfers_t,
)
from jsearch.tests.entities import BlockFromDumpWrapper

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.usefixtures('disable_metrics_setup')


async def test_get_block_404(cli):
    resp = await cli.get('/v1/blocks/1')
    await assert_not_404_response(resp)


async def test_get_block_by_number(cli, db, link_txs_with_block, block_factory, transaction_factory):
    # given
    block = block_factory.create()
    from_tx, to_tx = transaction_factory.create_for_block(block)

    link_txs_with_block([from_tx.hash, to_tx.hash], block.hash)

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
        'transactions': [from_tx.hash, to_tx.hash],
        'transactionsRoot': block.transactions_root,
        'staticReward': str(int(block.static_reward)),
        'txFees': str(int(block.tx_fees)),
        'uncleInclusionReward': str(int(block.uncle_inclusion_reward)),
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
    block = BlockFromDumpWrapper.from_dump(
        dump=main_db_data,
        filters={'hash': block_hash},
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
        'staticReward': str(b['static_reward']),
        'txFees': str(b['tx_fees']),
        'uncleInclusionReward': str(b['uncle_inclusion_reward']),
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
        'staticReward': str(b['static_reward']),
        'txFees': str(b['tx_fees']),
        'uncleInclusionReward': str(b['uncle_inclusion_reward']),
        'uncles': []
    }


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
    assert res == [
        {
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
            'value': tx.value,
        },
    ]


async def test_get_block_transactions_forked(cli, db):
    # given
    db.execute('INSERT INTO transactions (block_number, block_hash, timestamp, hash, is_forked, transaction_index)'
               'values (%s, %s, %s, %s, %s, %s)', [
                   (1, 'aa', 1550000000, 'tx1', False, 1),
                   (2, 'ab', 1550000000, 'tx2', False, 1),
                   (2, 'ax', 1550000000, 'tx3', True, 1),
                   (3, 'ac', 1550000000, 'tx3', False, 1),
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
        'timestamp': block.timestamp,
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
            'reward': str(3750000000000000000),
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
        'timestamp': None,
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
             'timestamp': main_db_data['logs'][0]['timestamp'],
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


@pytest.mark.usefixtures('uncles')
async def test_get_uncles(cli, main_db_data):
    resp = await cli.get('/v1/uncles?timestamp=1550000000')
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
         'reward': str(3750000000000000000),
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
         'reward': str(3750000000000000000),
         'transactionsRoot': '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421'}
    ]


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
        'reward': str(3750000000000000000),
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
        'reward': str(3750000000000000000),
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


async def test_get_blockchain_tip(cli, block_factory):
    block_factory.create()
    last_block = block_factory.create()

    response = await cli.get(f'/v1/blockchain_tip?tip=aa')
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
        {
            'address': 'a1',
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
            'ordering': 2
        },
        {
            'address': 'a1',
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
            'ordering': 1
        },
        {
            'address': 'a2',
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
            'ordering': 1
        },
        {
            'address': 'a2',
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
            'ordering': 1
        },

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
