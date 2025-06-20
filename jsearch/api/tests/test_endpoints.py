import logging

import pytest
from aiohttp.test_utils import TestClient

from jsearch.api.tests.utils import assert_not_404_response
from jsearch.tests.entities import BlockFromDumpWrapper

logger = logging.getLogger(__name__)


async def test_get_block_404(cli):
    resp = await cli.get('/v1/blocks/1')
    await assert_not_404_response(resp)


async def test_unknown_url_404(cli):
    resp = await cli.get('/9a18d3d2-fdd6-4450-961e-a02fb8d4a50e')
    assert resp.status == 404


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


async def test_get_block_by_number_no_forked(cli, db, block_factory):
    # given
    block_factory.create(number=1, hash='aa', is_forked=False)
    block_factory.create(number=2, hash='ab', is_forked=False)
    block_factory.create(number=2, hash='ax', is_forked=True)
    block_factory.create(number=3, hash='ac', is_forked=False)

    # then
    resp = await cli.get('/v1/blocks/2')
    assert resp.status == 200
    b = await resp.json()
    assert b['data']['hash'] == 'ab'
    assert b['data']['number'] == 2


async def test_get_block_by_hash_forked_404(cli, db, block_factory):
    # given
    block_factory.create(number=1, hash='aa', is_forked=False)
    block_factory.create(number=2, hash='ab', is_forked=False)
    block_factory.create(number=2, hash='ax', is_forked=True)
    block_factory.create(number=3, hash='ac', is_forked=False)

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


async def test_get_block_transaction_by_tx_index(cli, block_factory, transaction_factory):
    # given
    block = block_factory.create()
    txs = transaction_factory.create_for_block(block)
    tx = txs[0]

    # when
    resp = await cli.get(f'/v1/blocks/{block.hash}/transactions?transaction_index=0')
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


async def test_get_block_transaction_count(cli, link_txs_with_block, block_factory, transaction_factory):
    # given
    block = block_factory.create()
    tx = transaction_factory.create_for_block(block)

    link_txs_with_block(
        [tx[0].hash, ],
        block.hash
    )

    # when
    resp = await cli.get(f'/v1/blocks/{block.hash}/transaction_count')
    assert resp.status == 200

    # then
    res = (await resp.json())['data']
    assert res == {
        'count': 1
    }


async def test_get_block_transaction_count_by_invalid_hash_block(cli):
    # when
    resp = await cli.get('/v1/blocks/invalid/transaction_count')
    assert resp.status == 200

    # then
    assert (await resp.json())['data'] == {'count': 0}


async def test_get_block_transactions_forked(cli, db, transaction_factory):
    # given
    transaction_factory.create(block_number=1, block_hash='aa', hash='tx1', is_forked=False)
    transaction_factory.create(block_number=2, block_hash='ab', hash='tx2', is_forked=False)
    transaction_factory.create(block_number=2, block_hash='ax', hash='tx3', is_forked=True)
    transaction_factory.create(block_number=3, block_hash='ac', hash='tx3', is_forked=False)

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


@pytest.mark.usefixtures('uncles')
async def test_get_block_uncle_by_uncle_index(cli, main_db_data):
    resp = await cli.get('/v1/blocks/' + main_db_data['blocks'][1]['hash'] + '/uncles?uncle_index=0')
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


@pytest.mark.usefixtures('uncles')
async def test_get_block_uncle_count(cli, main_db_data):
    resp = await cli.get('/v1/blocks/' + main_db_data['blocks'][1]['hash'] + '/uncle_count')
    assert resp.status == 200
    assert (await resp.json())['data'] == {'count': 1}


async def test_get_block_uncle_count_by_invalid_hash_block(cli):
    # when
    resp = await cli.get('/v1/blocks/invalid/uncle_count')
    assert resp.status == 200

    # then
    assert (await resp.json())['data'] == {'count': 0}


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


@pytest.mark.parametrize(
    'url, status_code',
    (
        ('/v1/accounts/balances?addresses=0x0193d941b50d91be6567c7ee1c0fe7af498b4137', 200),
        ('/v1/accounts/0x0193d941b50d91be6567c7ee1c0fe7af498b4137', 404),
        ('/v1/accounts/0x0193d941b50d91be6567c7ee1c0fe7af498b4137/transactions', 200),
        ('/v1/accounts/0x0193d941b50d91be6567c7ee1c0fe7af498b4137/internal_transactions', 200),
        ('/v1/accounts/0x0193d941b50d91be6567c7ee1c0fe7af498b4137/pending_transactions', 200),
        ('/v1/accounts/0x0193d941b50d91be6567c7ee1c0fe7af498b4137/mined_blocks', 200),
        ('/v1/accounts/0x0193d941b50d91be6567c7ee1c0fe7af498b4137/mined_uncles', 200),
        ('/v1/accounts/0x0193d941b50d91be6567c7ee1c0fe7af498b4137/token_transfers', 200),
        ('/v1/accounts/0x0193d941b50d91be6567c7ee1c0fe7af498b4137/token_balance/0x0193d941b50d91be6567c7ee1c0fe7af498b4137', 404),  # NOQA: E501
        ('/v1/accounts/0x0193d941b50d91be6567c7ee1c0fe7af498b4137/token_balances?contract_addresses=0x0193d941b50d91be6567c7ee1c0fe7af498b4137', 200),  # NOQA: E501
        ('/v1/accounts/0x0193d941b50d91be6567c7ee1c0fe7af498b4137/logs', 200),
        ('/v1/accounts/0x0193d941b50d91be6567c7ee1c0fe7af498b4137/logs?topics=0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef', 200),  # NOQA: E501
        ('/v1/accounts/0x0193d941b50d91be6567c7ee1c0fe7af498b4137/transaction_count', 200),
        ('/v1/accounts/0x0193d941b50d91be6567c7ee1c0fe7af498b4137/eth_transfers', 200),
        ('/v1/blocks', 200),
        ('/v1/blocks/latest', 404),
        ('/v1/blocks/latest/transactions', 200),
        ('/v1/blocks/latest/uncles', 200),
        ('/v1/blocks/latest/internal_transactions', 200),
        ('/v1/transactions/0xae334d3879824f8ece42b16f161caaa77417787f779a05534b122de0aabe3f7e', 404),
        ('/v1/transactions/0xae334d3879824f8ece42b16f161caaa77417787f779a05534b122de0aabe3f7e/internal_transactions', 200),  # NOQA: E501
        ('/v1/receipts/0xae334d3879824f8ece42b16f161caaa77417787f779a05534b122de0aabe3f7e', 404),
        ('/v1/uncles', 200),
        ('/v1/uncles/latest', 404),
        ('/v1/tokens/0x0193d941b50d91be6567c7ee1c0fe7af498b4137/transfers', 200),
        ('/v1/tokens/0x0193d941b50d91be6567c7ee1c0fe7af498b4137/holders', 200),
        ('/v1/blockchain_tip', 404),
        ('/v1/wallet/assets_summary?addresses=0x0193d941b50d91be6567c7ee1c0fe7af498b4137&assets=0x0193d941b50d91be6567c7ee1c0fe7af498b4137', 200),  # NOQA: E501
        ('/v1/wallet/events?blockchain_address=0x0193d941b50d91be6567c7ee1c0fe7af498b4137', 200),
    ),
)
async def test_endpoint_does_not_fail_if_database_is_empty(cli: TestClient, url: str, status_code: int) -> None:
    # when
    response = await cli.get(url)

    # then
    assert response.status == status_code


@pytest.mark.parametrize(
    'data, status_code, result',
    (
        ('{"data": "0x68656c6c6f20776f726c64"}', 200, '0x47173285a8d7341e5e972fc677286384f802f8ef42a5ec5f03bbfa254cb01fad'),  # NOQA: E501
        ('{"data": "0x00"}', 200, '0xbc36789e7a1e281436464229828f817d6612f7b477d66591ff96a9e064bcc98a'),
        ('{"data": "0x0"}', 400, None),
        ('{"data": "0x"}', 200, '0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470'),
        ('{"data": "abcdefg"}', 400, None),
        ('{"data": 123456789}', 400, None),
    ),
)
async def test_sha3(cli: TestClient, data: str, status_code: int, result: str) -> None:
    # when
    response = await cli.post('/v1/sha3', data=data)

    # then
    assert response.status == status_code
    if status_code == 200:
        resp_json = await response.json()
        assert resp_json['data'] == result
