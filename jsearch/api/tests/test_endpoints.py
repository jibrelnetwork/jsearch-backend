import os
import json

from aiohttp import web
import pytest


async def test_get_block_404(cli):
    resp = await cli.get('/blocks/1')
    assert resp.status == 404


async def test_get_block_by_number(cli, blocks, transactions):
    resp = await cli.get('/blocks/125')
    assert resp.status == 200
    assert await resp.json() == {
        'difficulty': 18136429964,
        'extraData': '0x476574682f4c5649562f76312e302e302f6c696e75782f676f312e342e32',
        'gasLimit': 5000,
        'gasUsed': 0,
        'hash': '0xd93f8129b3ed958dff542e717851243b53f2047d49147ea445af02c5e16062e7',
        'logsBloom': '0x01',
        'miner': '0xbb7b8287f3f0a933474a79eae42cbca977791171',
        'mixHash': '0x665913f982272782b5190dd6ce57d3e1800c80388b8c725c8414f6556cff65f8',
        'nonce': '0x697c2379797b4af9',
        'number': 125,
        'parentHash': '0x57b6c499b06c497350c9f96e8a46ee0503a3888a8ee297f612d1d9dfb0eb376f',
        'receiptsRoot': '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421',
        'sha3Uncles': '0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347',
        'size': None,
        'stateRoot': '0xfa528c95ea0455a48e9cd513453c907635315a556679f8b73c2fbad9c8a90423',
        'timestamp': 1438270497,
        'totalDifficulty': 18136429964,
        'transactions': ["0x8fd6b14d790d40b4dac9651c451250e2348b845e46be9b721fab905c3b526f2a",
                         "0x67762945eeabcd08851c83fc0d0042474f3c32b774abc0f5b435b671d3122cc2"],
        'transactionsRoot': '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421',
        'staticReward': 5000000000000000000,
        'txFees': 52569880000000000,
        'uncleInclusionReward': 156250000000000000,
        'uncles': None}


async def test_get_block_by_hash(cli, blocks, transactions):
    resp = await cli.get('/blocks/0xd93f8129b3ed958dff542e717851243b53f2047d49147ea445af02c5e16062e7')
    assert resp.status == 200
    assert await resp.json() == {
        'difficulty': 18136429964,
        'extraData': '0x476574682f4c5649562f76312e302e302f6c696e75782f676f312e342e32',
        'gasLimit': 5000,
        'gasUsed': 0,
        'hash': '0xd93f8129b3ed958dff542e717851243b53f2047d49147ea445af02c5e16062e7',
        'logsBloom': '0x01',
        'miner': '0xbb7b8287f3f0a933474a79eae42cbca977791171',
        'mixHash': '0x665913f982272782b5190dd6ce57d3e1800c80388b8c725c8414f6556cff65f8',
        'nonce': '0x697c2379797b4af9',
        'number': 125,
        'parentHash': '0x57b6c499b06c497350c9f96e8a46ee0503a3888a8ee297f612d1d9dfb0eb376f',
        'receiptsRoot': '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421',
        'sha3Uncles': '0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347',
        'size': None,
        'stateRoot': '0xfa528c95ea0455a48e9cd513453c907635315a556679f8b73c2fbad9c8a90423',
        'timestamp': 1438270497,
        'totalDifficulty': 18136429964,
        'transactions': ["0x8fd6b14d790d40b4dac9651c451250e2348b845e46be9b721fab905c3b526f2a",
                         "0x67762945eeabcd08851c83fc0d0042474f3c32b774abc0f5b435b671d3122cc2"],
        'transactionsRoot': '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421',
        'staticReward': 5000000000000000000,
        'txFees': 52569880000000000,
        'uncleInclusionReward': 156250000000000000,
        'uncles': None}


async def test_get_block_latest(cli, blocks, transactions):
    resp = await cli.get('/blocks/latest')
    assert resp.status == 200
    assert await resp.json() == {
        'difficulty': 18145285642,
        'extraData': '0x476574682f4c5649562f76312e302e302f6c696e75782f676f312e342e32',
        'gasLimit': 5000,
        'gasUsed': 0,
        'hash': '0x6a27d325aa1f3a1639cb72b704cf80f25470139efaaf5d48ea6e318269a28f8a',
        'logsBloom': '0x01',
        'miner': '0xbb7b8287f3f0a933474a79eae42cbca977791171',
        'mixHash': '0xb6f0e4ea1b694de4755f0405c53e136cace8a2b8763235dba7e1d6f736966a64',
        'nonce': '0xa4dabf1919c3b4ee',
        'number': 126,
        'parentHash': '0xd93f8129b3ed958dff542e717851243b53f2047d49147ea445af02c5e16062e7',
        'receiptsRoot': '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421',
        'sha3Uncles': '0xce79e1ed35eb08ba6262ebba998721bed2c6bf960282c5a5ba796891a19f69b6',
        'size': None,
        'stateRoot': '0x18e42e4f80a76649687e71bf099f9bab0de463155fd085fd4ec7117608b8f55c',
        'timestamp': 1438270500,
        'totalDifficulty': 18136429964,
        'transactions': ['0x8accbe5a1836237291a21cd23f5e0dcb86fcd35dde5aa6b5f0e11a9587743093'],
        'transactionsRoot': '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421',
        'staticReward': 5000000000000000000,
        'txFees': 52569880000000000,
        'uncleInclusionReward': 156250000000000000,
        'uncles': None
    }


async def test_get_account_404(cli, accounts):
    resp = await cli.get('/accounts/x')
    assert resp.status == 404


async def test_get_account(cli, accounts):
    resp = await cli.get('/accounts/0xbb7b8287f3f0a933474a79eae42cbca977791171')
    assert resp.status == 200
    assert await resp.json() == {'address': '0xbb7b8287f3f0a933474a79eae42cbca977791171',
                                 'balance': 420937500000000000000,
                                 'blockHash': '0xd93f8129b3ed958dff542e717851243b53f2047d49147ea445af02c5e16062e7',
                                 'blockNumber': 125,
                                 'code': '',
                                 'codeHash': 'c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470',
                                 'nonce': 0}


async def test_get_account_transactions(cli, blocks, transactions, accounts):
    resp = await cli.get('/accounts/0x0182673de3787e3a77cb1f25fc8b1adedd686465/transactions')
    assert resp.status == 200
    assert await resp.json() == [{'blockHash': '0xd93f8129b3ed958dff542e717851243b53f2047d49147ea445af02c5e16062e7',
                                  'blockNumber': 125,
                                  'from': None,
                                  'gas': '0x61a8',
                                  'gasPrice': '0xba43b7400',
                                  'hash': '0x8fd6b14d790d40b4dac9651c451250e2348b845e46be9b721fab905c3b526f2a',
                                  'input': '0x',
                                  'nonce': '0x51b',
                                  'r': '0x5c3723a80187c010b631a9b288128dac10dc10eaa289902e65e2a857b7e32466',
                                  's': '0x6e8cfc6a77b6e6d36f941baac77083f0a936a75b3df11cf48fbd49cb1323af6e',
                                  'to': '0x0182673de3787e3a77cb1f25fc8b1adedd686465',
                                  'transactionIndex': 1,
                                  'v': '0x1b',
                                  'value': '0x1068e7e28b45fc80'}]


async def test_get_block_transactions(cli, blocks, transactions):
    resp = await cli.get('/blocks/0xd93f8129b3ed958dff542e717851243b53f2047d49147ea445af02c5e16062e7/transactions')
    assert resp.status == 200
    assert await resp.json() == [{'blockHash': '0xd93f8129b3ed958dff542e717851243b53f2047d49147ea445af02c5e16062e7',
                                  'blockNumber': 125,
                                  'from': None,
                                  'gas': '0x61a8',
                                  'gasPrice': '0xba43b7400',
                                  'hash': '0x8fd6b14d790d40b4dac9651c451250e2348b845e46be9b721fab905c3b526f2a',
                                  'input': '0x',
                                  'nonce': '0x51b',
                                  'r': '0x5c3723a80187c010b631a9b288128dac10dc10eaa289902e65e2a857b7e32466',
                                  's': '0x6e8cfc6a77b6e6d36f941baac77083f0a936a75b3df11cf48fbd49cb1323af6e',
                                  'to': '0x0182673de3787e3a77cb1f25fc8b1adedd686465',
                                  'transactionIndex': 1,
                                  'v': '0x1b',
                                  'value': '0x1068e7e28b45fc80'},
                                 {'blockHash': '0xd93f8129b3ed958dff542e717851243b53f2047d49147ea445af02c5e16062e7',
                                  'blockNumber': 125,
                                  'from': None,
                                  'gas': '0x61a8',
                                  'gasPrice': '0xba43b7400',
                                  'hash': '0x67762945eeabcd08851c83fc0d0042474f3c32b774abc0f5b435b671d3122cc2',
                                  'input': '0x',
                                  'nonce': '0x51c',
                                  'r': '0x86975c372e809025d84a16b00f9abaf2433f4ed90f03013261083bec87e2035f',
                                  's': '0x21b1ef431012daea27e3d04164e922d47a29a621350c50e825d245139d08f970',
                                  'to': '0x22bbea521a19c065b6c83a6398e6e21c6f981406',
                                  'transactionIndex': 2,
                                  'v': '0x1b',
                                  'value': '0xec23d4719579180'}]


async def test_get_block_uncles(cli, blocks, uncles):
    resp = await cli.get('/blocks/0xd93f8129b3ed958dff542e717851243b53f2047d49147ea445af02c5e16062e7/uncles')
    assert resp.status == 200
    assert await resp.json() == [{'difficulty': 17578564779,
                                  'blockNumber': 125,
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
                                  'reward': 3750000000000000000,
                                  'transactionsRoot': '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421'}]


async def test_get_transaction(cli, blocks, transactions):
    resp = await cli.get('/transactions/0x8fd6b14d790d40b4dac9651c451250e2348b845e46be9b721fab905c3b526f2a')
    assert resp.status == 200
    assert await resp.json() == {'blockHash': '0xd93f8129b3ed958dff542e717851243b53f2047d49147ea445af02c5e16062e7',
                                 'blockNumber': 125,
                                 'from': None,
                                 'gas': '0x61a8',
                                 'gasPrice': '0xba43b7400',
                                 'hash': '0x8fd6b14d790d40b4dac9651c451250e2348b845e46be9b721fab905c3b526f2a',
                                 'input': '0x',
                                 'nonce': '0x51b',
                                 'r': '0x5c3723a80187c010b631a9b288128dac10dc10eaa289902e65e2a857b7e32466',
                                 's': '0x6e8cfc6a77b6e6d36f941baac77083f0a936a75b3df11cf48fbd49cb1323af6e',
                                 'to': '0x0182673de3787e3a77cb1f25fc8b1adedd686465',
                                 'transactionIndex': 1,
                                 'v': '0x1b',
                                 'value': '0x1068e7e28b45fc80'}


async def test_get_receipt(cli, blocks, transactions, receipts, logs):
    resp = await cli.get('/receipts/0x8fd6b14d790d40b4dac9651c451250e2348b845e46be9b721fab905c3b526f2a')
    assert resp.status == 200
    assert await resp.json() == {
        'blockHash': '0xd93f8129b3ed958dff542e717851243b53f2047d49147ea445af02c5e16062e7',
        'blockNumber': 125,
        'contractAddress': '0x0000000000000000000000000000000000000000',
        'cumulativeGasUsed': 231000,
        'from': '0xbb7b8287f3f0a933474a79eae42cbca977791171',
        'gasUsed': 21000,
        'logs': [{'address': '0x2b237f94b3e8afb3d1d66c8f5e98d78c9c060f9c',
                  'blockHash': '0x70c3dd4bcf59829be6d4a8b97ce8ac821660bc7006c76d107ffffd50372b9832',
                  'blockNumber': 1498834,
                  'data': '0x0000000000000000000000008bc16dae51dfcf0aba8eebb63a2d01cc249f79310000000000000000000000000000000000000000000000004563918244f40000000000000000000000000000bb9bc244d798123fde783fcc1c72d3bb8c18941300000000000000000000000000000000000000000000000000000000000000800000000000000000000000000000000000000000000000000000000000000000',
                  'logIndex': 0,
                  'removed': False,
                  'topics': ['0x92ca3a80853e6663fa31fa10b99225f18d4902939b4c53a9caae9043f6efd004'],
                  'transactionHash': '0x8fd6b14d790d40b4dac9651c451250e2348b845e46be9b721fab905c3b526f2a',
                  'transactionIndex': 2},
                 {'address': '0xbb9bc244d798123fde783fcc1c72d3bb8c189413',
                  'blockHash': '0x70c3dd4bcf59829be6d4a8b97ce8ac821660bc7006c76d107ffffd50372b9832',
                  'blockNumber': 1498834,
                  'data': '0x0000000000000000000000000000000000000000000000004563918244f40000',
                  'logIndex': 1,
                  'removed': False,
                  'topics': ['0xdbccb92686efceafb9bb7e0394df7f58f71b954061b81afb57109bf247d3d75a',
                             '0x0000000000000000000000002b237f94b3e8afb3d1d66c8f5e98d78c9c060f9c'],
                  'transactionHash': '0x8fd6b14d790d40b4dac9651c451250e2348b845e46be9b721fab905c3b526f2a',
                  'transactionIndex': 2}],
        'logsBloom': '0x',
        'root': '0x2acfe9e09e5278ca573b2cba963f624d003b1dbfd318343994aa91de1bd84936',
        'status': 1,
        'to': '0xbb7b8287f3f0a933474a79eae42cbca977791172',
        'transactionHash': '0x8fd6b14d790d40b4dac9651c451250e2348b845e46be9b721fab905c3b526f2a',
        'transactionIndex': 1
    }


async def test_get_blocks_def(cli, blocks):
    resp = await cli.get('/blocks')
    assert resp.status == 200
    assert await resp.json() == [{'difficulty': 18145285642,
                                  'extraData': '0x476574682f4c5649562f76312e302e302f6c696e75782f676f312e342e32',
                                  'gasLimit': 5000,
                                  'gasUsed': 0,
                                  'hash': '0x6a27d325aa1f3a1639cb72b704cf80f25470139efaaf5d48ea6e318269a28f8a',
                                  'logsBloom': '0x01',
                                  'miner': '0xbb7b8287f3f0a933474a79eae42cbca977791171',
                                  'mixHash': '0xb6f0e4ea1b694de4755f0405c53e136cace8a2b8763235dba7e1d6f736966a64',
                                  'nonce': '0xa4dabf1919c3b4ee',
                                  'number': 126,
                                  'parentHash': '0xd93f8129b3ed958dff542e717851243b53f2047d49147ea445af02c5e16062e7',
                                  'receiptsRoot': '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421',
                                  'sha3Uncles': '0xce79e1ed35eb08ba6262ebba998721bed2c6bf960282c5a5ba796891a19f69b6',
                                  'size': None,
                                  'stateRoot': '0x18e42e4f80a76649687e71bf099f9bab0de463155fd085fd4ec7117608b8f55c',
                                  'timestamp': 1438270500,
                                  'totalDifficulty': 18136429964,
                                  'transactions': None,
                                  'transactionsRoot': '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421',
                                  'staticReward': 5000000000000000000,
                                  'txFees': 52569880000000000,
                                  'uncleInclusionReward': 156250000000000000,
                                  'uncles': None},
                                 {'difficulty': 18136429964,
                                  'extraData': '0x476574682f4c5649562f76312e302e302f6c696e75782f676f312e342e32',
                                  'gasLimit': 5000,
                                  'gasUsed': 0,
                                  'hash': '0xd93f8129b3ed958dff542e717851243b53f2047d49147ea445af02c5e16062e7',
                                  'logsBloom': '0x01',
                                  'miner': '0xbb7b8287f3f0a933474a79eae42cbca977791171',
                                  'mixHash': '0x665913f982272782b5190dd6ce57d3e1800c80388b8c725c8414f6556cff65f8',
                                  'nonce': '0x697c2379797b4af9',
                                  'number': 125,
                                  'parentHash': '0x57b6c499b06c497350c9f96e8a46ee0503a3888a8ee297f612d1d9dfb0eb376f',
                                  'receiptsRoot': '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421',
                                  'sha3Uncles': '0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347',
                                  'size': None,
                                  'stateRoot': '0xfa528c95ea0455a48e9cd513453c907635315a556679f8b73c2fbad9c8a90423',
                                  'timestamp': 1438270497,
                                  'totalDifficulty': 18136429964,
                                  'transactions': None,
                                  'transactionsRoot': '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421',
                                  'staticReward': 5000000000000000000,
                                  'txFees': 52569880000000000,
                                  'uncleInclusionReward': 156250000000000000,
                                  'uncles': None}]


async def test_get_blocks_ask(cli, blocks):
    resp = await cli.get('/blocks?order=asc')
    assert resp.status == 200
    assert await resp.json() == [{'difficulty': 18136429964,
                                  'extraData': '0x476574682f4c5649562f76312e302e302f6c696e75782f676f312e342e32',
                                  'gasLimit': 5000,
                                  'gasUsed': 0,
                                  'hash': '0xd93f8129b3ed958dff542e717851243b53f2047d49147ea445af02c5e16062e7',
                                  'logsBloom': '0x01',
                                  'miner': '0xbb7b8287f3f0a933474a79eae42cbca977791171',
                                  'mixHash': '0x665913f982272782b5190dd6ce57d3e1800c80388b8c725c8414f6556cff65f8',
                                  'nonce': '0x697c2379797b4af9',
                                  'number': 125,
                                  'parentHash': '0x57b6c499b06c497350c9f96e8a46ee0503a3888a8ee297f612d1d9dfb0eb376f',
                                  'receiptsRoot': '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421',
                                  'sha3Uncles': '0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347',
                                  'size': None,
                                  'stateRoot': '0xfa528c95ea0455a48e9cd513453c907635315a556679f8b73c2fbad9c8a90423',
                                  'timestamp': 1438270497,
                                  'totalDifficulty': 18136429964,
                                  'transactions': None,
                                  'transactionsRoot': '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421',
                                  'staticReward': 5000000000000000000,
                                  'txFees': 52569880000000000,
                                  'uncleInclusionReward': 156250000000000000,
                                  'uncles': None},
                                 {'difficulty': 18145285642,
                                  'extraData': '0x476574682f4c5649562f76312e302e302f6c696e75782f676f312e342e32',
                                  'gasLimit': 5000,
                                  'gasUsed': 0,
                                  'hash': '0x6a27d325aa1f3a1639cb72b704cf80f25470139efaaf5d48ea6e318269a28f8a',
                                  'logsBloom': '0x01',
                                  'miner': '0xbb7b8287f3f0a933474a79eae42cbca977791171',
                                  'mixHash': '0xb6f0e4ea1b694de4755f0405c53e136cace8a2b8763235dba7e1d6f736966a64',
                                  'nonce': '0xa4dabf1919c3b4ee',
                                  'number': 126,
                                  'parentHash': '0xd93f8129b3ed958dff542e717851243b53f2047d49147ea445af02c5e16062e7',
                                  'receiptsRoot': '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421',
                                  'sha3Uncles': '0xce79e1ed35eb08ba6262ebba998721bed2c6bf960282c5a5ba796891a19f69b6',
                                  'size': None,
                                  'stateRoot': '0x18e42e4f80a76649687e71bf099f9bab0de463155fd085fd4ec7117608b8f55c',
                                  'timestamp': 1438270500,
                                  'totalDifficulty': 18136429964,
                                  'transactions': None,
                                  'transactionsRoot': '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421',
                                  'staticReward': 5000000000000000000,
                                  'txFees': 52569880000000000,
                                  'uncleInclusionReward': 156250000000000000,
                                  'uncles': None},
                                 ]


async def test_get_blocks_limit_offset(cli, blocks):
    resp = await cli.get('/blocks?limit=1')
    assert resp.status == 200
    result = await resp.json()
    assert len(result) == 1
    assert result[0]['number'] == 126

    resp = await cli.get('/blocks?limit=1&offset=1')
    assert resp.status == 200
    result = await resp.json()
    assert len(result) == 1
    assert result[0]['number'] == 125


async def test_get_uncles(cli, blocks, uncles):
    resp = await cli.get('/uncles')
    assert resp.status == 200
    assert await resp.json() == [
        {'blockNumber': 126,
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
         'reward': 3750000000000000000,
         'transactionsRoot': '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421'},
        {'blockNumber': 125,
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
         'reward': 3750000000000000000,
         'transactionsRoot': '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421'}
    ]


async def test_get_uncles_asc(cli, blocks, uncles):
    resp = await cli.get('/uncles?order=asc')
    assert resp.status == 200
    uncles = await resp.json()
    assert uncles[0]['number'] == 61
    assert uncles[1]['number'] == 62


async def test_get_uncles_offset_limit(cli, blocks, uncles):
    resp = await cli.get('/uncles?offset=1&limit=1')
    assert resp.status == 200
    uncles = await resp.json()
    assert len(uncles) == 1
    assert uncles[0]['number'] == 61


async def test_get_uncle_404(cli, uncles):
    resp = await cli.get('/uncles/111')
    assert resp.status == 404
    resp = await cli.get('/uncles/0x6a')
    assert resp.status == 404


async def test_get_uncle_by_hash(cli, blocks, uncles):
    resp = await cli.get('/uncles/0x6a5a801b12b94e1fb24e531b087719d699882a4f948564ba58706934bc5a19ff')
    assert resp.status == 200
    assert await resp.json() == {
        'blockNumber': 126,
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
        'reward': 3750000000000000000,
        'transactionsRoot': '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421'
    }


async def test_get_uncle_by_number(cli, blocks, uncles):
    resp = await cli.get('/uncles/62')
    assert resp.status == 200
    assert await resp.json() == {
        'blockNumber': 126,
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
        'reward': 3750000000000000000,
        'transactionsRoot': '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421'
    }


async def test_verify_contract_ok(db, celery_worker, cli, transactions, receipts):
    contract_data = {
        'address': '0xbb7b8287f3f0a933474a79eae42cbcaaaaaaaaaa',
        'contract_name': 'FucksToken',
        'compiler': 'v0.4.18+commit.9cf6e910',
        'optimization_enabled': True,
        'constructor_args': None,
        'source_code': open(os.path.join(os.path.dirname(__file__), 'FucksToken.sol'), 'r').read()
    }
    resp = await cli.post('/verify_contract', json=contract_data)
    assert resp.status == 200
    assert await resp.json() == {'verification_passed': True}

    from sqlalchemy import select
    from jsearch.common.tables import contracts_t
    from jsearch.common.contracts import ERC20_ABI

    q = select([contracts_t])
    rows = db.execute(q).fetchall()
    assert len(rows) == 1
    c = rows[0]
    assert c['address'] == contract_data['address']
    assert c['name'] == contract_data['contract_name']
    assert c['compiler_version'] == contract_data['compiler']
    assert c['optimization_enabled'] == contract_data['optimization_enabled']
    assert c['constructor_args'] == ''
    assert c['source_code'] == contract_data['source_code']
    abi = json.load(open(os.path.join(os.path.dirname(__file__), 'FucksToken.abi'), 'rb'))
    assert c['abi'] == abi
    assert c['metadata_hash'] == 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaa55d'
    assert c['grabbed_at'] is None
    assert c['verified_at'] is not None
    assert c['is_erc20_token'] is True


