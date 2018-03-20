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
        'uncles': None}


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


async def test_get_receipt(cli, blocks, transactions, receipts):
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
