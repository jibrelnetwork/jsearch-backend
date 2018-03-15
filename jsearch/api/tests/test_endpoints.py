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
    resp = await cli.get('/accounts/0xbb7b8287f3f0a933474a79eae42cbca977791171/')
    assert resp.status == 200
    assert await resp.json() == {'address': '0xbb7b8287f3f0a933474a79eae42cbca977791171',
                                 'balance': 420937500000000000000,
                                 'blockHash': '0xd93f8129b3ed958dff542e717851243b53f2047d49147ea445af02c5e16062e7',
                                 'blockNumber': 125,
                                 'code': '',
                                 'codeHash': 'c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470',
                                 'nonce': 0}
