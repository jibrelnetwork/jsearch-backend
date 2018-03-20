import os

import pytest

from jsearch.syncer import database


@pytest.mark.asyncio
async def test_maindb_insert_header():
    db = database.MainDB(os.environ['JSEARCH_MAIN_DB_TEST'])
    await db.connect()
    header = {'block_number': 46170,
              'block_hash': '0xf4a537e8e2233149929a9b6964c9aced6ee95f42131aa6b648d2c7946dfc6fe2',
              'fields': '{"hash": "0xf4a537e8e2233149929a9b6964c9aced6ee95f42131aa6b648d2c7946dfc6fe2", \
                          "miner": "0xf927a40c8b7f6e07c5af7fa2155b4864a4112b13", \
                           "nonce": "0x827a6369128a45a1", \
                           "number": "0xb45a", \
                           "gasUsed": "0x5208", \
                           "mixHash": "0xc32cda5582c2d75e20084e35f87a0e2af22a69dc43b7f5441cdcc9cb7dc7ea39", \
                           "gasLimit": "0x5374", \
                           "extraData": "0x476574682f76312e302e312f6c696e75782f676f312e342e32", \
                           "logsBloom": "0x0000", \
                           "stateRoot": "0x4150d34e4d7cef3cb2eb6baf1fc84a6470d1d69c7ebba950c64e0b36e27bf42b", \
                           "timestamp": "0x55c427e6", \
                           "difficulty": "0x15308f3e385", \
                           "parentHash": "0x5793f91c9fa8f824d8ed77fc1687dddcf334da81c68be65a782a36463b6f7998", \
                           "sha3Uncles": "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347", \
                           "receiptsRoot": "0xc3c8d68d9f98582e3ba95df6cfbe433993331b477fa0f6b27766c6301123d749", \
                           "transactionsRoot": "0x59a195bec25ed6f19d81c71ea96629abbba0cf991de9649dc6d8738c4cd7a3a4"}'}
    uncles = []
    transactions = [{
        'r': '0x34b6fdc33ea520e8123cf5ac4a9ff476f639cab68980cd9366ccae7aef437ea0',
        's': '0xe517caa5f50e27ca0d1e9a92c503b4ccb039680c6d9d0c71203ed611ea4feb33',
        'v': '0x1b',
        'to': '0xc93f2250589a6563f5359051c1ea25746549f0d8',
        'gas': '0x5208',
        'hash': '0x9e6e19637bb625a8ff3d052b7c2fe57dc78c55a15d258d77c43d5a9c160b0384',
        'input': '0x',
        'nonce': '0x0',
        'value': '0x208686e75e903bc000',
        'gasPrice': '0x746a528800'}]
    accounts = [{'address': '0x63Ac545C991243fa18aec41D4F6f598e555015dc',
                 'fields': '{"code": "", "root": "56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421", "nonce": 1, "balance": "0", "storage": {}, "codeHash": "c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"}'},
                {'address': '0xC93f2250589a6563f5359051c1eA25746549f0D8',
                 'fields': '{"code": "", "root": "56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421", "nonce": 0, "balance": "599989500000000000000", "storage": {}, "codeHash": "c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"}'},
                {'address': '0xf927a40C8B7F6E07c5af7FA2155B4864a4112B13',
                 'fields': '{"code": "", "root": "56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421", "nonce": 0, "balance": "27103448000000000000000", "storage": {}, "codeHash": "c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"}'}]

    receipts = {'fields': '{"Receipts": [{"logs":[{"data": "0x0f400",\
                                                  "topics": ["0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c"],\
                                                  "address": "0x2761c0ad62d0f7f96f38332cabb9229378c9bfc9",\
                                                  "removed": false,\
                                                  "logIndex": "0x0",\
                                                  "blockHash": "0xf4a537e8e2233149929a9b6964c9aced6ee95f42131aa6b648d2c7946dfc6fe2",\
                                                  "blockNumber": "0x650fd",\
                                                  "transactionHash": "0xf254afac1fd2a84275316e2d43bbb91a12583f75f2c556a8e69b24fe316d6920",\
                                                  "transactionIndex": "0x0"}],\
                            "root": "0x59383cc2bde1aeab23d8fc741c82e213b90c0dcbba537d9cd4eae227758efd70",\
                            "status": "0x1",\
                            "gasUsed": "0x5208",\
                            "logsBloom": "0x000",\
                            "contractAddress": "0x0000000000000000000000000000000000000000",\
                            "transactionHash": "0x9e6e19637bb625a8ff3d052b7c2fe57dc78c55a15d258d77c43d5a9c160b0384",\
                            "cumulativeGasUsed": "0x5208"}]}'}
    await db.write_block(header, uncles, transactions, receipts, accounts)

    pg = database.pg
    blocks = await pg.fetch('SELECT * FROM blocks')
    assert len(blocks) == 1
    assert dict(blocks[0]) == {
        'difficulty': 1456144114565,
        'extra_data': '0x476574682f76312e302e312f6c696e75782f676f312e342e32',
        'gas_limit': 21364,
        'gas_used': 21000,
        'hash': '0xf4a537e8e2233149929a9b6964c9aced6ee95f42131aa6b648d2c7946dfc6fe2',
        'is_sequence_sync': True,
        'logs_bloom': '0x0000',
        'miner': '0xf927a40c8b7f6e07c5af7fa2155b4864a4112b13',
        'mix_hash': '0xc32cda5582c2d75e20084e35f87a0e2af22a69dc43b7f5441cdcc9cb7dc7ea39',
        'nonce': '0x827a6369128a45a1',
        'number': 46170,
        'parent_hash': '0x5793f91c9fa8f824d8ed77fc1687dddcf334da81c68be65a782a36463b6f7998',
        'receipts_root': '0xc3c8d68d9f98582e3ba95df6cfbe433993331b477fa0f6b27766c6301123d749',
        'sha3_uncles': '0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347',
        'size': None,
        'state_root': '0x4150d34e4d7cef3cb2eb6baf1fc84a6470d1d69c7ebba950c64e0b36e27bf42b',
        'timestamp': 1438918630,
        'total_difficulty': None,
        'transactions_root': '0x59a195bec25ed6f19d81c71ea96629abbba0cf991de9649dc6d8738c4cd7a3a4'}

    receipts = await pg.fetch('SELECT * FROM receipts')
    assert len(receipts) == 1
    assert dict(receipts[0]) == {
        'block_hash': '0xf4a537e8e2233149929a9b6964c9aced6ee95f42131aa6b648d2c7946dfc6fe2',
        'block_number': 46170,
        'contract_address': '0x0000000000000000000000000000000000000000',
        'cumulative_gas_used': 21000,
        'from': None,
        'gas_used': 21000,
        'logs_bloom': '0x000',
        'root': '0x59383cc2bde1aeab23d8fc741c82e213b90c0dcbba537d9cd4eae227758efd70',
        'status': 1,
        'to': '0xc93f2250589a6563f5359051c1ea25746549f0d8',
        'transaction_hash': '0x9e6e19637bb625a8ff3d052b7c2fe57dc78c55a15d258d77c43d5a9c160b0384',
        'transaction_index': 0}

    logs = await pg.fetch('SELECT * FROM logs')
    assert len(logs) == 1
    assert dict(logs[0]) == {
        'address': '0x2761c0ad62d0f7f96f38332cabb9229378c9bfc9',
        'block_hash': '0xf4a537e8e2233149929a9b6964c9aced6ee95f42131aa6b648d2c7946dfc6fe2',
        'block_number': 413949,
        'data': '0x0f400',
        'log_index': 0,
        'removed': False,
        'topics': ['0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c'],
        'transaction_hash': '0xf254afac1fd2a84275316e2d43bbb91a12583f75f2c556a8e69b24fe316d6920',
        'transaction_index': 0}
