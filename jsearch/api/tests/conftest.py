import os

import pytest
from asyncpgsa import pg

from jsearch.common.testutils import setup_database, teardown_database
from jsearch.common import tables as t

from jsearch.api.app import make_app


@pytest.fixture(scope="session", autouse=True)
def setup_database_fixture(request, pytestconfig):
    setup_database()
    request.addfinalizer(teardown_database)


@pytest.fixture
def cli(loop, aiohttp_client):
    app = loop.run_until_complete(make_app())
    return loop.run_until_complete(aiohttp_client(app))


@pytest.fixture
@pytest.mark.asyncio
async def db():
    await pg.init(os.environ['JSEARCH_MAIN_DB_TEST'])
    # conn = await pg.pool.acquire()
    # print("BEFORE")
    # tx = conn.transaction()
    # await tx.start()
    # # await conn.execute('BEGIN')
    # yield conn
    # print("AFTER")
    # await tx.rollback()
    # # await conn.execute('ROLLBACK')
    # await pg.pool.release(conn)
    return pg


@pytest.fixture
@pytest.mark.asyncio
async def blocks(db):
    blocks = [{"difficulty": 18136429964,
               "extra_data": "0x476574682f4c5649562f76312e302e302f6c696e75782f676f312e342e32",
               "gas_limit": 5000,
               "gas_used": 0,
               "hash": "0xd93f8129b3ed958dff542e717851243b53f2047d49147ea445af02c5e16062e7",
               "logs_bloom": "0x01",
               "miner": "0xbb7b8287f3f0a933474a79eae42cbca977791171",
               "mix_hash": "0x665913f982272782b5190dd6ce57d3e1800c80388b8c725c8414f6556cff65f8",
               "nonce": "0x697c2379797b4af9",
               "number": 125,
               "parent_hash": "0x57b6c499b06c497350c9f96e8a46ee0503a3888a8ee297f612d1d9dfb0eb376f",
               "receipts_root": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
               "sha3_uncles": "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
               # "size": None,
               "state_root": "0xfa528c95ea0455a48e9cd513453c907635315a556679f8b73c2fbad9c8a90423",
               "timestamp": 1438270497,
               "total_difficulty": 18136429964,
               "transactions_root": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
               "is_sequence_sync": True},
              {"difficulty": 18145285642,
               "extra_data": "0x476574682f4c5649562f76312e302e302f6c696e75782f676f312e342e32",
               "gas_limit": 5000,
               "gas_used": 0,
               "hash": "0x6a27d325aa1f3a1639cb72b704cf80f25470139efaaf5d48ea6e318269a28f8a",
               "logs_bloom": "0x01",
               "miner": "0xbb7b8287f3f0a933474a79eae42cbca977791171",
               "mix_hash": "0xb6f0e4ea1b694de4755f0405c53e136cace8a2b8763235dba7e1d6f736966a64",
               "nonce": "0xa4dabf1919c3b4ee",
               "number": 126,
               "parent_hash": "0xd93f8129b3ed958dff542e717851243b53f2047d49147ea445af02c5e16062e7",
               "receipts_root": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
               "sha3_uncles": "0xce79e1ed35eb08ba6262ebba998721bed2c6bf960282c5a5ba796891a19f69b6",
               # "size": None,
               "state_root": "0x18e42e4f80a76649687e71bf099f9bab0de463155fd085fd4ec7117608b8f55c",
               "timestamp": 1438270500,
               "total_difficulty": 18136429964,
               "transactions_root": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
               "is_sequence_sync": True}]
    for b in blocks:
        query = t.blocks_t.insert().values(**b)
        await pg.execute(query)
    yield blocks
    await pg.execute("DELETE FROM blocks")


@pytest.fixture
@pytest.mark.asyncio
async def transactions(db, blocks):
    txs = [
        {
            "block_hash": blocks[0]['hash'],
            "block_number": blocks[0]['number'],
            "from": None,
            "gas": "0x61a8",
            "gas_price": "0xba43b7400",
            "hash": "0x8fd6b14d790d40b4dac9651c451250e2348b845e46be9b721fab905c3b526f2a",
            "input": "0x",
            "nonce": "0x51b",
            "r": "0x5c3723a80187c010b631a9b288128dac10dc10eaa289902e65e2a857b7e32466",
            "s": "0x6e8cfc6a77b6e6d36f941baac77083f0a936a75b3df11cf48fbd49cb1323af6e",
            "to": "0x0182673de3787e3a77cb1f25fc8b1adedd686465",
            "transaction_index": 1,
            "v": "0x1b",
            "value": "0x1068e7e28b45fc80"
        },
        {
            "block_hash": blocks[0]['hash'],
            "block_number": blocks[0]['number'],
            "from": None,
            "gas": "0x61a8",
            "gas_price": "0xba43b7400",
            "hash": "0x67762945eeabcd08851c83fc0d0042474f3c32b774abc0f5b435b671d3122cc2",
            "input": "0x",
            "nonce": "0x51c",
            "r": "0x86975c372e809025d84a16b00f9abaf2433f4ed90f03013261083bec87e2035f",
            "s": "0x21b1ef431012daea27e3d04164e922d47a29a621350c50e825d245139d08f970",
            "to": "0x22bbea521a19c065b6c83a6398e6e21c6f981406",
            "transaction_index": 2,
            "v": "0x1b",
            "value": "0xec23d4719579180"
        },
        {
            "block_hash": blocks[1]['hash'],
            "block_number": blocks[1]['number'],
            "from": None,
            "gas": "0x61a8",
            "gas_price": "0xba43b7400",
            "hash": "0x8accbe5a1836237291a21cd23f5e0dcb86fcd35dde5aa6b5f0e11a9587743093",
            "input": "0x",
            "nonce": "0x51d",
            "r": "0x60cb761b2c786feeda43c22db251148c54ccc587a0aa47166f99d411c29290b8",
            "s": "0x5f10a7d9473e1f19d5ebbec86007f825db60896de80b536f340a7d51f6ec8aa4",
            "to": "0x51033f1a1a59cb6a1bf6ca2087a53bd202ac1c83",
            "transaction_index": 1,
            "v": "0x1c",
            "value": "0x1425e9ad089d6600"
        }
    ]
    for tx in txs:
        query = t.transactions_t.insert().values(**tx)
        await pg.execute(query)
    yield txs
    await pg.execute("DELETE FROM transactions")


@pytest.fixture
@pytest.mark.asyncio
async def receipts(db, blocks):
    receipts = [
        {
          "block_hash": "0xd93f8129b3ed958dff542e717851243b53f2047d49147ea445af02c5e16062e7",
          "block_number": 125,
          "contract_address": "0x0000000000000000000000000000000000000000",
          "cumulative_gas_used": 231000,
          "from": '0xbb7b8287f3f0a933474a79eae42cbca977791171',
          "gas_used": 21000,
          "logs_bloom": "0x",
          "root": "0x2acfe9e09e5278ca573b2cba963f624d003b1dbfd318343994aa91de1bd84936",
          "to": '0xbb7b8287f3f0a933474a79eae42cbca977791172',
          "transaction_hash": "0x8fd6b14d790d40b4dac9651c451250e2348b845e46be9b721fab905c3b526f2a",
          "transaction_index": 1,
          "status": 1
        },
        {
          "block_hash": "0x6a27d325aa1f3a1639cb72b704cf80f25470139efaaf5d48ea6e318269a28f8a",
          "block_number": 126,
          "contract_address": "0x0000000000000000000000000000000000000000",
          "cumulative_gas_used": 273000,
          "from": '0xbb7b8287f3f0a933474a79eae42cbca977791171',
          "gas_used": 21000,
          "logs_bloom": "0x0",
          "root": "0xd0ce3248c7752b7f77b8aece6fe954ff47a5f1a0ae09c9036d1e614afd66ba6f",
          "to": '0xbb7b8287f3f0a933474a79eae42cbca977791172',
          "transaction_hash": "0x8accbe5a1836237291a21cd23f5e0dcb86fcd35dde5aa6b5f0e11a9587743093",
          "transaction_index": 1,
          "status": 1
        }
    ]
    for r in receipts:
        query = t.receipts_t.insert().values(**r)
        await pg.execute(query)
    yield receipts
    await pg.execute("DELETE FROM receipts")


@pytest.fixture
@pytest.mark.asyncio
async def accounts(db, blocks):
    accounts = [
        {"block_number": blocks[0]['number'],
         "block_hash": blocks[0]['hash'],
         "address":"0xbb7b8287f3f0a933474a79eae42cbca977791171",
         "nonce":0,
         "code":"",
                "code_hash":"c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470",
                "root":"56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
                "storage":{},
                "balance": 420937500000000000000},
    ]
    for acc in accounts:
        query = t.accounts_t.insert().values(**acc)
        await pg.execute(query)
    yield accounts
    await pg.execute("DELETE FROM accounts")


@pytest.fixture
@pytest.mark.asyncio
async def uncles(db, blocks):
    uncles = [{"difficulty": 17578564779,
               "extra_data": "0x476574682f76312e302e302f6c696e75782f676f312e342e32",
               "gas_limit": 5000,
               "gas_used": 0,
               "hash": "0x7852fb223883cd9af4cd9d448998c879a1f93a02954952666075df696c61a2cc",
               "logs_bloom": "0x0",
               "miner": "0x0193d941b50d91be6567c7ee1c0fe7af498b4137",
               "mix_hash": "0x94a09bb3ef9208bf434855efdb1089f80d07334d91930387a1f3150494e806cb",
               "nonce": "0x32de6ee381be0179",
               "number": 61,
               "parent_hash": "0x3cd0324c7ba14ba7cf6e4b664dea0360681458d76bd25dfc0d2207ce4e9abed4",
               "receipts_root": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
               "sha3_uncles": "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
               # "size": None,
               "state_root": "0x1f4f1cf07f087191901752fe3da8ca195946366db6565f17afec5c04b3d75fd8",
               "timestamp": 1438270332,
               # "total_difficulty": None,
               "transactions_root": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
               "block_hash": "0xd93f8129b3ed958dff542e717851243b53f2047d49147ea445af02c5e16062e7",
               "block_number": 125}
              ]
    for u in uncles:
        query = t.uncles_t.insert().values(**u)
        await pg.execute(query)
    yield accounts
    await pg.execute("DELETE FROM uncles")
