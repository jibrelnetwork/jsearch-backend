import os
import asyncio
from datetime import datetime
import json

import pytest
import py.path
from sqlalchemy import create_engine

from jsearch.common.testutils import setup_database, teardown_database
from jsearch.common import tables as t
from jsearch.common.contracts import ERC20_ABI

from jsearch.api.app import make_app


@pytest.fixture
def here():
    return py.path.local(os.path.dirname(__file__) + '/tests')


@pytest.fixture
def main_db_data(here):
    return json.loads(here.join('maindb_fixture.json').read())


@pytest.fixture
def loop(event_loop):
    '''Ensure usable event loop for everyone.

    If you comment this fixture out, default pytest-aiohttp one is used
    and things start failing (when redis pool is in place).
    '''
    return event_loop


@pytest.fixture(scope='session', autouse=True)
def setup_database_fixture(request, pytestconfig):
    setup_database()
    request.addfinalizer(teardown_database)


@pytest.fixture
@pytest.mark.asyncio
async def cli(event_loop, aiohttp_client):
    app = await make_app()
    return await aiohttp_client(app)


@pytest.fixture(scope='session')
def db():
    engine = create_engine(os.environ['JSEARCH_MAIN_DB_TEST'])
    conn = engine.connect()
    yield conn
    conn.close()


@pytest.fixture
def blocks(db, main_db_data):
    blocks = main_db_data['blocks']
    # import pprint; pprint.pprint(blocks)
    for b in blocks:
        b['size'] = b['size'] or 0
        b['total_difficulty'] = b['total_difficulty'] or 0
        query = t.blocks_t.insert().values(**b)
        res = db.execute(query)
    yield blocks
    db.execute('DELETE FROM blocks')


@pytest.fixture
def transactions(db, blocks, here, main_db_data):
    txs = main_db_data['transactions']
    for tx in txs:
        tx["is_token_transfer"] == bool(tx["is_token_transfer"])

        query = t.transactions_t.insert().values(**tx)
        db.execute(query)
    yield txs
    db.execute('DELETE FROM transactions')


@pytest.fixture
def receipts(db, blocks, main_db_data):
    receipts = main_db_data['receipts']
    for r in receipts:
        query = t.receipts_t.insert().values(**r)
        db.execute(query)
    yield receipts
    db.execute('DELETE FROM receipts')


@pytest.fixture
def accounts(db, blocks, here, main_db_data):
    accounts = main_db_data['accounts']
    for acc in accounts:
        query = t.accounts_t.insert().values(**acc)
        db.execute(query)
    yield accounts
    db.execute('DELETE FROM accounts')


@pytest.fixture
def uncles(db, blocks, main_db_data):
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
               "block_hash": main_db_data['blocks'][1]['hash'],
               "block_number": main_db_data['blocks'][1]['number'],
               'reward': 3750000000000000000},

               {"hash": "0x6a5a801b12b94e1fb24e531b087719d699882a4f948564ba58706934bc5a19ff",
                "number": 62,
                "block_hash": main_db_data['blocks'][2]['hash'],
                "block_number": main_db_data['blocks'][2]['number'],
                "parent_hash": "0x5656b852baa80ce4db00c60998f5cf6e7a8d76f0339d3cf97955d933f731fecf",
                "difficulty": 18180751616,
                "extra_data": "0x476574682f76312e302e302d30636463373634372f6c696e75782f676f312e34",
                "gas_limit": 5000,
                "gas_used": 0,
                "logs_bloom": "0x0",
                "miner": "0x70137010922f2fc2964b3792907f79fbb75febe8",
                "mix_hash": "0x48b762afc38197f6962c31851fd54ebbdff137bae3c64fff414eaa14ec243dbf",
                "nonce": "0x5283f7dfcd4a29ec",
                "receipts_root": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
                "sha3_uncles": "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
                # "size":null,
                "state_root": "0x901a42ee6ef09d68712df93609a8adbce98b314118d69a3dd07497615aa7b37b",
                "timestamp": 1438270505,
                # "total_difficulty":null,
                "transactions_root": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
                'reward': 3750000000000000000}
              ] 
    for u in uncles:
        query = t.uncles_t.insert().values(**u)
        db.execute(query)
    yield accounts
    db.execute('DELETE FROM uncles')


@pytest.fixture
def logs(db, transactions, main_db_data):
    records = main_db_data['logs']
    for r in records:
        query = t.logs_t.insert().values(**r)
        db.execute(query)
    yield records
    db.execute('DELETE FROM logs')


@pytest.fixture
def contracts(db, here, main_db_data):
    contracts = [
        {
            'address': main_db_data['accounts'][2]['address'],
            'name': '',
            'byte_code': here.join('FucksToken.bin').read(),
            'source_code': here.join('FucksToken.sol').read(),
            'abi': ERC20_ABI,
            'compiler_version': 'v0.4.18+commit.9cf6e910',
            'optimization_enabled': True,
            'optimization_runs': 200,
            'constructor_args': '',
            'metadata_hash': 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaa55d',
            'is_erc20_token': True,
            'token_name': 'FucksToken',
            'token_symbol': 'ZFUCK',
            'token_decimals': 2,
            'token_total_supply': 1000000000,
            'grabbed_at': None,
            'verified_at': datetime(2018, 5, 10),
        },
        # {
        #     'address': 'caa',
        #     'name': '',
        #     'byte_code': here.join('FucksToken.bin').read(),
        #     'source_code': here.join('FucksToken.sol').read(),
        #     'abi': ERC20_ABI,
        #     'compiler_version': 'v0.4.18+commit.9cf6e910',
        #     'optimization_enabled': True,
        #     'optimization_runs': 200,
        #     'constructor_args': '',
        #     'metadata_hash': 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaa55d',
        #     'is_erc20_token': False,
        #     'token_name': None,
        #     'token_symbol': None,
        #     'token_decimals': None,
        #     'token_total_supply': None,
        #     'grabbed_at': None,
        #     'verified_at': datetime(2018, 5, 10),
        # },
    ]
    contracts = main_db_data['contracts']
    db.execute('DELETE FROM contracts')
    for c in contracts:
        query = t.contracts_t.insert().values(**c)
        db.execute(query)
    yield contracts
    db.execute('DELETE FROM contracts')


@pytest.fixture(scope='session')
def celery_config():
    return {
        'broker_url': 'redis://',
        'result_backend': 'redis://'
    }
