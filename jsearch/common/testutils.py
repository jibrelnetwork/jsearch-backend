import os
import os.path
from datetime import datetime

from sqlalchemy import create_engine

from .alembic_utils import upgrade, downgrade
from .tables import contracts_t
from .contracts import ERC20_ABI

def setup_database():
    connection_string = os.environ['JSEARCH_MAIN_DB_TEST']
    upgrade(connection_string, 'head')


def teardown_database():
    connection_string = os.environ['JSEARCH_MAIN_DB_TEST']
    downgrade(connection_string, 'base')




FUCKS_TOKEN = {
        'name': 'FucksToken',
        'byte_code': open(os.path.dirname(__file__) + '/../tests/FucksToken.bin', 'r').read(),
        'source_code': open(os.path.dirname(__file__) + '/../tests/FucksToken.sol', 'r').read(),
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
    }


def add_test_contract(connection_string, address):
    engine = create_engine(connection_string)
    conn = engine.connect()
    query = contracts_t.insert().values(address=address, **FUCKS_TOKEN)
    conn.execute(query)
