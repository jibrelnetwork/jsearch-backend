import os

import pytest
from asyncpgsa import pg

from jsearch.common.testutils import setup_database, teardown_database
from jsearch.common.contracts import ERC20_ABI
from jsearch.common import tables as t


def pytest_runtestloop(session):
    print("SESS:", session)


@pytest.fixture(scope="session", autouse=True)
def my_own_session_run_at_beginning(request):
    print('\nIn my_own_session_run_at_beginning()')
    request.addfinalizer(my_own_session_run_at_end)


def my_own_session_run_at_end():
    print('In my_own_session_run_at_end()')


@pytest.fixture(scope="session", autouse=True)
def setup_database_fixture(request, pytestconfig):
    setup_database()
    request.addfinalizer(teardown_database)


@pytest.fixture
@pytest.mark.asyncio
async def db():
    await pg.init(os.environ['JSEARCH_MAIN_DB_TEST'])
    return pg


@pytest.fixture
@pytest.mark.asyncio
async def contracts(db):
    contracts = [
        {"address": "0x4B3A32dD76EC46a91474d5C08C3904f7cDe6D7d5",
         "name": "TokenContract",
         "byte_code": "xx",
         "source_code": "//",
         "abi": ERC20_ABI,
         "compiler_version": "v0.4.11+commit.68ef5810",
         "optimization_enabled": True,
         "optimization_runs": 200,
         "is_erc20_token": True,
         "token_name": "Token",
         "token_symbol": "TKN",
         "token_decimals": 8},
    ]

    for c in contracts:
        query = t.contracts_t.insert().values(**c)
        res = await pg.execute(query)
    yield contracts
    await pg.execute("DELETE FROM contracts")
