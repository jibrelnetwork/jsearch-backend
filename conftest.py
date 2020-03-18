import asyncio
import logging

import pytest
from aiohttp.test_utils import TestServer, TestClient
from pathlib import Path

from jsearch.api.app import make_app
from jsearch.syncer.services.api import make_app as syncer_make_app
from jsearch.common import logs
from jsearch.tests.plugins.api import spec_testing_middleware

logger = logging.getLogger(__name__)

pytest_plugins = (
    "jsearch.tests.plugins.cli",
    "jsearch.tests.plugins.databases.dumps",
    "jsearch.tests.plugins.databases.factories.accounts",
    "jsearch.tests.plugins.databases.factories.assets_summary",
    "jsearch.tests.plugins.databases.factories.blocks",
    "jsearch.tests.plugins.databases.factories.chain_events",
    "jsearch.tests.plugins.databases.factories.contracts",
    "jsearch.tests.plugins.databases.factories.internal_transactions",
    "jsearch.tests.plugins.databases.factories.logs",
    "jsearch.tests.plugins.databases.factories.pending_transactions",
    "jsearch.tests.plugins.databases.factories.receipts",
    "jsearch.tests.plugins.databases.factories.reorgs",
    "jsearch.tests.plugins.databases.factories.token_holder",
    "jsearch.tests.plugins.databases.factories.token_descriptions",
    "jsearch.tests.plugins.databases.factories.token_transfers",
    "jsearch.tests.plugins.databases.factories.transactions",
    "jsearch.tests.plugins.databases.factories.uncles",
    "jsearch.tests.plugins.databases.factories.wallet_events",
    "jsearch.tests.plugins.databases.factories.dex_logs",
    "jsearch.tests.plugins.databases.factories.raw.pending_transactions",
    "jsearch.tests.plugins.databases.factories.raw.chain_events",
    "jsearch.tests.plugins.databases.factories.mixed.dex",
    "jsearch.tests.plugins.databases.main_db",
    "jsearch.tests.plugins.databases.raw_db",
    "jsearch.tests.plugins.syncer",
    "jsearch.tests.plugins.settings",
    "jsearch.tests.plugins.tokens.fuck_token",
)


@pytest.fixture
def here() -> Path:
    return Path(__file__).parent / 'tests'


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def loop(event_loop):
    return event_loop


@pytest.fixture(scope="session")
@pytest.mark.asyncio
async def aiohttp_client_session_wide(event_loop):
    """Session-wide `aiohttp.pytest_plugin.aiohttp_client`.

    Behaves same as the lib's fixture, but accepts only `web.Application` as an
    argument.

    Allows to make `cli` fixture below session-wide also. This speeds-up
    unittest by a lot because `cli` fixture execution time is about 0.7 seconds.
    """
    clients = []

    async def go(application):
        server = TestServer(application, loop=event_loop)
        client = TestClient(server, loop=event_loop)

        await client.start_server()
        clients.append(client)

        return client

    yield go

    async def finalize():
        while clients:
            await clients.pop().close()

    await finalize()


@pytest.fixture(scope="session")
def app(loop, aiohttp_client_session_wide):
    app = loop.run_until_complete(make_app())

    app['validate_spec'] = True
    app._middlewares += (spec_testing_middleware,)
    return app


@pytest.fixture(scope="session")
def cli(loop, aiohttp_client_session_wide, app):
    client = loop.run_until_complete(aiohttp_client_session_wide(app))
    return client


@pytest.fixture(scope="session")
def cli_syncer(loop, aiohttp_client_session_wide):
    app = syncer_make_app()
    return loop.run_until_complete(aiohttp_client_session_wide(app))


@pytest.fixture(scope="session", autouse=True)
def setup_logs():
    logs.configure('DEBUG', formatter_class='logging.Formatter')


@pytest.fixture
def uncles(db, main_db_data):
    from jsearch.common.tables import uncles_t
    uncles = [
        {
            "difficulty": 17578564779,
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

        {
            "hash": "0x6a5a801b12b94e1fb24e531b087719d699882a4f948564ba58706934bc5a19ff",
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
            'reward': 3750000000000000000
        }
    ]
    for u in uncles:
        query = uncles_t.insert().values(**u)
        db.execute(query)
    yield uncles

    db.execute('DELETE FROM uncles')
