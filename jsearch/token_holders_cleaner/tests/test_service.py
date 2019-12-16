import functools

import pytest
from sqlalchemy.engine import Engine

from jsearch.common.tables import token_holders_t
from jsearch.tests.plugins.databases.factories.assets_summary import AssetsSummaryPairFactory
from jsearch.tests.plugins.databases.factories.common import generate_address
from jsearch.tests.plugins.databases.factories.token_holder import TokenHolderFactory
from jsearch.token_holders_cleaner.service import TokenHoldersCleaner

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def token_holders_cleaner(db_dsn: str) -> TokenHoldersCleaner:
    service = TokenHoldersCleaner(main_db_dsn=db_dsn)

    await service.on_start()
    yield service
    await service.on_stop()


async def test_token_holders_cleaner_removes_stale_rows(
        token_holders_cleaner: TokenHoldersCleaner,
        token_holder_factory: TokenHolderFactory,
        assets_summary_pair_factory: AssetsSummaryPairFactory,
        db: Engine,
):
    account_address = generate_address()
    token_address = generate_address()

    create_holders_batch = functools.partial(
        token_holder_factory.create_batch,
        account_address=account_address,
        token_address=token_address,
    )

    assets_summary_pair_factory.create(address=account_address, asset_address=token_address)

    token_holder_factory.reset_sequence(1)
    create_holders_batch(size=3)
    fresh_holders = create_holders_batch(size=7)
    fresh_holders_hashes = {r.block_hash for r in fresh_holders}

    await token_holders_cleaner.clean_next_batch(last_scanned='0')

    db_holders = db.execute(token_holders_t.select()).fetchall()
    db_holders_hashes = {r.block_hash for r in db_holders}

    assert db_holders_hashes == fresh_holders_hashes
