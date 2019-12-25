import functools

import pytest
from sqlalchemy.engine import Engine

from jsearch.common.tables import token_holders_t
from jsearch.tests.plugins.databases.factories.assets_summary import AssetsSummaryPairFactory
from jsearch.tests.plugins.databases.factories.common import generate_address
from jsearch.tests.plugins.databases.factories.token_holder import TokenHolderFactory
from jsearch.token_holders_cleaner.service import TokenHoldersCleaner, get_starting_pair

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def token_holders_cleaner(db_dsn: str) -> TokenHoldersCleaner:
    service = TokenHoldersCleaner(main_db_dsn=db_dsn)

    await service.on_start()
    await service.database.on_start()

    yield service

    await service.database.on_stop()
    await service.on_stop()


async def test_token_holders_cleaner_removes_stale_rows(
        token_holders_cleaner: TokenHoldersCleaner,
        token_holder_factory: TokenHolderFactory,
        assets_summary_pair_factory: AssetsSummaryPairFactory,
        db: Engine,
) -> None:
    """
    For this case:
    [1] --- Account1-Token1
    [2] --- Account1-Token1
    [3] --- Account1-Token1
    [4] --- Account1-Token1
    [5] --- Account1-Token1
    [6] --- Account1-Token1
    [7] --- Account1-Token1
    [8] --- Account1-Token1
    [9] --- Account1-Token1
    [10] --- Account1-Token1

    Old entry `Account1-Token1` at blocks from `[1]` to `[4]` can be safely
    removed. Others should be kept.
    """
    account_address = generate_address()
    token_address = generate_address()

    create_holders_batch = functools.partial(
        token_holder_factory.create_batch,
        account_address=account_address,
        token_address=token_address,
    )

    assets_summary_pair_factory.create(address=account_address, asset_address=token_address)

    token_holder_factory.reset_sequence(1)
    create_holders_batch(size=4)
    fresh_holders = create_holders_batch(size=6)
    fresh_holders_hashes = {r.block_hash for r in fresh_holders}

    await token_holders_cleaner.clean_next_batch(get_starting_pair(), 0)

    db_holders = db.execute(token_holders_t.select()).fetchall()
    db_holders_hashes = {r.block_hash for r in db_holders}

    assert db_holders_hashes == fresh_holders_hashes


async def test_token_holders_cleaner_does_not_affect_older_records_with_no_updates(
        token_holders_cleaner: TokenHoldersCleaner,
        token_holder_factory: TokenHolderFactory,
        assets_summary_pair_factory: AssetsSummaryPairFactory,
        db: Engine,
) -> None:
    """
    For this case:
    [1] --- Account1-Token1
            Account1-Token2
            Account2-Token1
            Account2-Token2
    ...
    [7] --- Account1-Token1

    Old entry `Account1-Token1` at block `[1]` can be safely removed. Others
    should be kept.
    """
    account1 = generate_address()
    token1 = generate_address()

    account2 = generate_address()
    token2 = generate_address()

    assets_summary_pair_factory.create(address=account1, asset_address=token1)
    assets_summary_pair_factory.create(address=account1, asset_address=token2)
    assets_summary_pair_factory.create(address=account2, asset_address=token1)
    assets_summary_pair_factory.create(address=account2, asset_address=token2)

    # stale account1-token1 on the first block
    token_holder_factory.create(account_address=account1, token_address=token1, block_number=1),

    fresh_holders = [
        token_holder_factory.create(account_address=account1, token_address=token1, block_number=7),
        token_holder_factory.create(account_address=account1, token_address=token2, block_number=1),
        token_holder_factory.create(account_address=account2, token_address=token1, block_number=1),
        token_holder_factory.create(account_address=account2, token_address=token2, block_number=1),
    ]

    fresh_holders_hashes = {r.block_hash for r in fresh_holders}

    await token_holders_cleaner.clean_next_batch(get_starting_pair(), 0)

    db_holders = db.execute(token_holders_t.select()).fetchall()
    db_holders_hashes = {r.block_hash for r in db_holders}

    assert db_holders_hashes == fresh_holders_hashes


async def test_token_holders_cleaner_does_not_crashes_if_there_s_no_holders_for_pair(
        token_holders_cleaner: TokenHoldersCleaner,
        token_holder_factory: TokenHolderFactory,
        assets_summary_pair_factory: AssetsSummaryPairFactory,
        db: Engine,
) -> None:
    account = generate_address()
    token = generate_address()

    assets_summary_pair_factory.create(address=account, asset_address=token)

    await token_holders_cleaner.clean_next_batch(get_starting_pair(), 0)
