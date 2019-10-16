import pytest
from typing import List, NamedTuple, Optional

from jsearch.common.structs import BlockRange
from jsearch.syncer.database import MainDB
from jsearch.syncer.state import SyncerState
from jsearch.tests.plugins.databases.factories.blocks import BlockFactory
from jsearch.tests.plugins.databases.factories.chain_events import ChainEventFactory

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def main_db(db_dsn):
    main_db = MainDB(db_dsn)
    await main_db.connect()

    try:
        yield main_db
    finally:
        await main_db.disconnect()


@pytest.fixture
def node_id():
    return '1'


@pytest.fixture
def sync_block(node_id, block_factory: BlockFactory, chain_events_factory: ChainEventFactory):
    def create_block(number: int):
        block = block_factory.create(number=number)
        chain_events_factory.create_block(block, node_id=node_id, id=number)

    return create_block


@pytest.fixture
def sync_block_range(sync_block):
    def create_blocks(start, end):
        for i in range(start, end):
            sync_block(number=i)

    return create_blocks


class SyncHoleCase(NamedTuple):
    sync_range: BlockRange
    synced_ranges: List[BlockRange]


@pytest.mark.parametrize(
    "case",
    (
            SyncHoleCase(
                sync_range=BlockRange(10, 20),
                synced_ranges=[],
            ),
            SyncHoleCase(
                sync_range=BlockRange(10, 20),
                synced_ranges=[BlockRange(12, 14)],
            ),
            SyncHoleCase(
                sync_range=BlockRange(10, 20),
                synced_ranges=[BlockRange(10, 12)],
            ),
            SyncHoleCase(
                sync_range=BlockRange(10, 20),
                synced_ranges=[BlockRange(18, 20)],
            ),
            SyncHoleCase(
                sync_range=BlockRange(10, 20),
                synced_ranges=[BlockRange(12, 14), BlockRange(16, 18)],
            ),
            SyncHoleCase(
                sync_range=BlockRange(10, 20),
                synced_ranges=[BlockRange(10, 14), BlockRange(16, 20)],
            ),
    )
)
async def test_fills_holes(
        main_db,
        sync_block_range,
        sync_block,
        db,
        case
) -> None:
    # given
    from jsearch.syncer.manager import get_range_and_check_holes
    for range_start, range_end in case.synced_ranges:
        sync_block_range(range_start, range_end)

    # when
    state = SyncerState(last_processed_block=0)

    block_range = case.sync_range
    while state.last_processed_block < case.sync_range.end:
        range_ = await get_range_and_check_holes(main_db, block_range, state)

        event = await main_db.get_last_chain_event(range_, node_id="1")
        number = event and event['block_number'] + 1 or case.sync_range.start

        sync_block(number=number)
        state.update(number)

    # then
    numbers = db.execute("select number from blocks order by number").fetchall()
    for i, n in enumerate(range(*case.sync_range), ):
        assert n == numbers[i].number

    assert not await main_db.check_on_holes(*case.sync_range)


class FindHoleCase(NamedTuple):
    sync_range: BlockRange
    synced_ranges: List[BlockRange]

    gap: Optional[BlockRange]


@pytest.mark.parametrize(
    "case",
    (
            FindHoleCase(
                sync_range=BlockRange(10, 20),
                synced_ranges=[],
                gap=None
            ),
            FindHoleCase(
                sync_range=BlockRange(10, 20),
                synced_ranges=[BlockRange(12, 14)],
                gap=BlockRange(10, 11)
            ),
            FindHoleCase(
                sync_range=BlockRange(10, 20),
                synced_ranges=[BlockRange(10, 12)],
                gap=None
            ),
            FindHoleCase(
                sync_range=BlockRange(10, 20),
                synced_ranges=[BlockRange(18, 20)],
                gap=BlockRange(10, 17)
            ),
            FindHoleCase(
                sync_range=BlockRange(10, 20),
                synced_ranges=[BlockRange(12, 14), BlockRange(16, 18)],
                gap=BlockRange(10, 11)
            ),
            FindHoleCase(
                sync_range=BlockRange(10, 20),
                synced_ranges=[BlockRange(10, 12), BlockRange(14, 16)],
                gap=BlockRange(11, 13)
            ),
    )
)
async def test_find_holes(
        main_db,
        sync_block_range,
        case
):
    # given
    for range_start, range_end in case.synced_ranges:
        sync_block_range(range_start, range_end)

    # then
    gap = await main_db.check_on_holes(case.sync_range.start, case.sync_range.end)

    # when
    assert gap == case.gap
