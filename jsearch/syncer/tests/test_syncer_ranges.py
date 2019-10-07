import pytest
from typing import List

from jsearch.common.structs import SyncRange
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


@pytest.mark.parametrize(
    "start, end, synced_blocks",
    (
            (10, 20, []),
            (10, 20, [(12, 14), ]),
            (10, 20, [(10, 12), ]),
            (10, 20, [(18, 20), ]),
            (10, 20, [(12, 14), (16, 18)]),
    )
)
async def test_fills_holes(
        main_db,
        start: int,
        end: int,
        synced_blocks: List[SyncRange],
        sync_block_range,
        sync_block,
        db
) -> None:
    # given
    from jsearch.syncer.manager import get_range_and_check_holes
    for range_start, range_end in synced_blocks:
        sync_block_range(range_start, range_end)

    # when
    state = SyncerState(last_processed_block=0)

    while state.last_processed_block < end:
        range_start, range_end = await get_range_and_check_holes(main_db, start, end, state)

        event = await main_db.get_last_chain_event([range_start, range_end], node_id="1")
        number = event and event['block_number'] + 1 or start

        sync_block(number=number)
        state.update(number)

    # then
    numbers = db.execute("select number from blocks order by number").fetchall()
    for i, n in enumerate(range(start, end), ):
        assert n == numbers[i].number

    assert not await main_db.check_on_holes(start, end)


@pytest.mark.parametrize(
    "start, end, synced_blocks, expected",
    (
            (10, 20, [], None),
            (10, 20, [(12, 14)], 11),
            (10, 20, [(10, 12)], None),
            (10, 20, [(18, 20)], 17),
            (10, 20, [(12, 14), (16, 18)], 11),
            (10, 20, [(10, 12), (14, 16)], 13),
    )
)
async def test_find_holes(
        main_db,
        start: int,
        end: int,
        synced_blocks: List[SyncRange],
        expected: int,
        sync_block_range
):
    # given
    for range_start, range_end in synced_blocks:
        sync_block_range(range_start, range_end)

    # then
    right_hole_border = await main_db.check_on_holes(start, end)

    # when
    assert right_hole_border == expected
