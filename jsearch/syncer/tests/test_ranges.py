import pytest
from typing import List, NamedTuple, Optional

from jsearch.common.structs import BlockRange
from jsearch.tests.plugins.databases.factories.blocks import BlockFactory
from jsearch.tests.plugins.databases.factories.chain_events import ChainEventFactory

pytestmark = pytest.mark.asyncio


@pytest.fixture
def node_id():
    return '1'


@pytest.fixture
def sync_block(node_id, block_factory: BlockFactory, chain_events_factory: ChainEventFactory):
    def create_block(number: int, event_id: Optional[int] = None):
        block = block_factory.create(number=number)
        chain_events_factory.create_block(block, node_id=node_id, id=event_id if event_id is not None else number)

    return create_block


@pytest.fixture
def sync_block_range(sync_block):
    def create_blocks(start, end):
        for i in range(start, end):
            sync_block(number=i)

    return create_blocks


class RawDbEvent(NamedTuple):
    block_number: int
    id: int


class SyncHoleCase(NamedTuple):
    raw_db_events: List[RawDbEvent]
    sync_range: BlockRange
    synced_ranges: List[BlockRange]


class RawDBMock(NamedTuple):
    db_events: List[RawDbEvent]

    async def get_first_chain_event_for_block_range(self, block_range: BlockRange, *args, **kwargs):
        for event in self.db_events:
            if event.block_number in block_range:
                return event

    async def get_next_chain_event(self, block_range: BlockRange, last_id: int, *args, **kwargs):
        for event in self.db_events:
            if event.block_number in block_range and event.id > last_id:
                return event


@pytest.mark.parametrize(
    "case",
    (
            SyncHoleCase(
                raw_db_events=[RawDbEvent(i, i) for i in range(10, 21)],
                sync_range=BlockRange(10, 20),
                synced_ranges=[],
            ),
            SyncHoleCase(
                raw_db_events=[RawDbEvent(i, i) for i in range(10, 21)],
                sync_range=BlockRange(10, 20),
                synced_ranges=[BlockRange(12, 14)],
            ),
            SyncHoleCase(
                raw_db_events=[RawDbEvent(i, i) for i in range(10, 21)],
                sync_range=BlockRange(10, 20),
                synced_ranges=[BlockRange(10, 12)],
            ),
            SyncHoleCase(
                raw_db_events=[RawDbEvent(i, i) for i in range(10, 21)],
                sync_range=BlockRange(10, 20),
                synced_ranges=[BlockRange(18, 20)],
            ),
            SyncHoleCase(
                raw_db_events=[RawDbEvent(i, i) for i in range(10, 21)],
                sync_range=BlockRange(10, 20),
                synced_ranges=[BlockRange(12, 14), BlockRange(16, 18)],
            ),
            SyncHoleCase(
                raw_db_events=[RawDbEvent(i, i) for i in range(10, 21)],
                sync_range=BlockRange(10, 20),
                synced_ranges=[BlockRange(10, 14), BlockRange(16, 20)],
            ),
            SyncHoleCase(
                raw_db_events=[RawDbEvent(i, i) for i in range(10, 30)],
                sync_range=BlockRange(10, 30),
                synced_ranges=[
                    BlockRange(10, 14),
                    BlockRange(16, 20),
                    BlockRange(20, 24),
                    BlockRange(27, 29)
                ],
            ),
    )
)
async def test_fills_holes(
        main_db_wrapper,
        sync_block_range,
        sync_block,
        db,
        case,
        mocker,
        node_id,
) -> None:
    # given
    async def process_chain_event(self, event):
        blocks = {x.block_number for x in case.raw_db_events}
        if event.block_number in blocks:
            sync_block(event.block_number, event.id)

    mocker.patch("jsearch.syncer.manager.Manager.process_chain_event", process_chain_event)

    from jsearch.syncer.manager import Manager

    for range_start, range_end in case.synced_ranges:
        sync_block_range(range_start, range_end)

    manager = Manager(
        service=None,
        main_db=main_db_wrapper,
        raw_db=RawDBMock(case.raw_db_events),
        sync_range=case.sync_range
    )
    manager._running = True
    manager.node_id = node_id

    # when
    while manager._running:
        await manager.get_and_process_chain_event()

    # then

    # check - all block are synced
    numbers = db.execute("select number from blocks order by number").fetchall()
    for i, n in enumerate(range(*case.sync_range), ):
        assert n == numbers[i].number

    # check - there are no holes
    assert not await main_db_wrapper.check_on_holes(*case.sync_range)


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
                synced_ranges=[BlockRange(10, 12), BlockRange(21, 21)],
                gap=BlockRange(12, 20)
            ),
            FindHoleCase(
                sync_range=BlockRange(10, 20),
                synced_ranges=[BlockRange(12, 14), BlockRange(16, 18)],
                gap=BlockRange(10, 11)
            ),
            FindHoleCase(
                sync_range=BlockRange(10, 20),
                synced_ranges=[BlockRange(10, 12), BlockRange(14, 16)],
                gap=BlockRange(12, 13)
            ),
    )
)
async def test_find_holes(
        main_db_wrapper,
        sync_block_range,
        sync_block,
        case
):
    # given
    for range_start, range_end in case.synced_ranges:
        if range_start == range_end:
            sync_block(range_start)
        else:
            sync_block_range(range_start, range_end)

    # then
    gap = await main_db_wrapper.check_on_holes(case.sync_range.start, case.sync_range.end)

    # when
    assert gap == case.gap
