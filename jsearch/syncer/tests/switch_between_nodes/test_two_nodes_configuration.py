from contextlib import contextmanager
from dataclasses import dataclass

import pytest
from aiopg.sa import Engine
from typing import Callable, Any, Optional, Union

from jsearch.common.structs import BlockRange
from jsearch.tests.plugins.databases.factories.raw.chain_events import ChainGenerator, ChainPointer, Chain

pytestmark = pytest.mark.asyncio


@dataclass
class NodeDescription:
    id: str
    blocks: int = 0

    start_number: int = 0
    start_hash: Optional[Union[str, ChainPointer]] = None


@dataclass
class SwitchCase:
    label: str

    left: NodeDescription
    right: NodeDescription

    blocks: Optional[BlockRange] = None
    forked: Optional[BlockRange] = None

    synced_range: Optional[BlockRange] = None

    left_id: str = '0xleft'
    right_id: str = '0xright'

    should_switch: bool = True
    exception: Exception = None

    def get_right_start_hash(self, left: Chain):
        if isinstance(self.right.start_hash, ChainPointer):
            return self.right.start_hash.get_hash(left)
        if self.right.start_hash:
            return self.right.start_hash
        return left.last.hash

    def __str__(self):
        return self.label


@pytest.fixture(autouse=True)
def disable_switch_timeout(override_settings: Callable[[str, Any], None]):
    override_settings('ETH_NODE_SWITCH_TIMEOUT', 0.1)


cases = [
    SwitchCase(
        label='right node is in canonical chain',
        blocks=BlockRange(1, 20),
        left=NodeDescription(id='0xleft', blocks=10, start_number=1),
        right=NodeDescription(id='0xright', blocks=10, start_number=11),
        synced_range=BlockRange(1, 20)
    ),
    SwitchCase(
        label='right node is in fork chain',
        blocks=BlockRange(1, 20),
        forked=BlockRange(6, 10),
        left=NodeDescription(id='0xleft', blocks=10, start_number=1),
        right=NodeDescription(id='0xright', blocks=15, start_number=6, start_hash=ChainPointer(offset=6)),
        synced_range=BlockRange(1, 20)
    ),
    SwitchCase(
        label='right has no events',
        blocks=BlockRange(1, 20),
        left=NodeDescription(id='0xleft', blocks=10, start_number=1),
        right=NodeDescription(id='0xright', blocks=0, start_number=10),
        synced_range=BlockRange(1, 20)
    ),
    SwitchCase(
        label='there is not common blocks',
        blocks=BlockRange(1, 20),
        left=NodeDescription(id='0xleft', blocks=10, start_number=1),
        right=NodeDescription(id='0xfork', blocks=10, start_number=10, start_hash='0xfork'),
        should_switch=False,
        synced_range=BlockRange(1, 20),
        exception=ValueError(f'No common block between 0xleft and 0xright')
    ),
    SwitchCase(
        label='a gap between nodes',
        blocks=BlockRange(1, 10),
        left=NodeDescription(id='0xleft', blocks=10, start_number=1),
        right=NodeDescription(id='0xright', blocks=10, start_number=12),
        should_switch=False,
        synced_range=BlockRange(1, 20),
    ),
]


def generate_chains(chain_generator: ChainGenerator, given: SwitchCase, raw_db: Engine) -> None:
    from jsearch.common.logs import configure
    configure('DEBUG', 'pythonjsonlogger.jsonlogger.JsonFormatter')

    # given
    left = chain_generator(node_id=given.left_id)
    right = chain_generator(node_id=given.right_id)

    left.create_chain(
        given.left.blocks,
        start_from=given.left.start_number,
        start_from_hash=given.left.start_hash
    )
    left.collector.flush(raw_db)

    right.create_chain(
        given.right.blocks,
        start_from=given.right.start_number,
        start_from_hash=given.get_right_start_hash(left.chain)
    )
    right.collector.flush(raw_db)


@contextmanager
def catch_on_condition(exception: Optional[Exception]):
    context = None
    if exception:
        context = pytest.raises(type(exception)).__enter__()

    yield

    if context:
        context.__exit__()


@pytest.mark.parametrize(
    "given",
    cases,
    ids=[*map(str, cases)]
)
async def test_switch_events_chain(
        db: Engine,
        raw_db,
        sync: Callable[[BlockRange, str], None],
        chain_generator: ChainGenerator,
        given: SwitchCase
):
    # given
    generate_chains(chain_generator, given, raw_db)

    # when
    try:
        await sync(given.synced_range, node_id=given.left_id)
    except Exception as e:
        if not given.exception:
            raise

        assert isinstance(e, type(given.exception))
        assert e.args == given.exception.args

    # then

    # check blocks are in main db
    blocks = db.execute(f'select number from blocks where is_forked = false').fetchall()
    blocks_numbers = [block['number'] for block in blocks]
    assert blocks_numbers == [] if given.blocks is None else given.blocks.as_range()

    # check forked blocks are in main db
    forked_blocks = db.execute(f'select number from blocks where is_forked = true;').fetchall()
    blocks_numbers = [block['number'] for block in forked_blocks]
    assert blocks_numbers == [] if given.forked is None else given.forked.as_range()

    # check
    results = db.execute(
        f'select count(block_hash) as blocks, node_id from chain_events group by node_id;'
    ).fetchall()
    results = {item['node_id']: item['blocks'] for item in results}
    assert results.get(given.left_id, 0) == given.left.blocks
    if given.should_switch:
        assert results.get(given.right_id, 0) == given.right.blocks
