from aiopg.sa import Engine

from jsearch.common.check_canonical_chain import get_canonical_holes
from jsearch.common.structs import Block
from jsearch.tests.plugins.databases.factories.blocks import BlockFactory


async def test_get_canonical_holes_retrieves_hole_because_of_missed_block(
        sa_engine: Engine,
        block_factory: BlockFactory,
) -> None:
    block_factory.create(number=1, is_forked=False, hash='0x1', parent_hash=None)
    block_factory.create(number=2, is_forked=False, hash='0x2', parent_hash='0x1')
    block_factory.create(number=4, is_forked=False, hash='0x4', parent_hash='0x3')

    observed_holes = await get_canonical_holes(sa_engine, depth=3)
    expected_holes = [
        Block(
            number=4,
            hash='0x4',
            parent_hash='0x3',
        ),
    ]

    assert observed_holes == expected_holes


async def test_get_canonical_holes_retrieves_hole_because_of_forked_block(
        sa_engine: Engine,
        block_factory: BlockFactory,
) -> None:
    block_factory.create(number=1, is_forked=False, hash='0x1', parent_hash=None)
    block_factory.create(number=2, is_forked=True, hash='0x2', parent_hash='0x1')
    block_factory.create(number=3, is_forked=False, hash='0x3', parent_hash='0x2')

    observed_holes = await get_canonical_holes(sa_engine, depth=3)
    expected_holes = [
        Block(
            number=3,
            hash='0x3',
            parent_hash='0x2',
        ),
    ]

    assert observed_holes == expected_holes
