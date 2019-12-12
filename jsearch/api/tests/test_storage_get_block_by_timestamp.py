from typing import NamedTuple

import pytest

from jsearch.api.database_queries.blocks import get_block_number_by_timestamp_query
from jsearch.api.storage import Storage
from jsearch.tests.plugins.databases.factories.blocks import BlockFactory


class ClosestBlockCase(NamedTuple):
    timestamp_delta: int
    order_direction: str
    is_block_expected: bool


@pytest.mark.parametrize(
    'timestamp_delta, order_direction, is_block_expected',
    (
        ClosestBlockCase(timestamp_delta=2, order_direction='desc', is_block_expected=True),
        ClosestBlockCase(timestamp_delta=2, order_direction='asc', is_block_expected=False),
        ClosestBlockCase(timestamp_delta=-2, order_direction='desc', is_block_expected=False),
        ClosestBlockCase(timestamp_delta=-2, order_direction='asc', is_block_expected=True),
        ClosestBlockCase(timestamp_delta=0, order_direction='desc', is_block_expected=True),
        ClosestBlockCase(timestamp_delta=0, order_direction='asc', is_block_expected=True),
    ),
    ids=[
        'timestamp is bigger + descending = block is returned',
        'timestamp is bigger + ascending = block is not returned',
        'timestamp is smaller + descending = block is not returned',
        'timestamp is smaller + ascending = block is returned',
        'timestamp is exact + ascending = block is returned',
        'timestamp is exact + descending = block is returned',
    ]
)
async def test_get_block_by_timestamp_selects_closest_block(
        block_factory: BlockFactory,
        storage: Storage,
        timestamp_delta: int,
        order_direction: str,
        is_block_expected: bool,
) -> None:
    block = block_factory.create()
    block_info = await storage.get_block_by_timestamp(
        timestamp=block.timestamp + timestamp_delta,
        order_direction=order_direction,
    )

    assert bool(block_info) is is_block_expected


@pytest.mark.parametrize('order_direction', ('desc', 'asc'))
async def test_get_block_by_timestamp_does_not_return_anything_if_theres_no_blocks(
        block_factory: BlockFactory,
        storage: Storage,
        order_direction: str,
) -> None:
    assert await storage.get_block_by_timestamp(1575289040, order_direction) is None


@pytest.mark.parametrize('order_direction', ('desc', 'asc'))
async def test_get_block_by_timestamp_does_not_return_block_from_fork(
        block_factory: BlockFactory,
        storage: Storage,
        order_direction: str,
) -> None:
    block = block_factory.create(is_forked=True)
    block_info = await storage.get_block_by_timestamp(timestamp=block.timestamp, order_direction=order_direction)

    assert block_info is None


@pytest.mark.parametrize('order_direction', ('desc', 'asc'))
async def test_get_block_number_by_timestamp_query_limits_query_by_one(
        block_factory: BlockFactory,
        storage: Storage,
        order_direction: str,
) -> None:
    query = get_block_number_by_timestamp_query(1575289040, order_direction)
    assert query._limit == 1
