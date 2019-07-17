import asyncio

import pytest

from jsearch.common.async_utils import chain_dependent_coros


pytestmark = pytest.mark.asyncio


async def test_chain_dependent_coros_preserves_order_of_dependent_coros() -> None:
    coros_result = []
    sequence = [
        {'id': 'one', 'val': 1},
        {'id': 'one', 'val': 2},
        {'id': 'one', 'val': 3},
        {'id': 'one', 'val': 4},
        {'id': 'one', 'val': 5},
        {'id': 'one', 'val': 6},
        {'id': 'two', 'val': 7},
        {'id': 'two', 'val': 8},
        {'id': 'one', 'val': 9},
        {'id': 'one', 'val': 10},
        {'id': 'three', 'val': 11},
        {'id': 'two', 'val': 12},
        {'id': 'two', 'val': 13},
        {'id': 'three', 'val': 14},
        {'id': 'one', 'val': 15},
        {'id': 'two', 'val': 16},
        {'id': 'one', 'val': 17},
        {'id': 'three', 'val': 18},
        {'id': 'one', 'val': 19},
        {'id': 'one', 'val': 10},
    ]

    async def create_task(item):
        coros_result.append(item)

    coros = chain_dependent_coros(sequence, 'id', create_task)

    await asyncio.gather(*coros)

    coros_result_of_one = [c for c in coros_result if c['id'] == 'one']
    coros_result_of_two = [c for c in coros_result if c['id'] == 'two']
    coros_result_of_three = [c for c in coros_result if c['id'] == 'three']

    assert coros_result_of_one == [
        {'id': 'one', 'val': 1},
        {'id': 'one', 'val': 2},
        {'id': 'one', 'val': 3},
        {'id': 'one', 'val': 4},
        {'id': 'one', 'val': 5},
        {'id': 'one', 'val': 6},
        {'id': 'one', 'val': 9},
        {'id': 'one', 'val': 10},
        {'id': 'one', 'val': 15},
        {'id': 'one', 'val': 17},
        {'id': 'one', 'val': 19},
        {'id': 'one', 'val': 10},
    ]

    assert coros_result_of_two == [
        {'id': 'two', 'val': 7},
        {'id': 'two', 'val': 8},
        {'id': 'two', 'val': 12},
        {'id': 'two', 'val': 13},
        {'id': 'two', 'val': 16},
    ]

    assert coros_result_of_three == [
        {'id': 'three', 'val': 11},
        {'id': 'three', 'val': 14},
        {'id': 'three', 'val': 18},
    ]
