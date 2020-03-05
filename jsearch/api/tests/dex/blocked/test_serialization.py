from typing import Callable, Any, Dict, List

import pytest
import yarl
from aiohttp.test_utils import TestClient

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.api,
    pytest.mark.dex,
    pytest.mark.serialization
]


async def test_serialization(
        cli: TestClient,
        url: yarl.URL,
        user_address: str,
        token_address: str,
        random_assets_locks_and_unlocks: Callable[..., List[Dict[str, Any]]],
        get_total_locks: Callable[[List[Dict[str, Any]]], Dict[str, str]]
) -> None:
    # given
    events = random_assets_locks_and_unlocks(21, user=user_address, tokens=[token_address])
    total_locks = get_total_locks(events)
    assert total_locks

    # when
    response = await cli.get(str(url))

    # then
    data = await response.json()

    assert data['status']['success'] is True
    assert data['data'] == [
        {
            'asset_address': token_address,
            'blocked_amount': f'{sum(map(int, total_locks.values()), 0)}'
        }
    ]
