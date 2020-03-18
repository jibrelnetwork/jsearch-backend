from typing import Callable, Any, Dict, List

import pytest
import yarl
from aiohttp.test_utils import TestClient

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.api,
    pytest.mark.dex,
    pytest.mark.smoke
]


async def test_smoke(
        cli: TestClient,
        url: yarl.URL,
        user_address: str,
        random_assets_locks_and_unlocks: Callable[..., List[Dict[str, Any]]],
        get_total_locks: Callable[[List[Dict[str, Any]]], Dict[str, str]]
) -> None:
    # given
    events = random_assets_locks_and_unlocks(21, user=user_address)
    total_locks = get_total_locks(events)
    assert total_locks

    # when
    response = await cli.get(str(url))
    resp_json = await response.json()

    # then
    assert resp_json['status']['success'] is True
    assert len(resp_json['data']) == len(total_locks)
