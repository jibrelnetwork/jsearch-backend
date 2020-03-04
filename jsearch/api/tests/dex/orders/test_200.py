from typing import Callable, Any, Dict, List

import pytest
import yarl
from aiohttp.test_utils import TestClient

from jsearch.common.processing.dex_logs import DexEventType

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.api,
    pytest.mark.dex,
    pytest.mark.smoke
]


async def test_smoke(
        cli: TestClient,
        url: yarl.URL,
        random_events: Callable[[int], List[Dict[str, Any]]]
) -> None:
    # given
    events = random_events(21)
    orders = [event for event in events if event['event_type'] == DexEventType.ORDER_PLACED]
    assert orders

    # when
    response = await cli.get(str(url))

    # then
    data = await response.json()

    assert data['status']['success'] is True
    assert len(data['data']) == len(orders)
