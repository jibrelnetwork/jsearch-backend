from typing import Callable, Dict, List, Any, Optional

import pytest
import yarl
from aiohttp.test_utils import TestClient

pytestmark = [pytest.mark.asyncio, pytest.mark.api, pytest.mark.dex, pytest.mark.ordering]


@pytest.mark.parametrize(
    "order, reverse",
    [
        ('desc', True),
        ('asc', False),
        (None, True)
    ],
    ids=['desc', 'asc', 'default']
)
async def test_pagination_order(
        cli: TestClient,
        url: yarl.URL,
        random_events: Callable[[int], List[Dict[str, Any]]],
        order: Optional[str],
        reverse: bool
) -> None:
    # given
    events = random_events(30)
    if order:
        url = url.with_query({'order': order})

    # when
    response = await cli.get(str(url))
    resp_json = await response.json()

    # then
    assert response.status == 200
    assert resp_json['status']['success']

    expected_indexes = [item['event_index'] for item in events]
    if reverse:
        expected_indexes = expected_indexes[::-1]

    indexes = [item['event_data']['event_index'] for item in resp_json['data']]

    assert expected_indexes[:20] == indexes
