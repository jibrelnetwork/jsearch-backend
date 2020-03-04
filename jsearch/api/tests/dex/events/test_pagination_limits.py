from typing import Callable, Optional

import pytest
import yarl
from aiohttp.test_utils import TestClient

pytestmark = [pytest.mark.asyncio, pytest.mark.api, pytest.mark.dex, pytest.mark.limit]


@pytest.mark.parametrize(
    "target_limit, expected_items_count, expected_errors",
    (
            (None, 20, []),
            (19, 19, []),
            (20, 20, []),
            (21, None, [
                {
                    "field": "limit",
                    "message": "Must be between 1 and 20.",
                    "code": "INVALID_LIMIT_VALUE",
                }
            ]),
    ),
    ids=[
        "limit=None --- 20 rows returned",
        "limit=19   --- 19 rows returned",
        "limit=20   --- 20 rows returned",
        "limit=21   --- error is returned",
    ],
)
async def test_limits(
        cli: TestClient,
        url: yarl.URL,
        random_events: Callable[[int], None],
        target_limit: Optional[int],
        expected_items_count: Optional[int],
        expected_errors: Optional[int],
) -> None:
    # given
    random_events(30)

    # when
    params = {'block_number': 'latest'}

    if target_limit is not None:
        params['limit'] = target_limit

    url = url.with_query(params)

    # when
    response = await cli.get(str(url))
    resp_json = await response.json()

    # then
    observed_errors = resp_json['status']['errors']
    if resp_json['data'] is None:
        observed_items_count = None
    else:
        observed_items_count = len(resp_json['data'])

    assert (observed_errors, observed_items_count) == (expected_errors, expected_items_count)
