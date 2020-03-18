from typing import Callable

import pytest
import yarl
from aiohttp.test_utils import TestClient

pytestmark = [pytest.mark.asyncio, pytest.mark.api, pytest.mark.dex, pytest.mark.smoke]


async def test_smoke(
        cli: TestClient,
        url: yarl.URL,
        random_events: Callable[[int], None]
) -> None:
    # given
    random_events(21)

    # when
    response = await cli.get(str(url))

    # then
    data = await response.json()

    assert data['status']['success'] is True
    assert len(data['data']) == 20
