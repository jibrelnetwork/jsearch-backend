import pytest
import yarl
from aiohttp.test_utils import TestClient

pytestmark = [pytest.mark.asyncio, pytest.mark.api, pytest.mark.dex, pytest.mark.empty_db]


async def test_empty_db(cli: TestClient, url: yarl.URL) -> None:
    # when
    response = await cli.get(str(url))

    # then
    data = await response.json()

    assert data['status']['success'] is True
    assert data['data'] == []
