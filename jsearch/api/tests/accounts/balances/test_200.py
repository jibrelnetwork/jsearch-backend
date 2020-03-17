import pytest
import yarl
from aiohttp.test_utils import TestClient

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.api,
    pytest.mark.accounts,
    pytest.mark.smoke
]


async def test_smoke(
        cli: TestClient,
        url: yarl.URL,
) -> None:
    # when
    response = await cli.get(str(url))

    # then
    data = await response.json()

    assert response.status == 200
    assert data['status']['success'] is True
    assert len(data['data']) == 1
