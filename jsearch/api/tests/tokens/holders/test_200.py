import pytest
import yarl
from aiohttp.test_utils import TestClient

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.api,
    pytest.mark.token_holders,
    pytest.mark.smoke
]


async def test_get_token_holders_by_big_value(
        cli: TestClient,
        url: yarl.URL,
):
    # when
    resp = await cli.get(str(url))

    # then
    assert resp.status == 200
