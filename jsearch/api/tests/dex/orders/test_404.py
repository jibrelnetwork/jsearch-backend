import pytest
import yarl
from aiohttp.test_utils import TestClient

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.api,
    pytest.mark.dex,
    pytest.mark.negative
]


async def test_asset_does_not_exist(
        cli: TestClient,
        url: yarl.URL,
        token_address: str
) -> None:
    # when
    response = await cli.get(str(url))
    data = await response.json()

    # then
    assert response.status == 404
    assert data['status']['success'] is False
    assert data['status']['errors'] == [
        {
            'code': 'NOT_FOUND',
            'message': f'Asset {token_address} was not found'
        }
    ]
