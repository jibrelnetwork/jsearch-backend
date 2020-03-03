import pytest
import yarl
from aiohttp.test_utils import TestClient

from jsearch.common.processing.dex_logs import DexEventType

pytestmark = [pytest.mark.asyncio, pytest.mark.api, pytest.mark.dex, pytest.mark.negative]


async def test_asset_does_not_exists(
        cli: TestClient,
        url: yarl.URL,
        token_address: str,
) -> None:
    # when
    response = await cli.get(str(url))
    data = await response.json()

    # then
    assert data['status']['success'] is False
    assert data['status']['errors'] == [
        {
            'code': 'NOT_FOUND',
            'message': f'Asset {token_address} was not found'
        }
    ]


@pytest.mark.parametrize(
    'event_type',
    ['invalid', *map(lambda x: x.lower(), DexEventType.ALL)]
)
async def test_invalid_type(
        cli: TestClient,
        url: yarl.URL,
        token_address: str,
        event_type: str
) -> None:
    choices = ', '.join([*DexEventType.ORDERS, *DexEventType.TRADE])
    url = url.with_query({'event_type': event_type})

    # when
    response = await cli.get(str(url))
    data = await response.json()

    # then
    assert data['status']['success'] is False
    assert data['status']['errors'] == [
        {
            'code': 'INVALID_VALUE',
            'field': 'event_type',
            'message': f'Available filters: {choices}'
        }
    ]
