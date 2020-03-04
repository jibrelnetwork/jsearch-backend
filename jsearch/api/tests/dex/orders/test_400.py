import pytest
import yarl
from aiohttp.test_utils import TestClient

from jsearch.common.processing.dex_logs import ORDER_STATUSES

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.api,
    pytest.mark.dex,
    pytest.mark.dex_orders,
    pytest.mark.negative
]


@pytest.mark.parametrize(
    'order_status',
    ['invalid', *map(lambda x: x.lower(), ORDER_STATUSES.keys())]
)
async def test_invalid_order_status(
        cli: TestClient,
        url: yarl.URL,
        order_status: str
) -> None:
    choices = ', '.join([*ORDER_STATUSES.keys()])
    url = url.with_query({'order_status': order_status})

    # when
    response = await cli.get(str(url))
    data = await response.json()

    # then
    assert data['status']['success'] is False
    assert data['status']['errors'] == [
        {
            'code': 'INVALID_VALUE',
            'field': 'order_status',
            'message': f'Available filters: {choices}'
        }
    ]
