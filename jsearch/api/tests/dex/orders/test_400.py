import pytest
import yarl
from aiohttp.test_utils import TestClient

from jsearch.api.tests.dex.conftest import DexLinkedEventsFactory
from jsearch.common.processing.dex_logs import ORDER_STATUSES

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.api,
    pytest.mark.dex,
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
    assert response.status == 400
    assert data['status']['success'] is False
    assert data['status']['errors'] == [
        {
            'code': 'INVALID_VALUE',
            'field': 'order_status',
            'message': f'Available filters: {choices}'
        }
    ]


async def test_not_existed_order_creator(
        cli: TestClient,
        url: yarl.URL,
        order_creator: str,
        token_address: str,
        dex_linked_events_factory: DexLinkedEventsFactory,
) -> None:
    dex_linked_events_factory.add_order(tradedAsset=token_address)
    url = url.with_query({'order_creator': order_creator})

    # when
    response = await cli.get(str(url))
    data = await response.json()

    # then
    assert response.status == 400
    assert data['status']['success'] is False
    assert data['status']['errors'] == [
        {
            'code': 'NOT_FOUND',
            'field': 'order_creator',
            'message': f'An order with {order_creator} as an order creator does not exist.'
        }
    ]
