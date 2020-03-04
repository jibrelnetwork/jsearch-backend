import pytest
import yarl
from aiohttp.test_utils import TestClient

from jsearch.api.tests.dex.conftest import DexLinkedEventsFactory
from jsearch.common.processing.dex_logs import DexEventType, ORDER_EVENT_TYPE_TO_STATUS

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.api,
    pytest.mark.dex,
    pytest.mark.dex_orders,
    pytest.mark.serialization
]


async def test_serialization(
        cli: TestClient,
        url: yarl.URL,
        token_address: str,
        order_creator: str,
        dex_linked_events_factory: DexLinkedEventsFactory,
) -> None:
    # given
    order = dex_linked_events_factory.add_order(tradedAsset=token_address, orderCreator=order_creator)
    trade = dex_linked_events_factory.add_trade()
    dex_linked_events_factory.add_trade_status(event_type=DexEventType.TRADE_COMPLETED)
    order_state = dex_linked_events_factory.add_order_status(event_type=DexEventType.ORDER_COMPLETED)

    # when
    response = await cli.get(str(url))

    # then
    data = await response.json()

    assert data['status']['success'] is True
    assert data['data'] == [
        {
            "order_data": {
                "order_creator": order_creator,
                "order_creation_timestamp": order['timestamp'],
                "order_id": order['orderID'],
                "order_type": order['orderType'],
                "traded_asset": order['tradedAsset'],
                "traded_asset_amount": str(order['tradedAmount']),
                "fiat_asset": order['fiatAsset'],
                "fiat_price": str(order['assetPrice']),
                "expiration_timestamp": order['expirationTimestamp']
            },
            "order_status": {
                "order_status": ORDER_EVENT_TYPE_TO_STATUS[DexEventType.ORDER_COMPLETED],
                "order_status_block_number": order_state['block_number'],
                "remaining_traded_asset_amount": str(order['tradedAmount'] - trade['tradedAmount'])
            }
        }
    ]
