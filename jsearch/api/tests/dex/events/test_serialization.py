import pytest
import yarl
from aiohttp.test_utils import TestClient

from jsearch.api.tests.dex.conftest import DexLinkedEventsFactory
from jsearch.common.processing.dex_logs import DexEventType

pytestmark = [pytest.mark.asyncio, pytest.mark.api, pytest.mark.dex, pytest.mark.serialization]


async def test_serialization_order_placed_serialization(
        cli: TestClient,
        url: yarl.URL,
        token_address: str,
        dex_linked_events_factory: DexLinkedEventsFactory
) -> None:
    # given
    order = dex_linked_events_factory.add_order(tradedAsset=token_address)

    # when
    response = await cli.get(str(url))

    # then
    data = await response.json()

    assert data['status']['success'] is True
    assert data['data'] == [
        {
            'event_data': {
                'event_block_number': order['block_number'],
                'event_timestamp': order['timestamp'],
                'event_type': 'OrderPlacedEvent',
                'event_index': order['event_index']
            },
            'order_data': {
                'expiration_timestamp': order['expirationTimestamp'],
                'fiat_asset': order['fiatAsset'],
                'fiat_price': str(order['assetPrice']),
                'order_creation_timestamp': order['timestamp'],
                'order_creator': order['orderCreator'],
                'order_id': order['orderID'],
                'order_type': order['orderType'],
                'traded_asset': order['tradedAsset'],
                'traded_asset_amount': str(order['tradedAmount'])
            }
        }
    ]


@pytest.mark.parametrize(
    'event_type',
    [
        DexEventType.ORDER_CANCELLED,
        DexEventType.ORDER_ACTIVATED,
        DexEventType.ORDER_COMPLETED,
        DexEventType.ORDER_EXPIRED,
    ]
)
async def test_serialization_order_statuses(
        cli: TestClient,
        url: yarl.URL,
        token_address: str,
        dex_linked_events_factory: DexLinkedEventsFactory,
        event_type: str
) -> None:
    # given
    order = dex_linked_events_factory.add_order(tradedAsset=token_address)
    order_state = dex_linked_events_factory.add_order_status(event_type=event_type)

    # when
    response = await cli.get(str(url))

    # then
    data = await response.json()

    assert data['status']['success'] is True
    assert data['data'] == [
        {
            'event_data': {
                'event_block_number': order['block_number'],
                'event_timestamp': order['timestamp'],
                'event_type': 'OrderPlacedEvent',
                'event_index': order['event_index']
            },
            'order_data': {
                'expiration_timestamp': order['expirationTimestamp'],
                'fiat_asset': order['fiatAsset'],
                'fiat_price': str(order['assetPrice']),
                'order_creation_timestamp': order['timestamp'],
                'order_creator': order['orderCreator'],
                'order_id': order['orderID'],
                'order_type': order['orderType'],
                'traded_asset': order['tradedAsset'],
                'traded_asset_amount': str(order['tradedAmount'])
            }
        },
        {
            'event_data': {
                'event_block_number': order['block_number'],
                'event_timestamp': order['timestamp'],
                'event_type': event_type,
                'event_index': order_state['event_index']
            },
            'order_data': {
                'expiration_timestamp': order['expirationTimestamp'],
                'fiat_asset': order['fiatAsset'],
                'fiat_price': str(order['assetPrice']),
                'order_creation_timestamp': order['timestamp'],
                'order_creator': order['orderCreator'],
                'order_id': order['orderID'],
                'order_type': order['orderType'],
                'traded_asset': order['tradedAsset'],
                'traded_asset_amount': str(order['tradedAmount'])
            }
        }
    ]


async def test_serialization_trade(
        cli: TestClient,
        url: yarl.URL,
        token_address: str,
        dex_linked_events_factory: DexLinkedEventsFactory,
) -> None:
    # given
    order = dex_linked_events_factory.add_order(tradedAsset=token_address)
    trade = dex_linked_events_factory.add_trade()

    # when
    response = await cli.get(str(url))

    # then
    data = await response.json()

    assert data['status']['success'] is True
    assert data['data'] == [
        {
            'event_data': {
                'event_block_number': order['block_number'],
                'event_timestamp': order['timestamp'],
                'event_type': 'OrderPlacedEvent',
                'event_index': order['event_index'],
            },
            'order_data': {
                'expiration_timestamp': order['expirationTimestamp'],
                'fiat_asset': order['fiatAsset'],
                'fiat_price': str(order['assetPrice']),
                'order_creation_timestamp': order['timestamp'],
                'order_creator': order['orderCreator'],
                'order_id': order['orderID'],
                'order_type': order['orderType'],
                'traded_asset': order['tradedAsset'],
                'traded_asset_amount': str(order['tradedAmount'])
            }
        },
        {
            'event_data': {
                'event_block_number': order['block_number'],
                'event_timestamp': order['timestamp'],
                'event_type': DexEventType.TRADE_PLACED,
                'event_index': trade['event_index']
            },
            'order_data': {
                'expiration_timestamp': order['expirationTimestamp'],
                'fiat_asset': order['fiatAsset'],
                'fiat_price': str(order['assetPrice']),
                'order_creation_timestamp': order['timestamp'],
                'order_creator': order['orderCreator'],
                'order_id': order['orderID'],
                'order_type': order['orderType'],
                'traded_asset': order['tradedAsset'],
                'traded_asset_amount': str(order['tradedAmount'])
            },
            'trade_data': {
                'trade_id': str(trade['tradeID']),
                'trade_amount': str(trade['tradedAmount']),
                'trade_creation_timestamp': trade['timestamp'],
                'trade_creator': trade['tradeCreator']
            }
        }
    ]


@pytest.mark.parametrize(
    'event_type',
    [
        DexEventType.TRADE_COMPLETED,
        DexEventType.TRADE_CANCELLED,
    ]
)
async def test_serialization_trade_statuses(
        cli: TestClient,
        url: yarl.URL,
        token_address: str,
        dex_linked_events_factory: DexLinkedEventsFactory,
        event_type: str
) -> None:
    # given
    order = dex_linked_events_factory.add_order(tradedAsset=token_address)
    trade = dex_linked_events_factory.add_trade()
    trade_state = dex_linked_events_factory.add_trade_status(event_type=event_type)

    # when
    response = await cli.get(str(url))

    # then
    data = await response.json()

    assert data['status']['success'] is True
    assert data['data'] == [
        {
            'event_data': {
                'event_block_number': order['block_number'],
                'event_timestamp': order['timestamp'],
                'event_type': 'OrderPlacedEvent',
                'event_index': order['event_index'],
            },
            'order_data': {
                'expiration_timestamp': order['expirationTimestamp'],
                'fiat_asset': order['fiatAsset'],
                'fiat_price': str(order['assetPrice']),
                'order_creation_timestamp': order['timestamp'],
                'order_creator': order['orderCreator'],
                'order_id': order['orderID'],
                'order_type': order['orderType'],
                'traded_asset': order['tradedAsset'],
                'traded_asset_amount': str(order['tradedAmount'])
            }
        },
        {
            'event_data': {
                'event_block_number': trade['block_number'],
                'event_timestamp': trade['timestamp'],
                'event_type': DexEventType.TRADE_PLACED,
                'event_index': trade['event_index']
            },
            'order_data': {
                'expiration_timestamp': order['expirationTimestamp'],
                'fiat_asset': order['fiatAsset'],
                'fiat_price': str(order['assetPrice']),
                'order_creation_timestamp': order['timestamp'],
                'order_creator': order['orderCreator'],
                'order_id': order['orderID'],
                'order_type': order['orderType'],
                'traded_asset': order['tradedAsset'],
                'traded_asset_amount': str(order['tradedAmount'])
            },
            'trade_data': {
                'trade_id': str(trade['tradeID']),
                'trade_amount': str(trade['tradedAmount']),
                'trade_creation_timestamp': trade['timestamp'],
                'trade_creator': trade['tradeCreator']
            }
        },
        {
            'event_data': {
                'event_block_number': trade_state['block_number'],
                'event_timestamp': trade_state['timestamp'],
                'event_type': event_type,
                'event_index': trade_state['event_index']
            },
            'order_data': {
                'expiration_timestamp': order['expirationTimestamp'],
                'fiat_asset': order['fiatAsset'],
                'fiat_price': str(order['assetPrice']),
                'order_creation_timestamp': order['timestamp'],
                'order_creator': order['orderCreator'],
                'order_id': order['orderID'],
                'order_type': order['orderType'],
                'traded_asset': order['tradedAsset'],
                'traded_asset_amount': str(order['tradedAmount'])
            },
            'trade_data': {
                'trade_id': str(trade['tradeID']),
                'trade_amount': str(trade['tradedAmount']),
                'trade_creation_timestamp': trade['timestamp'],
                'trade_creator': trade['tradeCreator']
            }
        }
    ]
