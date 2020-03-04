import itertools
from typing import List, Union

import pytest
import yarl
from aiohttp.test_utils import TestClient

from jsearch.api.tests.dex.conftest import DexLinkedEventsFactory
from jsearch.common.processing.dex_logs import DexEventType

pytestmark = [pytest.mark.asyncio, pytest.mark.api, pytest.mark.dex, pytest.mark.filtration]


async def test_filtration_by_asset(
        cli: TestClient,
        url: yarl.URL,
        token_address: str,
        dex_linked_events_factory: DexLinkedEventsFactory,
) -> None:
    # given

    # create events witch will filtered
    dex_linked_events_factory.add_order(tradedAsset='0xtrash')
    dex_linked_events_factory.add_trade()

    dex_linked_events_factory.add_trade_status(event_type=DexEventType.TRADE_CANCELLED),
    dex_linked_events_factory.add_trade_status(event_type=DexEventType.TRADE_COMPLETED),

    dex_linked_events_factory.add_order_status(event_type=DexEventType.ORDER_ACTIVATED),
    dex_linked_events_factory.add_order_status(event_type=DexEventType.ORDER_CANCELLED),
    dex_linked_events_factory.add_order_status(event_type=DexEventType.ORDER_COMPLETED),
    dex_linked_events_factory.add_order_status(event_type=DexEventType.ORDER_EXPIRED),

    # create expected events
    order = dex_linked_events_factory.add_order(tradedAsset=token_address)
    trade = dex_linked_events_factory.add_trade()
    states = [
        dex_linked_events_factory.add_trade_status(event_type=DexEventType.TRADE_CANCELLED),
        dex_linked_events_factory.add_trade_status(event_type=DexEventType.TRADE_COMPLETED),
        dex_linked_events_factory.add_order_status(event_type=DexEventType.ORDER_ACTIVATED),
        dex_linked_events_factory.add_order_status(event_type=DexEventType.ORDER_CANCELLED),
        dex_linked_events_factory.add_order_status(event_type=DexEventType.ORDER_COMPLETED),
        dex_linked_events_factory.add_order_status(event_type=DexEventType.ORDER_EXPIRED),
    ]

    expected_events = [order, trade, *states]
    expected_indexes = {item['event_index'] for item in expected_events}

    # when
    response = await cli.get(str(url))
    resp_json = await response.json()

    # then
    assert response.status == 200
    assert resp_json['status']['success']

    assert len(resp_json['data']) > 0
    assert {item['event_data']['event_index'] for item in resp_json['data']} == expected_indexes


@pytest.mark.parametrize(
    'event_type',
    [
        *DexEventType.ORDERS,
        *DexEventType.TRADE,
        *map(lambda x: ','.join(x), itertools.combinations([*DexEventType.ORDERS, *DexEventType.TRADE], 2)),
    ]
)
async def test_filtration_by_event_type(
        cli: TestClient,
        url: yarl.URL,
        token_address: str,
        dex_linked_events_factory: DexLinkedEventsFactory,
        event_type: Union[str, List[str]]
) -> None:
    event_types = event_type.split(',')

    # create events
    events = [
        dex_linked_events_factory.add_order(tradedAsset=token_address),
        dex_linked_events_factory.add_trade(),
        dex_linked_events_factory.add_trade_status(event_type=DexEventType.TRADE_CANCELLED),
        dex_linked_events_factory.add_trade_status(event_type=DexEventType.TRADE_COMPLETED),
        dex_linked_events_factory.add_order_status(event_type=DexEventType.ORDER_ACTIVATED),
        dex_linked_events_factory.add_order_status(event_type=DexEventType.ORDER_CANCELLED),
        dex_linked_events_factory.add_order_status(event_type=DexEventType.ORDER_COMPLETED),
        dex_linked_events_factory.add_order_status(event_type=DexEventType.ORDER_EXPIRED),
    ]
    indexes = {item['event_index'] for item in events if item['event_type'] in event_types}

    url = url.with_query({'event_type': ','.join(event_types)})

    # when
    response = await cli.get(str(url))
    resp_json = await response.json()

    # then
    assert response.status == 200
    assert resp_json['status']['success']

    assert len(resp_json['data']) > 0
    assert {item['event_data']['event_index'] for item in resp_json['data']} == indexes
