import itertools
from collections import defaultdict
from typing import List, Union

import pytest
import yarl
from aiohttp.test_utils import TestClient

from jsearch.api.tests.dex.conftest import DexLinkedEventsFactory
from jsearch.common.processing.dex_logs import DexEventType, ORDER_STATUSES, ORDER_EVENT_TYPE_TO_STATUS

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.api,
    pytest.mark.dex,
    pytest.mark.filtration
]


async def test_filtration_by_asset(
        cli: TestClient,
        url: yarl.URL,
        token_address: str,
        dex_linked_events_factory: DexLinkedEventsFactory,
) -> None:
    # given

    # create events witch will filtered
    dex_linked_events_factory.add_order(tradedAsset='0xtrash')

    # create expected events
    order = dex_linked_events_factory.add_order(tradedAsset=token_address)

    # when
    response = await cli.get(str(url))
    resp_json = await response.json()

    # then
    assert response.status == 200
    assert resp_json['status']['success']

    assert len(resp_json['data']) > 0
    assert {item['order_data']['order_id'] for item in resp_json['data']} == {order['orderID']}


async def test_filtration_by_order_creator(
        cli: TestClient,
        url: yarl.URL,
        token_address: str,
        order_creator: str,
        dex_linked_events_factory: DexLinkedEventsFactory,
) -> None:
    # given

    # create events witch will filtered
    dex_linked_events_factory.add_order(tradedAsset=token_address, orderCreator='0xtrash')

    # create expected events
    order = dex_linked_events_factory.add_order(tradedAsset=token_address, orderCreator=order_creator)

    url = url.with_query({'order_creator': order_creator})

    # when
    response = await cli.get(str(url))
    resp_json = await response.json()

    # then
    assert response.status == 200
    assert resp_json['status']['success']

    assert len(resp_json['data']) > 0
    assert {item['order_data']['order_id'] for item in resp_json['data']} == {order['orderID']}


@pytest.mark.parametrize(
    'order_status',
    [
        *ORDER_STATUSES.keys(),
        *map(lambda x: ','.join(x), itertools.combinations([*ORDER_STATUSES.keys()], 2)),
    ]
)
async def test_filtration_by_order_status(
        cli: TestClient,
        url: yarl.URL,
        token_address: str,
        dex_linked_events_factory: DexLinkedEventsFactory,
        order_status: Union[str, List[str]]
) -> None:
    event_types = order_status.split(',')
    orders_by_status = defaultdict(set)

    # create events
    for state, event_type in ORDER_STATUSES.items():
        # order + order state
        order = dex_linked_events_factory.add_order(tradedAsset=token_address)
        if event_type != DexEventType.ORDER_PLACED:
            dex_linked_events_factory.add_order_status(event_type=event_type)

        orders_by_status[state].add(order['orderID'])

    state = ORDER_EVENT_TYPE_TO_STATUS[DexEventType.ORDER_PLACED]

    # order + trade
    order = dex_linked_events_factory.add_order(tradedAsset=token_address)
    dex_linked_events_factory.add_trade()
    orders_by_status[state].add(order['orderID'])

    # order + trade + trade state
    order = dex_linked_events_factory.add_order(tradedAsset=token_address)
    dex_linked_events_factory.add_trade()
    dex_linked_events_factory.add_trade_status(event_type=DexEventType.TRADE_COMPLETED)
    orders_by_status[state].add(order['orderID'])

    # order + trade + trade state
    order = dex_linked_events_factory.add_order(tradedAsset=token_address)
    dex_linked_events_factory.add_trade()
    dex_linked_events_factory.add_trade_status(event_type=DexEventType.TRADE_CANCELLED)
    orders_by_status[state].add(order['orderID'])

    # order + order state + trade + trade state
    state = ORDER_EVENT_TYPE_TO_STATUS[DexEventType.ORDER_COMPLETED]
    order = dex_linked_events_factory.add_order(tradedAsset=token_address)
    dex_linked_events_factory.add_trade()
    dex_linked_events_factory.add_trade_status(event_type=DexEventType.TRADE_COMPLETED)
    dex_linked_events_factory.add_order_status(event_type=DexEventType.ORDER_COMPLETED)
    orders_by_status[state].add(order['orderID'])

    url = url.with_query({'order_status': ','.join(event_types)})

    # when
    response = await cli.get(str(url))
    resp_json = await response.json()

    expected = set()
    for status in order_status.split(','):
        expected |= orders_by_status[status]

    # then
    assert response.status == 200
    assert resp_json['status']['success']

    assert len(resp_json['data']) > 0
    assert {item['order_data']['order_id'] for item in resp_json['data']} == expected
