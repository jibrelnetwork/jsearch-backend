from collections import defaultdict
from dataclasses import dataclass
from functools import partial
from itertools import groupby, chain
from typing import DefaultDict, Tuple, NamedTuple, Union
from typing import List, Optional, Dict, Any

from jsearch.api.database_queries.dex_logs import (
    get_dex_orders_query,
    get_dex_orders_events_query,
    get_dex_trades_query,
    get_dex_trades_events_query,
    get_dex_events_query,
    get_dex_blocked_query
)
from jsearch.api.ordering import Ordering
from jsearch.common.db import DbActionsMixin
from jsearch.common.processing.dex_logs import DexEventType, ORDER_STATUSES, ORDER_EVENT_TYPE_TO_STATUS
from jsearch.common.tables import dex_logs_t


class OrderStatusInfo(NamedTuple):
    order_status: str
    order_status_block_number: int
    remaining_traded_asset_amount: str


class OrderDataInfo(NamedTuple):
    order_creator: str
    order_creation_timestamp: int
    order_id: int
    order_type: str
    traded_asset: str
    traded_asset_amount: str
    fiat_asset: str
    fiat_price: str
    expiration_timestamp: int


class OrderInfo(NamedTuple):
    order_data: OrderDataInfo
    order_status: OrderStatusInfo

    def as_dict(self):
        return {
            'order_data': self.order_data._asdict(),
            'order_status': self.order_status._asdict(),
        }


class OrderEvent(NamedTuple):
    order_id: int
    order_creator: str
    order_creation_timestamp: int
    order_type: str
    traded_asset: str
    traded_asset_amount: str
    fiat_asset: str
    fiat_price: str
    expiration_timestamp: int


class TradeEvent(NamedTuple):
    trade_id: str
    trade_amount: str
    trade_creator: str
    trade_creation_timestamp: int


class EventDescription(NamedTuple):
    event_type: str
    event_block_number: int
    event_timestamp: int
    event_index: int


def get_event_value(event: Dict[str, Any], key: str) -> Any:
    return event['event_data'][key]


get_trade_id = partial(get_event_value, key='tradeID')
get_order_id = partial(get_event_value, key='orderID')
get_traded_amount = partial(get_event_value, key='tradedAmount')
get_asset_address = partial(get_event_value, key='assetAddress')


def get_last_states(
        orders: List[Dict[str, Any]],
        status_change_events: List[Dict[str, Any]],
        id_key: str
) -> Dict[str, Any]:
    states = {}
    events = sorted(chain(orders, status_change_events), key=lambda x: (get_event_value(x, id_key), x['timestamp']))
    for order_id, order_states in groupby(events, key=lambda x: get_event_value(x, id_key)):
        last_state: Dict[str, Any] = list(order_states)[-1]
        states[order_id] = last_state
    return states


get_last_order_states = partial(get_last_states, id_key='orderID')
get_last_trade_states = partial(get_last_states, id_key='tradeID')


@dataclass
class HistoryEvent:
    event_data: EventDescription
    order_data: OrderEvent
    trade_data: Optional[TradeEvent] = None

    def __getitem__(self, item) -> Optional[Union[str, int]]:
        if item == 'block_number':
            return str(self.event_data.event_block_number)

        if item == 'timestamp':
            return self.event_data.event_timestamp

        if item == 'event_index':
            return self.event_data.event_index

        return None

    def as_dict(self):
        data = {
            'event_data': self.event_data._asdict(),
            'order_data': self.order_data._asdict()
        }
        if self.trade_data:
            data['trade_data'] = self.trade_data._asdict()

        return data


class BlockedAssetAmount(NamedTuple):
    asset_address: str
    blocked_amount: str


class DexStorage(DbActionsMixin):
    async def get_order(
            self,
            token_address: str,
            order_creator: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        order_filter = {
            'traded_asset': token_address
        }
        if order_creator:
            order_filter['creator'] = order_creator

        orders_query = get_dex_orders_query(**order_filter).limit(1)
        result = await self.fetch_one(orders_query)
        if result:
            return result
        return None

    async def get_dex_history(
            self,
            ordering: Ordering,
            limit: int,
            token_address: str,
            event_type: Optional[List[str]] = None,
            block_number: Optional[int] = None,
            timestamp: Optional[int] = None,
            event_index: Optional[int] = None,
    ) -> Tuple[List['HistoryEvent'], Optional[int]]:
        # WTF: only orders have information about assets
        orders_query = get_dex_orders_query(traded_asset=token_address)
        orders_ids_query = orders_query.with_only_columns([dex_logs_t.c.event_data['orderID'].astext])

        orders = await self.fetch_all(orders_query)
        orders_map = {get_order_id(item): item for item in orders}

        trades_query = get_dex_trades_query(order_ids=orders_ids_query)
        trades_ids_query = trades_query.with_only_columns([dex_logs_t.c.event_data['tradeID'].astext])

        trades = await self.fetch_all(trades_query)
        trades_map = {get_trade_id(item): item for item in trades}

        trades_orders_map = {get_trade_id(item): get_order_id(item) for item in trades}

        history_query = get_dex_events_query(
            limit=limit,
            orders_ids=orders_ids_query,
            trades_ids=trades_ids_query,
            ordering=ordering,
            block_number=block_number,
            timestamp=timestamp,
            event_index=event_index,
            events_types=event_type
        )
        history = await self.fetch_all(history_query)

        events = sorted(history, key=lambda x: x['timestamp'])
        last_affected_block = events[-1]['block_number'] if events else None

        result = []
        for event in history:
            description = EventDescription(
                event_type=event['event_type'],
                event_block_number=event['block_number'],
                event_timestamp=event['timestamp'],
                event_index=event['event_index']
            )

            if description.event_type in DexEventType.TRADE:
                trade_id = get_trade_id(event)
                order_id = trades_orders_map[trade_id]
            else:
                order_id = get_order_id(event)
                trade_id = None

            order_data = orders_map[order_id]
            order_payload = order_data['event_data']
            order_event = OrderEvent(
                order_creator=order_payload['orderCreator'],
                order_creation_timestamp=order_data['timestamp'],
                order_id=order_id,
                order_type=order_payload['orderType'],
                traded_asset=order_payload['tradedAsset'],
                traded_asset_amount=str(order_payload['tradedAmount']),
                fiat_asset=order_payload['fiatAsset'],
                fiat_price=str(order_payload['assetPrice']),
                expiration_timestamp=order_payload['expirationTimestamp'],
            )

            trade_event = None
            if trade_id:
                trade_data = trades_map[trade_id]
                trade_payload = trade_data['event_data']
                trade_event = TradeEvent(
                    trade_id=str(trade_id),
                    trade_creator=trade_payload['tradeCreator'],
                    trade_creation_timestamp=trade_data['timestamp'],
                    trade_amount=str(trade_payload['tradedAmount'])
                )

            history_event = HistoryEvent(description, order_event, trade_event)
            result.append(history_event)

        return result, last_affected_block

    async def get_dex_orders(
            self,
            token_address: str,
            order_creator: Optional[str] = None,
            order_statuses: Optional[List[str]] = None,
    ):
        # WTF: only orders have information about traded assets
        orders_query = get_dex_orders_query(creator=order_creator, traded_asset=token_address)
        orders = await self.fetch_all(orders_query)
        orders_ids_query = orders_query.with_only_columns([dex_logs_t.c.event_data['orderID'].astext])

        order_events_query = get_dex_orders_events_query(orders_ids_query)
        order_events = await self.fetch_all(order_events_query)
        order_states = get_last_order_states(orders, order_events)

        trades_query = get_dex_trades_query(order_ids=orders_ids_query)
        trades_ids_query = trades_query.with_only_columns([dex_logs_t.c.event_data['tradeID'].astext])

        trades = await self.fetch_all(trades_query)

        trades_events_query = get_dex_trades_events_query(trade_ids=trades_ids_query)
        trades_events = await self.fetch_all(trades_events_query)
        trades_states = get_last_trade_states(trades, trades_events)

        if order_statuses:
            event_types = [ORDER_STATUSES[status] for status in order_statuses]
            orders = [order for order in orders if order_states[get_order_id(order)]['event_type'] in event_types]

        completed_trades = []
        for trade in trades:
            trade_id = get_trade_id(trade)
            state = trades_states[trade_id]
            if state['event_type'] == DexEventType.TRADE_COMPLETED:
                completed_trades.append(trade)

        sorted_trades = sorted(completed_trades, key=get_order_id)
        trades_by_orders = groupby(sorted_trades, key=get_order_id)
        trades_sum_by_orders = {order_id: sum(map(get_traded_amount, trades)) for order_id, trades in trades_by_orders}

        results = []
        for order in orders:
            payload = order['event_data']
            order_id = payload['orderID']
            data = OrderDataInfo(
                order_creator=payload['orderCreator'],
                order_creation_timestamp=order['timestamp'],
                order_id=order_id,
                order_type=payload['orderType'],
                traded_asset=payload['tradedAsset'],
                traded_asset_amount=str(payload['tradedAmount']),
                fiat_asset=payload['fiatAsset'],
                fiat_price=str(payload['assetPrice']),
                expiration_timestamp=payload['expirationTimestamp'],
            )

            state = order_states[order_id]
            trades_sump = trades_sum_by_orders.get(order_id, 0)

            order_status = ORDER_EVENT_TYPE_TO_STATUS[state['event_type']]
            status = OrderStatusInfo(
                order_status=order_status,
                order_status_block_number=state['block_number'],
                remaining_traded_asset_amount=str(get_traded_amount(order) - trades_sump)
            )
            results.append(OrderInfo(order_data=data, order_status=status))

        all_events = chain(orders, order_events, trades, trades_events)
        sorted_events = sorted(all_events, key=lambda x: x['block_number'])
        last_affected_block = sorted_events[-1]['block_number'] if sorted_events else None
        return results, last_affected_block

    async def get_dex_blocked(
            self,
            user_address: str,
            token_addresses: Optional[List[str]],
    ):
        events_query = get_dex_blocked_query(user_address, token_addresses)
        events = await self.fetch_all(events_query)

        events = sorted(events, key=get_asset_address)

        total: DefaultDict[str, int] = defaultdict(lambda: 0)
        for asset, blocked_amounts in groupby(events, key=get_asset_address):
            for event in blocked_amounts:
                value = event['event_data']['assetAmount']

                if event['event_type'] == DexEventType.TOKEN_UNBLOCKED:
                    value *= -1

                total[asset] += value

        blocked_amounts = {BlockedAssetAmount(asset, str(value)) for asset, value in total.items()}

        sorted_events = sorted(events, key=lambda x: x['block_number'])
        last_affected_block = sorted_events[-1]['block_number'] if sorted_events else None
        return blocked_amounts, last_affected_block
