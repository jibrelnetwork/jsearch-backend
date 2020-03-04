from _operator import and_
from functools import reduce
from typing import List, Optional, Any, Dict

from sqlalchemy import Column, select, false
from sqlalchemy.sql import ClauseElement

from jsearch.api.ordering import Ordering, get_ordering
from jsearch.common.processing.dex_logs import DexEventType
from jsearch.common.tables import dex_logs_t
from jsearch.typing import OrderScheme, OrderDirection


def get_default_fields() -> List[Column]:
    return [
        dex_logs_t.c.block_hash,
        dex_logs_t.c.block_number,
        dex_logs_t.c.timestamp,
        dex_logs_t.c.tx_hash,
        dex_logs_t.c.event_type,
        dex_logs_t.c.event_data,
        dex_logs_t.c.event_index,
    ]


def get_events_ordering(scheme: OrderScheme, direction: OrderDirection) -> Ordering:
    columns = [dex_logs_t.c.event_index]
    return get_ordering(columns, scheme, direction)


def get_clause(key, value) -> ClauseElement:
    filtered_key = dex_logs_t.c.event_data[key].astext
    if isinstance(value, (list, tuple)):
        q = filtered_key.in_(map(str, value))
    else:
        q = filtered_key == value
    return q


def get_dex_logs_query(
        event_types: List[str],
        **data_filter
) -> ClauseElement:
    assert event_types

    columns = get_default_fields()

    query = select(columns).where(
        and_(
            dex_logs_t.c.event_type.in_(event_types),
            dex_logs_t.c.is_forked == false()
        )
    )

    filters = [get_clause(key, value) for key, value in data_filter.items()]
    if filters:
        query = query.where(reduce(and_, filters[1:], filters[0]))
    return query


def get_dex_events_query(
        orders_ids: List[int],
        trades_ids: List[str],
        ordering: Ordering,
        limit: Optional[int] = None,
        events_types: Optional[List[str]] = None,
        block_number: Optional[int] = None,
        timestamp: Optional[int] = None,
        event_index: Optional[int] = None
) -> ClauseElement:
    if not events_types:
        events_types = [
            DexEventType.ORDER_PLACED,
            DexEventType.ORDER_ACTIVATED,
            DexEventType.ORDER_COMPLETED,
            DexEventType.ORDER_CANCELLED,
            DexEventType.ORDER_EXPIRED,
            DexEventType.TRADE_PLACED,
            DexEventType.TRADE_COMPLETED,
            DexEventType.TRADE_CANCELLED,
        ]
    query = get_dex_logs_query(event_types=events_types)
    if event_index:
        query = query.where(ordering.operator_or_equal(dex_logs_t.c.event_index, event_index))

    elif block_number:
        query = query.where(ordering.operator_or_equal(dex_logs_t.c.block_number, block_number))

    elif timestamp:
        query = query.where(ordering.operator_or_equal(dex_logs_t.c.timestamp, timestamp))

    order_ids_q = get_clause('orderID', orders_ids)
    trade_ids_q = get_clause('tradeID', trades_ids)

    query = query.where(order_ids_q | trade_ids_q)
    query = query.order_by(*ordering.columns)

    if limit:
        query = query.limit(limit)

    return query.order_by(*ordering.columns)


def get_dex_blocked_query(
        user_address: str = None,
        token_addresses: Optional[List[str]] = None,
) -> ClauseElement:
    filter_kwargs: Dict[str, Any] = {
        'userAddress': user_address
    }

    if token_addresses:
        filter_kwargs['assetAddress'] = token_addresses

    return get_dex_logs_query(
        event_types=[
            DexEventType.TOKEN_BLOCKED,
            DexEventType.TOKEN_UNBLOCKED
        ],
        **filter_kwargs
    )


def get_dex_orders_query(
        traded_asset: Optional[str] = None,
        creator: Optional[str] = None,
) -> ClauseElement:
    filter_kwargs = {}

    if creator:
        filter_kwargs['orderCreator'] = creator

    if traded_asset:
        filter_kwargs['tradedAsset'] = traded_asset

    return get_dex_logs_query(
        event_types=[
            DexEventType.ORDER_PLACED
        ],
        **filter_kwargs
    )


def get_dex_orders_events_query(ids: List[int]) -> ClauseElement:
    return get_dex_logs_query(
        event_types=[
            DexEventType.ORDER_ACTIVATED,
            DexEventType.ORDER_COMPLETED,
            DexEventType.ORDER_CANCELLED,
            DexEventType.ORDER_EXPIRED
        ],
        orderID=ids
    )


def get_dex_trades_query(order_ids: List[int]) -> ClauseElement:
    return get_dex_logs_query(
        event_types=[
            DexEventType.TRADE_PLACED,
        ],
        orderID=order_ids
    )


def get_dex_trades_events_query(trade_ids: List[int]) -> ClauseElement:
    return get_dex_logs_query(
        event_types=[
            DexEventType.TRADE_COMPLETED,
            DexEventType.TRADE_CANCELLED,
        ],
        tradeID=trade_ids
    )
