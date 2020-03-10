from _operator import and_
from typing import List

from sqlalchemy import Column, select, false
from sqlalchemy.sql import ClauseElement

from jsearch.common.processing.dex_logs import DexEventType
from jsearch.common.tables import dex_logs_t


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


def get_dex_logs_query(
        event_types: List[str],
) -> ClauseElement:
    assert event_types in DexEventType.ALL
    columns = get_default_fields()
    query = select(columns).where(
        and_(
            dex_logs_t.c.event_type.in_(event_types),
            dex_logs_t.c.is_forked == false()
        )
    )
    return query


def get_dex_orders_query(
        ids: List[str],
) -> ClauseElement:
    query = get_dex_logs_query(
        event_types=[
            DexEventType.ORDER_PLACED,
        ],
    )
    return query.where(
        dex_logs_t.c.event_data['orderID'].astext.in_(map(str, ids))
    )


def get_dex_trades_query(
        ids: List[str],
) -> ClauseElement:
    query = get_dex_logs_query(
        event_types=[
            DexEventType.TRADE_PLACED,
        ],
    )
    return query.where(
        dex_logs_t.c.event_data['tradeID'].astext.in_(map(str, ids))
    )
