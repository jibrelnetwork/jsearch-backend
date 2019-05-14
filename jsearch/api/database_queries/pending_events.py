from sqlalchemy import select, Column, and_, false
from sqlalchemy.orm import Query
from typing import List, Optional

from jsearch.common.tables import pending_wallet_events_t


def get_default_fields() -> List[Column]:
    return [
        pending_wallet_events_t.c.type,
        pending_wallet_events_t.c.event_index,
        pending_wallet_events_t.c.event_data,
        pending_wallet_events_t.c.tx_hash
    ]


def get_pending_wallet_events_query(address: str,
                                    limit: Optional[int],
                                    offset: Optional[int],
                                    columns: Optional[List[Column]] = None) -> Query:
    columns = columns or get_default_fields()
    query = select(columns).where(
        and_(
            pending_wallet_events_t.c.address == address,
            pending_wallet_events_t.c.is_removed == false()
        )
    )
    if limit:
        query = query.limit(limit)

    if offset:
        query = query.offset(offset)

    return query
