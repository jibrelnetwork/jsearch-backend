from typing import List, Optional

from sqlalchemy import select, func, Column
from sqlalchemy.orm import Query

from jsearch.api.helpers import ChainEvent
from jsearch.common.tables import chain_events_t


def get_default_fields() -> List[Column]:
    return [
        chain_events_t.c.id,
        chain_events_t.c.block_hash,
        chain_events_t.c.block_number,
        chain_events_t.c.type,
        chain_events_t.c.parent_block_hash,
        chain_events_t.c.common_block_number,
        chain_events_t.c.common_block_hash,
        chain_events_t.c.drop_length,
        chain_events_t.c.drop_block_hash,
        chain_events_t.c.add_length,
        chain_events_t.c.add_block_hash,
        chain_events_t.c.node_id,
        chain_events_t.c.created_at,
    ]


def select_latest_chain_event_id() -> Query:
    return select([func.max(chain_events_t.c.id).label('max_id')])


def select_closest_chain_split(
        last_known_chain_event_id: int,
        last_affected_block: int,
        fields: Optional[List[Column]] = None,
) -> Query:
    fields = fields or get_default_fields()

    return select(fields).where(
        (chain_events_t.c.id > last_known_chain_event_id) &
        (chain_events_t.c.block_number < last_affected_block) &
        (chain_events_t.c.type == ChainEvent.SPLIT)
    ).limit(1)
