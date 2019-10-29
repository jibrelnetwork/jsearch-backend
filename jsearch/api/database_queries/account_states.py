from sqlalchemy import select, and_, false, Column, desc
from sqlalchemy.orm import Query
from typing import List, Optional

from jsearch.api.helpers import Tag
from jsearch.common.tables import accounts_state_t, blocks_t


def get_default_fields() -> List[Column]:
    return [
        accounts_state_t.c.address,
        accounts_state_t.c.balance,
        accounts_state_t.c.block_number,
        accounts_state_t.c.block_hash,
        accounts_state_t.c.nonce
    ]


def get_account_state_query(address: str, tag: Tag) -> Query:
    block_clause = _get_block_clause(tag)
    query = select(get_default_fields()).where(
        and_(
            accounts_state_t.c.address == address,
            accounts_state_t.c.is_forked == false(),
        )
    )

    if block_clause is not None:
        query = query.where(block_clause)

    return query.order_by(desc(accounts_state_t.c.block_number)).limit(1)


def _get_block_clause(tag: Tag) -> Optional[Query]:
    if tag.is_hash():
        return accounts_state_t.c.block_number <= select([blocks_t.c.number]).where(blocks_t.c.hash == tag.value)

    if tag.is_number():
        return accounts_state_t.c.block_number <= tag.value

    return None
