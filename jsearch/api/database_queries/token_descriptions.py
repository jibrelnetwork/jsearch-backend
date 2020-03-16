from sqlalchemy import select, and_, false, desc
from sqlalchemy.sql import ClauseElement

from jsearch.common.tables import token_descriptions_t
from jsearch.typing import TokenAddress


def get_default_fields():
    return [
        token_descriptions_t.c.token,
        token_descriptions_t.c.total_supply,
        token_descriptions_t.c.block_number,
        token_descriptions_t.c.block_hash
    ]


def get_token_threshold_query(token_address: TokenAddress) -> ClauseElement:
    columns = get_default_fields()
    return select(columns).where(
        and_(
            token_descriptions_t.c.token == token_address,
            token_descriptions_t.c.is_forked == false(),
        )
    ).order_by(desc(token_descriptions_t.c.block_number)).limit(1)
