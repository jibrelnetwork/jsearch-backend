from sqlalchemy import select, and_, join
from sqlalchemy.dialects.postgresql import array, Any
from sqlalchemy.orm import Query
from sqlalchemy.sql.functions import max
from typing import List

from jsearch.common.tables import accounts_state_t


def get_last_balances_query(addresses: List[str]) -> Query:
    """
    Original query:

    SELECT a.address, a.balance
    FROM accounts_state a
    INNER JOIN (
        SELECT address, max(block_number) bn FROM accounts_state
        WHERE address = any($1::text[]) GROUP BY address
    ) gn
    ON a.address=gn.address AND a.block_number=gn.bn

    """
    sub_query = select(
        [
            accounts_state_t.c.address,
            max(accounts_state_t.c.block_number).label('latest_block'),
        ]
    ).where(
        Any(accounts_state_t.c.address, array(tuple(addresses)))
    ).group_by(
        accounts_state_t.c.address
    ).alias('latest_blocks')

    return join(
        accounts_state_t,
        sub_query,
        and_(
            sub_query.c.address == accounts_state_t.c.address,
            sub_query.c.latest_block == accounts_state_t.c.block_number
        )
    ).select().with_only_columns([
        accounts_state_t.c.address,
        accounts_state_t.c.balance,
        accounts_state_t.c.block_number
    ])
