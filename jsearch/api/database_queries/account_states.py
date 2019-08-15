from sqlalchemy import select, and_, false, tuple_
from sqlalchemy.dialects.postgresql import array, Any
from sqlalchemy.orm import Query
from sqlalchemy.sql.functions import max
from typing import List

from jsearch.common.tables import accounts_state_t


def get_last_balances_query(addresses: List[str]) -> Query:
    sub_query = select(
        [
            accounts_state_t.c.address,
            max(accounts_state_t.c.block_number)
        ]
    ).where(
        and_(
            Any(accounts_state_t.c.address, array(tuple(addresses))),
            accounts_state_t.c.is_forked == false()
        )
    ).group_by(
        accounts_state_t.c.address
    ).alias('latest_blocks')

    return select([
        accounts_state_t.c.address,
        accounts_state_t.c.balance,
        accounts_state_t.c.block_number,
        accounts_state_t.c.nonce
    ]).where(
        and_(
            tuple_(
                accounts_state_t.c.address,
                accounts_state_t.c.block_number
            ).in_(
                sub_query
            ),
            accounts_state_t.c.is_forked == false()
        )
    )
