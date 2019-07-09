from functools import partial
from sqlalchemy import select, Column, and_, false
from sqlalchemy.orm import Query
from sqlalchemy.sql.functions import sum
from typing import List

from jsearch.common.tables import token_transfers_t
from jsearch.syncer.structs import TokenHolder


def get_token_address_and_accounts_for_blocks_q(block_hashes: List[str]) -> Query:
    return select(
        columns=[
            token_transfers_t.c.token_address,
            token_transfers_t.c.address
        ],
        distinct=True
    ).where(token_transfers_t.c.block_hash.in_(block_hashes))


def get_transfers_after_block(holder: TokenHolder, address_column: Column, block: int) -> Query:
    query = select(
        columns=[
            sum(token_transfers_t.c.token_value).label('change')
        ]
    ).order_by(
        token_transfers_t.c.token_address,
        token_transfers_t.c.address,
        token_transfers_t.c.block_number,
    ).where(
        and_(
            token_transfers_t.c.block_number > block,
            token_transfers_t.c.token_address == holder.token,
            token_transfers_t.c.address == holder.account,
            token_transfers_t.c.is_forked == false(),
            token_transfers_t.c.status == 1,  # we need only transfers from success transactions
            token_transfers_t.c.address == address_column
            # we need ^ because we create 2 token transfers on 1 log record
        )
    )

    return query


get_incomes_after_block_query = partial(get_transfers_after_block, address_column=token_transfers_t.c.to_address)
get_outcomes_after_block_query = partial(get_transfers_after_block, address_column=token_transfers_t.c.from_address)
