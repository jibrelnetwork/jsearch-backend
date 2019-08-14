from sqlalchemy import select
from sqlalchemy.orm import Query
from typing import List

from jsearch.common.tables import accounts_state_t


def get_accounts_state_for_blocks_query(blocks_hashes: List[str]) -> Query:
    return select(
        columns=[
            accounts_state_t.c.address,
        ],
    ).where(accounts_state_t.c.block_hash.in_(blocks_hashes))


# def get_last_ether_balances_query(blocks_hashes: List[str]) -> Query:
#     return select(
#         columns=[
#             accounts_state_t.c.address,
#             accounts_state_t.c.block_number,
#             accounts_state_t.c.balance,
#             accounts_state_t.c.nonce,
#         ]
#     ).where(
#         and_(
#             accounts_state_t.c.address.in_(get_accounts_state_for_blocks_query(blocks_hashes=blocks_hashes)),
#             accounts_state_t.c.is_forked == false()
#         )
#     ).order_by(
#         accounts_state_t.c.address,
#         desc(accounts_state_t.c.block_number)
#     ).distinct(
#         accounts_state_t.c.address
#     )
