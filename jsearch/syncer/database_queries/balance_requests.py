from sqlalchemy import select, desc, and_, or_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Query
from typing import Set

from jsearch.common.tables import balance_requests_t
from jsearch.syncer.structs import TokenHolder
from jsearch.typing import TokenAddress, AccountAddress


def insert_balance_request_query(
        contract_address: TokenAddress,
        account_address: AccountAddress,
        balance: int,
        block_number: int
) -> Query:
    query = insert(balance_requests_t).values({
        'contract_address': contract_address,
        'account_address': account_address,
        'block_number': block_number,
        'balance': balance
    })
    query = query.on_conflict_do_update(
        index_elements=[
            balance_requests_t.c.contract_address,
            balance_requests_t.c.account_address,
        ],
        set_={
            'balance': balance,
            'block_number': block_number,
        },
        where=balance_requests_t.c.block_number < query.excluded.block_number,
    )

    return query


def get_balance_request_query(holders: Set[TokenHolder]) -> Query:
    query = select(
        columns=[
            balance_requests_t.c.account_address,
            balance_requests_t.c.token_address,
            balance_requests_t.c.balance,
            balance_requests_t.c.block_number
        ]
    ).order_by(
        balance_requests_t.c.token_address,
        balance_requests_t.c.address,
        desc(balance_requests_t.c.block_number),
    ).where(
        or_(*(and_(balance_requests_t.c.account_address == holder.account,
                   balance_requests_t.c.token == holder.token) for holder in holders))
    ).limit(1)

    return query
