from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Query

from jsearch.common.tables import token_holders_t


def update_token_holder_balance_q(token_address: str, account_address, balance: int, decimals: int) -> Query:
    insert_query = insert(token_holders_t).values(
        token_address=token_address,
        account_address=account_address,
        balance=balance,
        decimals=decimals
    )
    return insert_query.on_conflict_do_update(
        index_elements=['token_address', 'account_address'],
        set_=dict(balance=balance, decimals=decimals)
    )
