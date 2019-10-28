from sqlalchemy import select, Column
from sqlalchemy.orm import Query
from typing import List

from jsearch.common.tables import accounts_base_t


def get_default_fields() -> List[Column]:
    return [
        accounts_base_t.c.address,
        accounts_base_t.c.code,
        accounts_base_t.c.code_hash,
        accounts_base_t.c.last_known_balance,
        accounts_base_t.c.root,
    ]


def get_account_base_query(address: str) -> Query:
    return select(get_default_fields()).where(accounts_base_t.c.address == address)
