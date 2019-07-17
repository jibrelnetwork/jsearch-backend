from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Query

from jsearch.common.tables import notable_accounts_t
from jsearch.notable_accounts_worker import structs


def insert_or_update_notable_account(notable_account: structs.NotableAccount) -> Query:
    insert_query = insert(notable_accounts_t)
    insert_query = insert_query.values(notable_account.as_dict())
    insert_query = insert_query.on_conflict_do_update(
        index_elements=[
            notable_accounts_t.c.address,
        ],
        set_={
            'name': insert_query.excluded.name,
            'labels': insert_query.excluded.labels,
        },
    )

    return insert_query


def insert_or_skip_notable_account(notable_account: structs.NotableAccount) -> Query:
    insert_query = insert(notable_accounts_t)
    insert_query = insert_query.values(notable_account.as_dict())
    insert_query = insert_query.on_conflict_do_nothing(
        index_elements=[
            notable_accounts_t.c.address,
        ],
    )

    return insert_query
