from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Query
from typing import List, Any, Dict

from jsearch.common.tables import pending_transactions_t


def insert_or_update_pending_txs_q(pending_txs: List[Dict[str, Any]]) -> Query:
    insert_query = insert(pending_transactions_t)
    insert_query = insert_query.values(pending_txs)
    insert_query = insert_query.on_conflict_do_update(
        index_elements=[
            pending_transactions_t.c.hash,
        ],
        set_={
            'last_synced_id': insert_query.excluded.last_synced_id,
            'status': insert_query.excluded.status,
            'timestamp': insert_query.excluded.timestamp,
            'removed': insert_query.excluded.removed,
            'node_id': insert_query.excluded.node_id,
        },
        where=pending_transactions_t.c.last_synced_id < insert_query.excluded.last_synced_id,
    )

    return insert_query
