from typing import Dict, Any

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Query

from jsearch.common.tables import pending_transactions_t


def insert_or_update_pending_tx_q(pending_tx: Dict[str, Any]) -> Query:
    insert_query = insert(pending_transactions_t)
    insert_query = insert_query.values(**_prepare_pending_tx(pending_tx))
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
        }
    )

    return insert_query


def _prepare_pending_tx(raw_data: Dict[str, Any]) -> Dict[str, Any]:
    return {
        'last_synced_id': raw_data['id'],
        'hash': raw_data['tx_hash'],
        'status': raw_data['status'],
        'timestamp': raw_data['timestamp'],
        'removed': raw_data['removed'],
        'node_id': raw_data['node_id'],
        'r': raw_data['fields'].get('r'),
        's': raw_data['fields'].get('s'),
        'v': raw_data['fields'].get('v'),
        'to': raw_data['fields'].get('to'),
        'from': raw_data['fields'].get('from'),
        'gas': raw_data['fields'].get('gas'),
        'gas_price': raw_data['fields'].get('gasPrice'),
        'input': raw_data['fields'].get('input'),
        'nonce': raw_data['fields'].get('nonce'),
        'value': raw_data['fields'].get('value'),
    }
