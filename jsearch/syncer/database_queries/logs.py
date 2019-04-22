from sqlalchemy import and_
from sqlalchemy.orm import Query
from typing import Dict, Any

from jsearch.common.tables import logs_t


def update_log_query(tx_hash, block_hash, log_index, values: Dict[str, Any]) -> Query:
    return logs_t.update(). \
        where(and_(logs_t.c.transaction_hash == tx_hash,
                   logs_t.c.block_hash == block_hash,
                   logs_t.c.log_index == log_index)). \
        values(**values)
