from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Query

from jsearch.common.tables import reorgs_t


def insert_reorg(
        block_hash: str,
        block_number: int,
        reinserted: bool,
        node_id: str,
        split_id: str,
) -> Query:
    insert_query = insert(reorgs_t).values(
        block_hash=block_hash,
        block_number=block_number,
        node_id=node_id,
        split_id=split_id,
        reinserted=reinserted
    )
    return insert_query.on_conflict_do_nothing(
        index_elements=['block_hash', 'split_id', 'node_id'],
    )
