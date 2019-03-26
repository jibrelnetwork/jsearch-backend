from typing import List

from sqlalchemy import and_, false, func, Column, select
from sqlalchemy.orm import Query

from jsearch.common.tables import blocks_t


def get_default_fields():
    return [
        blocks_t.c.difficulty,
        blocks_t.c.extra_data,
        blocks_t.c.gas_limit,
        blocks_t.c.gas_used,
        blocks_t.c.hash,
        blocks_t.c.logs_bloom,
        blocks_t.c.miner,
        blocks_t.c.mix_hash,
        blocks_t.c.nonce,
        blocks_t.c.number,
        blocks_t.c.parent_hash,
        blocks_t.c.receipts_root,
        blocks_t.c.sha3_uncles,
        blocks_t.c.state_root,
        blocks_t.c.static_reward,
        blocks_t.c.timestamp,
        blocks_t.c.transactions_root,
        blocks_t.c.uncle_inclusion_reward,
        blocks_t.c.tx_fees,
    ]


def get_block_by_hash_query(block_hash: str, columns: List[Column] = None) -> Query:
    columns = columns or get_default_fields()
    return select(
        columns=columns,
        whereclause=and_(
            blocks_t.c.hash == block_hash,
            blocks_t.c.is_forked == false()
        )
    )


def get_block_by_number_query(number: int, columns: List[Column] = None) -> Query:
    columns = columns or get_default_fields()
    return select(
        columns=columns,
        whereclause=and_(
            blocks_t.c.number == number,
            blocks_t.c.is_forked == false()
        )
    )


def get_last_block_query(columns: List[Column] = None) -> Query:
    columns = columns or get_default_fields()
    return select(
        columns=columns,
        whereclause=and_(
            blocks_t.c.number == select(columns=[func.max(blocks_t.c.number)]),
            blocks_t.c.is_forked == false()
        )
    )
