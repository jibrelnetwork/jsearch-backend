from functools import reduce
from sqlalchemy import false, tuple_
from sqlalchemy import select, and_
from sqlalchemy.dialects.postgresql import array, Any
from sqlalchemy.orm import Query
from sqlalchemy.sql.functions import max
from typing import Optional, List

from jsearch.common.tables import assets_summary_t


def get_default_fields():
    return [
        assets_summary_t.c.address,
        assets_summary_t.c.asset_address,
        assets_summary_t.c.value,
        assets_summary_t.c.decimals,
        assets_summary_t.c.tx_number,
        assets_summary_t.c.nonce,
        assets_summary_t.c.block_number,
    ]


def get_assets_summary_query(addresses: List[str], assets: Optional[List[str]]) -> Query:
    conditions = [
        Any(assets_summary_t.c.address, array(tuple(addresses))),
        assets_summary_t.c.is_forked == false()
    ]
    if assets:
        conditions.append(
            Any(assets_summary_t.c.asset_address, array(tuple(assets)))
        )

    conditions = reduce(lambda accumulator, condition: accumulator & condition, conditions)

    sub_query = select(
        [
            assets_summary_t.c.address,
            assets_summary_t.c.asset_address,
            max(assets_summary_t.c.block_number)
        ]
    ).where(conditions).group_by(
        assets_summary_t.c.address,
        assets_summary_t.c.asset_address
    ).alias('latest_blocks')

    columns = get_default_fields()

    return select(columns).where(
        and_(
            tuple_(
                assets_summary_t.c.address,
                assets_summary_t.c.asset_address,
                assets_summary_t.c.block_number
            ).in_(
                sub_query
            ),
            assets_summary_t.c.is_forked == false(),
            assets_summary_t.c.value > 0
        )
    )
