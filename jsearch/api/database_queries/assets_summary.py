from sqlalchemy import select, and_, false
from sqlalchemy.dialects.postgresql import array, Any
from sqlalchemy.orm import Query
from typing import List

from jsearch.common.functions import get_assets_summaries_f
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


def get_assets_summary_query(addresses: List[str], assets: List[str]) -> Query:
    return select(['*']).select_from(get_assets_summaries_f(
        addresses=array(tuple(addresses)),
        assets=array(tuple(assets))),
    )


def get_distinct_assets_by_addresses_query(addresses: List[str]) -> Query:
    return select(
        [
            assets_summary_t.c.address,
            assets_summary_t.c.asset_address,
        ]
    ).where(
        and_(
            Any(assets_summary_t.c.address, array(tuple(addresses))),
            assets_summary_t.c.is_forked == false(),
        )
    ).distinct()
