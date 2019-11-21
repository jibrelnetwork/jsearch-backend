from sqlalchemy import select, and_, false, desc, column, String
from sqlalchemy.dialects.postgresql import array, Any
from sqlalchemy.orm import Query
from typing import List

from jsearch.common.clauses import values
from jsearch.common.tables import assets_summary_t, assets_summary_pairs_t


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
    subquery = select([assets_summary_pairs_t.c.address, assets_summary_pairs_t.c.asset_address]).where(
        Any(assets_summary_pairs_t.c.address, array(tuple(addresses))),
    )

    if assets:
        subquery = subquery.where(Any(assets_summary_pairs_t.c.asset_address, array(tuple(assets))))

    subquery = subquery.union_all(
        values(
            [
                column('address', String),
                column('asset_address', String),
            ],
            *[
                (address, '') for address in addresses
            ]
        )
    ).alias('subquery')

    subsubquery = select(get_default_fields()).where(
        and_(
            (assets_summary_t.c.address == subquery.c.address),
            (assets_summary_t.c.asset_address == subquery.c.asset_address),
            (assets_summary_t.c.is_forked == false()),
        )
    ).order_by(
        desc(assets_summary_t.c.address),
        desc(assets_summary_t.c.asset_address),
        desc(assets_summary_t.c.block_number),
    ).limit(1).alias('subsubquery')

    query = select(['*']).select_from(subquery).select_from(subsubquery.lateral())

    return query


def get_distinct_assets_by_addresses_query(addresses: List[str]) -> Query:
    return select([assets_summary_pairs_t.c.asset_address]).where(
        Any(assets_summary_pairs_t.c.address, array(tuple(addresses))),
    ).distinct()
