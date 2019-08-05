from sqlalchemy import Column, select
from sqlalchemy.dialects.postgresql import array, Any
from sqlalchemy.orm import Query
from typing import Optional, List

from jsearch.common.tables import assets_summary_t


def get_default_fields():
    return [
        assets_summary_t.c.address,
        assets_summary_t.c.asset_address,
        assets_summary_t.c.value,
        assets_summary_t.c.decimals,
        assets_summary_t.c.tx_number,
        assets_summary_t.c.nonce
    ]


def get_assets_summary_query(addresses: List[str],
                             assets: Optional[List[str]],
                             columns: Optional[List[Column]] = None) -> Query:
    columns = columns or get_default_fields()
    query = select(columns).where(Any(assets_summary_t.c.address, array(tuple(addresses))))

    if assets:
        query = query.where(Any(assets_summary_t.c.asset_address, array(tuple(assets))))

    return query.order_by(assets_summary_t.c.address, assets_summary_t.c.asset_address)
