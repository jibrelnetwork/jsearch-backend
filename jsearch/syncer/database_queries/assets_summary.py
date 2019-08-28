from sqlalchemy import delete, and_, or_, null
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Query
from typing import Optional, List

from jsearch.common.tables import assets_summary_t


def upsert_assets_summary_query(
        address: str,
        asset_address: str,
        value: int,
        decimals: Optional[int] = None,
        block_number: Optional[int] = None,
        nonce: Optional[int] = None,
        tx_number: Optional[int] = None,
        blocks_to_replace: Optional[List[int]] = None,
) -> Query:
    summary_data = {
        'address': address,
        'asset_address': asset_address,
        'value': value,
        'decimals': decimals,
        'tx_number': tx_number,
        'block_number': block_number,
        'nonce': nonce,
    }
    summary_data = {key: value for key, value in summary_data.items() if value is not None}
    query = insert(assets_summary_t).values(tx_number=1, block_hash=None, **summary_data)

    if blocks_to_replace:
        q = or_(
            assets_summary_t.c.block_number <= query.excluded.block_number,
            assets_summary_t.c.block_number.is_(null()),
            assets_summary_t.c.block_number.in_(blocks_to_replace)
        )
    else:
        q = or_(
            assets_summary_t.c.block_number <= query.excluded.block_number,
            assets_summary_t.c.block_number.is_(null())
        )

    query = query.on_conflict_do_update(
        index_elements=['address', 'asset_address', 'block_hash'],
        set_={
            'value': value,
            'tx_number': tx_number,
            'block_number': block_number,
            'nonce': nonce
        },
        where=q
    )

    return query


def delete_assets_summary_query(address: str, asset_address: str) -> Query:
    return delete(assets_summary_t).where(
        and_(
            assets_summary_t.c.address == address,
            assets_summary_t.c.asset_address == asset_address
        )
    )
