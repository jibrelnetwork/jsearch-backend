from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Query

from jsearch.common.tables import assets_summary_t


def insert_or_update_assets_summary(address, asset_address, value: int, decimals: int) -> Query:
    summary_data = {
        'address': address,
        'asset_address': asset_address,
        'value': value,
        'decimals': decimals,
    }
    query = insert(assets_summary_t).values(tx_number=1, **summary_data)

    return query.on_conflict_do_update(
        index_elements=['address', 'asset_address'],
        set_={
            'value': value,
            'decimals': decimals
        }
    )
