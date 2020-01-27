from sqlalchemy import select, and_, asc, false, func
from sqlalchemy.sql import Delete, Select

from jsearch.common.tables import assets_summary_pairs_t, token_holders_t
from jsearch.token_holders_cleaner.structs import Pair


def get_pairs_for_one_account(last_processed_pair: Pair, limit: int) -> Select:
    """
    Useful, if only a part of a batch has been processed.
    """
    return select(
        [
            assets_summary_pairs_t.c.address,
            assets_summary_pairs_t.c.asset_address,
        ],
    ).where(
        and_(
            assets_summary_pairs_t.c.address == last_processed_pair.account_address,
            assets_summary_pairs_t.c.asset_address > last_processed_pair.token_address,
        ),
    ).order_by(
        asc(assets_summary_pairs_t.c.address),
        asc(assets_summary_pairs_t.c.asset_address),
    ).limit(limit)


def get_pairs_for_all_accounts(last_processed_pair: Pair, limit: int) -> Select:
    """
    Useful, if all pairs for an account has been processed.
    """
    return select(
        [
            assets_summary_pairs_t.c.address,
            assets_summary_pairs_t.c.asset_address,
        ],
    ).where(
        and_(
            assets_summary_pairs_t.c.address > last_processed_pair.account_address,
        ),
    ).order_by(
        asc(assets_summary_pairs_t.c.address),
        asc(assets_summary_pairs_t.c.asset_address),
    ).limit(limit)


def get_max_block_number_for_pair(pair: Pair) -> Select:
    return select(
        [func.max(token_holders_t.c.block_number).label('max_block_number')],
    ).where(
        and_(
            token_holders_t.c.account_address == pair.account_address,
            token_holders_t.c.token_address == pair.token_address,
            token_holders_t.c.is_forked == false(),
        )
    )


def delete_stale_holders_by_pair(pair: Pair, block_number: int) -> Delete:
    return token_holders_t.delete().where(
        and_(
            token_holders_t.c.account_address == pair.account_address,
            token_holders_t.c.token_address == pair.token_address,
            token_holders_t.c.block_number <= block_number,
            token_holders_t.c.is_forked == false(),
        )
    )
