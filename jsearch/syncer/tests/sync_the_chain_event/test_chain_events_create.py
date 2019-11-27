import pytest
from typing import List, Optional

from jsearch.common.structs import BlockRange
from jsearch.syncer.tests.utils import normalize_data

BLOCK_NUMBER = 8500034
NODE_ID = ""


@pytest.fixture()
def load_blocks(load_blocks):
    load_blocks(BLOCK_NUMBER)


@pytest.mark.asyncio
@pytest.mark.usefixtures('load_blocks')
@pytest.mark.parametrize(
    'table, nullify_columns',
    (
            ('accounts_state', None),
            ('accounts_base', None),
            ('assets_summary', None),
            ('assets_summary_pairs', None),
            ('blocks', None),
            ('chain_events', ('created_at',)),
            ('internal_transactions', None),
            ('logs', None),
            ('receipts', None),
            ('token_holders', ('id',)),
            ('token_transfers', None),
            ('transactions', None),
            ('uncles', None),
            ('wallet_events', None)
    ),
    ids=[
        'accounts_state',
        'accounts_base',
        'assets_summary',
        'assets_summary_pairs',
        'blocks',
        'chain_events',
        'internal_transactions',
        'logs',
        'receipts',
        'token_holders',
        'token_transfers',
        'transactions',
        'uncles',
        'wallet_events'
    ]
)
async def test_write_insert_event(db, sync, get_main_db_dump, table, nullify_columns: Optional[List[str]]):
    # given
    expected = get_main_db_dump(BLOCK_NUMBER, table)

    # when
    await sync(BlockRange(BLOCK_NUMBER, BLOCK_NUMBER))

    # then
    results = db.execute(f'select * from {table};').fetchall()

    normalized_data = normalize_data(results, nullify_columns=nullify_columns)
    normalized_expected = expected.values_map(nullify_columns=nullify_columns)

    def sort_key(x):
        return tuple(x.values())

    normalized_data = sorted(normalized_data, key=sort_key)
    normalized_expected = sorted(normalized_expected, key=sort_key)

    assert normalized_expected == normalized_data
