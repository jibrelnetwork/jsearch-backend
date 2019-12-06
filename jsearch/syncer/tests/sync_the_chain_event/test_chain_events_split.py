import pytest
from aiopg.sa import Engine
from typing import Callable

from jsearch.common.structs import BlockRange

BLOCK_NUMBER_FROM = 8800938
BLOCK_NUMBER_UNTIL = 8800942
NODE_ID = "0x7c3b15b43fca4f42b5a9ad5b69d18dad1bf80cb91d5ce081c12f2c8fb242e406"


@pytest.fixture()
def load_splits(load_blocks):
    load_blocks(BLOCK_NUMBER_FROM, BLOCK_NUMBER_UNTIL)


FORKED_BLOCKS = [
    "0x90661d1627841c9e5c8bf3f539bede69c2b9252da8242275f6434630710b5421"
]
CANONICAL_BLOCKS = [
    "0x6ce5857605f2d676a2c945b9479cfe19526ff09badb7090cbdceda6d8181b37f",
    "0x8f3f6d5b2ad45b35a1f5279a95a379525bf91f9d7160628f7a4f80618361a8c5",
    "0x0caf8ec5e3c3228995c902746bf5eb14d499a3a629a77b210e50ae650bd90c1f",
    "0xfb067cf7de49ef5457bd4f411ee48cb0c1d5f6e897f401fb181ee7ee601f78d4",
    "0x7469520f62afe3ba6318d06b4c17cf4590317e0055246382232e24e84c2de5a3",
]


@pytest.mark.asyncio
@pytest.mark.usefixtures('load_splits')
@pytest.mark.parametrize(
    'table, hash_key',
    [
        ('accounts_state', 'block_hash'),
        ('assets_summary', 'block_hash'),
        ('blocks', 'hash'),
        ('internal_transactions', 'block_hash'),
        ('logs', 'block_hash'),
        ('receipts', 'block_hash'),
        ('token_holders', 'block_hash'),
        ('token_transfers', 'block_hash'),
        ('transactions', 'block_hash'),
        ('uncles', 'block_hash'),
        ('wallet_events', 'block_hash')
    ],
    ids=[
        'accounts_state',
        'assets_summary',
        'blocks',
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
async def test_apply_split_event(db: Engine, sync: Callable[..., None], table: str, hash_key: str):
    # when
    await sync(BlockRange(BLOCK_NUMBER_FROM, BLOCK_NUMBER_UNTIL), node_id=NODE_ID)

    # then
    results = db.execute(f'select * from {table};').fetchall()
    values = {getattr(item, hash_key, None): item['is_forked'] for item in results}

    assert all({values[block_hash] for block_hash in FORKED_BLOCKS if values.get(block_hash) is not None})
    assert not all({values[block_hash] for block_hash in CANONICAL_BLOCKS if values.get(block_hash) is not None})
