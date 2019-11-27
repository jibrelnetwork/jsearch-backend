import datetime
import pytest

from jsearch.common import tables as t


@pytest.mark.asyncio
async def test_main_db_get_missed_blocks_empty(db, main_db_wrapper):
    res = await main_db_wrapper.get_missed_blocks_numbers(10)
    assert res == []


@pytest.mark.asyncio
async def test_main_db_get_missed_blocks(db, main_db_wrapper):
    db.execute('INSERT INTO blocks (number, hash) values (%s, %s)', [
        (1, 'aa'),
        (2, 'ab'),
        (4, 'ad'),
        (5, 'ac'),
        (7, 'ae'),
    ])
    res = await main_db_wrapper.get_missed_blocks_numbers(10)
    assert res == [3, 6]


@pytest.mark.asyncio
async def test_main_db_get_missed_blocks_limit2(db, main_db_wrapper):
    db.execute('INSERT INTO blocks (number, hash) values (%s, %s)', [
        (1, 'aa'),
        (2, 'ab'),
        (4, 'ad'),
        (5, 'ac'),
        (7, 'ae'),
        (9, 'af'),
    ])
    res = await main_db_wrapper.get_missed_blocks_numbers(2)
    assert res == [3, 6]


@pytest.mark.asyncio
async def test_maindb_write_block_data_asset_summary_update(db, main_db_dump, main_db_wrapper):
    from jsearch.syncer.processor import BlockData

    block_data = main_db_dump['blocks'][2]

    assets_summary_updates = [
        {
            'address': '0x1',
            'asset_address': '0xc1',
            'tx_number': 1,
            'nonce': 1,
            'value': 1000,
            'decimals': 1,
            'block_number': 1,
            'block_hash': "0x01"
        }
    ]
    assets_summary_pairs = [
        {'address': '0x1', 'asset_address': '0xc1'}
    ]

    block = BlockData(
        block=block_data,
        uncles=[],
        txs=[],
        receipts=[],
        logs=[],
        accounts=[],
        internal_txs=[],
        assets_summary_updates=assets_summary_updates,
        assets_summary_pairs=assets_summary_pairs,
        token_holders_updates=[],
        transfers=[],
        wallet_events=[],
    )

    chain_event = {
        'id': 1,
        'block_number': 1,
        'block_hash': '0x01',
        'created_at': datetime.datetime.now(),
        'add_block_hash': None,
        'add_length': None,
        'common_block_hash': None,
        'common_block_number': None,
        'drop_block_hash': None,
        'drop_length': None,
        'node_id': '0xXX',
        'parent_block_hash': None,
        'type': 'create'
    }

    await main_db_wrapper.write_block(chain_event, block, rewrite=False)

    db_assets = db.execute(t.assets_summary_t.select()).fetchall()
    db_pairs = db.execute(t.assets_summary_pairs_t.select()).fetchall()

    assert len(db_assets) == 1
    assert dict(db_assets[0]) == {
        'address': '0x1',
        'asset_address': '0xc1',
        'tx_number': 1,
        'nonce': 1,
        'value': 1000,
        'decimals': 1,
        'block_number': 1,
        'block_hash': "0x01",
        'is_forked': False
    }

    assert len(db_pairs) == 1
    assert dict(db_pairs[0]) == {
        'address': '0x1',
        'asset_address': '0xc1',
    }

    assets_summary_updates = [
        {
            'address': '0x1',
            'asset_address': '0xc1',
            'tx_number': 2,
            'nonce': 2,
            'value': 2000,
            'decimals': 1,
            'block_number': 2,
            'block_hash': "0x02"
        }
    ]
    assets_summary_pairs = [
        {'address': '0x1', 'asset_address': '0xc1'}
    ]
    block_data = main_db_dump['blocks'][3]
    block = BlockData(
        block=block_data,
        uncles=[],
        txs=[],
        receipts=[],
        logs=[],
        accounts=[],
        internal_txs=[],
        assets_summary_updates=assets_summary_updates,
        assets_summary_pairs=assets_summary_pairs,
        token_holders_updates=[],
        transfers=[],
        wallet_events=[],
    )
    chain_event['id'] = 2

    await main_db_wrapper.write_block(chain_event, block, rewrite=False)

    db_assets = db.execute(t.assets_summary_t.select().order_by(t.assets_summary_t.c.block_number)).fetchall()
    db_pairs = db.execute(t.assets_summary_pairs_t.select()).fetchall()

    assert len(db_assets) == 2
    assert [dict(item) for item in db_assets] == [
        {
            'address': '0x1',
            'asset_address': '0xc1',
            'tx_number': 1,
            'nonce': 1,
            'value': 1000,
            'decimals': 1,
            'block_number': 1,
            'block_hash': "0x01",
            'is_forked': False
        },
        {
            'address': '0x1',
            'asset_address': '0xc1',
            'tx_number': 2,
            'nonce': 2,
            'value': 2000,
            'decimals': 1,
            'block_number': 2,
            'block_hash': "0x02",
            'is_forked': False
        },
    ]

    assert len(db_pairs) == 1
    assert dict(db_pairs[0]) == {
        'address': '0x1',
        'asset_address': '0xc1',
    }
