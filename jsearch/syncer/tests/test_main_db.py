import datetime
from decimal import Decimal

from jsearch.common import tables as t
from jsearch.common.database import MainDBSync
from jsearch.syncer.database import MainDB
from jsearch.syncer.manager import process_chain_split


async def test_main_db_get_last_synced_block_empty(db_dsn):
    main_db = MainDB(db_dsn)
    await main_db.connect()
    res = await main_db.get_latest_synced_block_number([1, None])
    assert res is None


async def test_main_db_get_last_synced_block_no_miss(db, db_dsn):
    db.execute('INSERT INTO blocks (number, hash) values (%s, %s)', [
        (1, 'aa'),
        (2, 'ab'),
        (3, 'ac'),
        (4, 'ad'),
    ])
    main_db = MainDB(db_dsn)
    await main_db.connect()
    res = await main_db.get_latest_synced_block_number([1, None])
    assert res == 4


async def test_main_db_get_last_synced_block_has_miss(db, db_dsn):
    db.execute('INSERT INTO blocks (number, hash) values (%s, %s)', [
        (1, 'aa'),
        (2, 'ab'),
        (4, 'ad'),
    ])
    main_db = MainDB(db_dsn)
    await main_db.connect()
    res = await main_db.get_latest_synced_block_number([1, None])
    assert res == 4


async def test_main_db_get_missed_blocks_empty(db, db_dsn):
    main_db = MainDB(db_dsn)
    await main_db.connect()
    res = await main_db.get_missed_blocks_numbers(10)
    assert res == []


async def test_main_db_get_missed_blocks(db, db_dsn):
    db.execute('INSERT INTO blocks (number, hash) values (%s, %s)', [
        (1, 'aa'),
        (2, 'ab'),
        (4, 'ad'),
        (5, 'ac'),
        (7, 'ae'),
    ])
    main_db = MainDB(db_dsn)
    await main_db.connect()
    res = await main_db.get_missed_blocks_numbers(10)
    assert res == [3, 6]


async def test_main_db_get_missed_blocks_limit2(db, db_dsn):
    db.execute('INSERT INTO blocks (number, hash) values (%s, %s)', [
        (1, 'aa'),
        (2, 'ab'),
        (4, 'ad'),
        (5, 'ac'),
        (7, 'ae'),
        (9, 'af'),
    ])
    main_db = MainDB(db_dsn)
    await main_db.connect()
    res = await main_db.get_missed_blocks_numbers(2)
    assert res == [3, 6]


async def test_maindb_write_block_data(db, main_db_dump, db_dsn):
    from jsearch.syncer.processor import BlockData

    main_db = MainDBSync(db_dsn)
    main_db.connect()
    d = main_db_dump
    block = d['blocks'][2]
    block_num = block['number']

    def for_block(items):
        return list(filter(lambda x: x['block_number'] == block_num, items))

    uncles = for_block(d['uncles'])
    transactions = for_block(d['transactions'])
    receipts = for_block(d['receipts'])
    logs = for_block(d['logs'])
    internal_txs = for_block(d['internal_transactions'])

    accounts_states = for_block(d['accounts_state'])
    accounts_bases = {item['address']: item for item in d['accounts_base']}
    receipts_statuses = {item['transaction_hash']: item['status'] for item in receipts}
    transactions = [{'status': receipts_statuses[tx['hash']], **tx} for tx in transactions]

    txs = []
    for tx in transactions:
        rt1 = {'address': tx['from'], **tx}
        rt2 = {'address': tx['to'], **tx}

        txs.append(rt1)
        txs.append(rt2)

    accounts = []
    for state in accounts_states:
        base = accounts_bases[state['address']]

        account = {}
        account.update(state)
        account.update(base)

        accounts.append(account)

    block = BlockData(
        block=block,
        uncles=uncles,
        txs=txs,
        receipts=receipts,
        logs=logs,
        accounts=accounts,
        internal_txs=internal_txs,
        assets_summary_updates=[],
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

    async with MainDB(db_dsn) as async_db:
        await block.write(async_db, chain_event)

    db_blocks = db.execute(t.blocks_t.select()).fetchall()
    db_transactions = db.execute(t.transactions_t.select()).fetchall()
    db_receipts = db.execute(t.receipts_t.select()).fetchall()
    db_logs = db.execute(t.logs_t.select()).fetchall()
    db_accounts_base = db.execute(t.accounts_base_t.select()).fetchall()
    db_accounts_state = db.execute(t.accounts_state_t.select()).fetchall()
    db_chain_events = db.execute(t.chain_events_t.select()).fetchall()

    assert dict(db_blocks[0]) == block.block
    assert [dict(tx) for tx in db_transactions] == txs
    assert [dict(r) for r in db_receipts] == receipts
    assert [dict(l) for l in db_logs] == logs
    assert [dict(a) for a in db_accounts_base] == [
        {
            'address': accounts[0]['address'],
            'code': accounts[0]['code'],
            'code_hash': accounts[0]['code_hash'],
            'last_known_balance': accounts[0]['balance'],
            'root': accounts[0]['root'],
        },
        {
            'address': accounts[1]['address'],
            'code': accounts[1]['code'],
            'code_hash': accounts[1]['code_hash'],
            'last_known_balance': accounts[1]['balance'],
            'root': accounts[1]['root'],
        },
    ]

    assert [dict(a) for a in db_accounts_state] == [
        {
            'block_number': accounts[0]['block_number'],
            'block_hash': accounts[0]['block_hash'],
            'address': accounts[0]['address'],
            'nonce': accounts[0]['nonce'],
            'root': accounts[0]['root'],
            'balance': Decimal(accounts[0]['balance']),
            'is_forked': False,

        },
        {
            'block_number': accounts[1]['block_number'],
            'block_hash': accounts[1]['block_hash'],
            'address': accounts[1]['address'],
            'nonce': accounts[1]['nonce'],
            'root': accounts[1]['root'],
            'balance': Decimal(accounts[1]['balance']),
            'is_forked': False,
        },
    ]

    assert dict(db_chain_events[0]) == chain_event


async def test_maindb_write_block_data_asset_summary_update(db, main_db_dump, db_dsn):
    from jsearch.syncer.processor import BlockData

    main_db = MainDBSync(db_dsn)
    main_db.connect()

    block_data = main_db_dump['blocks'][2]
    block_num = block_data['number']

    assets_summary_updates = [{'address': '0x1',
                               'asset_address': '0xc1',
                               'tx_number': 1,
                               'nonce': 1,
                               'value': 1000,
                               'decimals': 1,
                               'block_number': 1}]

    block = BlockData(
        block=block_data,
        uncles=[],
        txs=[],
        receipts=[],
        logs=[],
        accounts=[],
        internal_txs=[],
        assets_summary_updates=assets_summary_updates,
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

    async with MainDB(db_dsn) as async_db:
        await block.write(async_db, chain_event)

    db_assets = db.execute(t.assets_summary_t.select()).fetchall()
    assert len(db_assets) == 1
    assert dict(db_assets[0]) == {'address': '0x1',
                               'asset_address': '0xc1',
                               'tx_number': 1,
                                'nonce': 1,
                               'value': 1000,
                               'decimals': 1,
                               'block_number': 1}

    assets_summary_updates = [{'address': '0x1',
                               'asset_address': '0xc1',
                               'tx_number': 2,
                               'nonce': 2,
                               'value': 2000,
                               'decimals': 1,
                               'block_number': 2}]
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
        token_holders_updates=[],
        transfers=[],
        wallet_events=[],
    )
    chain_event['id'] = 2

    async with MainDB(db_dsn) as async_db:
        await block.write(async_db, chain_event)

    db_assets = db.execute(t.assets_summary_t.select()).fetchall()

    assert len(db_assets) == 1
    assert dict(db_assets[0]) == {'address': '0x1',
                               'asset_address': '0xc1',
                               'tx_number': 2,
                                'nonce': 2,
                               'value': 2000,
                               'decimals': 1,
                               'block_number': 2}

    assets_summary_updates = [{'address': '0x1',
                               'asset_address': '0xc1',
                               'tx_number': 1,
                               'nonce': 1,
                               'value': 1111,
                               'decimals': 1,
                               'block_number': 1}]
    block_data = main_db_dump['blocks'][4]
    block = BlockData(
        block=block_data,
        uncles=[],
        txs=[],
        receipts=[],
        logs=[],
        accounts=[],
        internal_txs=[],
        assets_summary_updates=assets_summary_updates,
        token_holders_updates=[],
        transfers=[],
        wallet_events=[],
    )
    chain_event['id'] = 3

    async with MainDB(db_dsn) as async_db:
        await block.write(async_db, chain_event)

    db_assets = db.execute(t.assets_summary_t.select()).fetchall()

    assert len(db_assets) == 1
    assert dict(db_assets[0]) == {'address': '0x1',
                               'asset_address': '0xc1',
                               'tx_number': 2,
                                'nonce': 2,
                               'value': 2000,
                               'decimals': 1,
                               'block_number': 2}





async def test_apply_chain_split(db, db_dsn):
    """
       1, 0x1, 1, created, NULL, NULL, node1, t1
       2, 0x2, 2, created, NULL, NULL, node1, t2
       3, 0x3, 3, created, NULL, NULL, node1, t3
       4, 0x4, 4, created, NULL, NULL, node1, t4
       5, 0x5, 5, created, NULL, NULL, node1, t5
       6, 0x44, 4, created, NULL, NULL, node1, t6
       7, 0x55, 5, created, NULL, NULL, node1, t6
       8, 0x66, 6, created, NULL, NULL, node1, t6
       9, NULL, NULL, split, NULL, 1, node1, t6
     """
    # given
    db.execute('INSERT INTO blocks (number, hash, parent_hash, is_forked) values (%s, %s, %s, %s)', [
        (1, '0x1', '0x0', False),
        (2, '0x2', '0x1', False),
        (3, '0x3', '0x2', False),
        (4, '0x4', '0x3', False),
        (5, '0x5', '0x4', False),
        (4, '0x44', '0x3', True),
        (5, '0x55', '0x44', True),
        (6, '0x66', '0x55', True),
    ])

    # when
    main_db = MainDB(db_dsn)
    await main_db.connect()

    split_data = {
        'id': 1,
        'block_hash': '0x3',
        'block_number': 3,
        'add_block_hash': '0x66',
        'drop_block_hash': '0x5',
        'add_length': 3,
        'drop_length': 2,
        'common_block_hash': None,
        'common_block_number': None,
        'created_at': datetime.datetime.now(),
        'node_id': '0xXX',
        'parent_block_hash': None,
        'type': 'create'
    }
    await process_chain_split(main_db, split_data)

    # then
    blocks_forks = {b['hash']: b['is_forked'] for b in db.execute(t.blocks_t.select()).fetchall()}
    assert blocks_forks == {
        '0x1': False,
        '0x2': False,
        '0x3': False,
        '0x4': True,
        '0x5': True,
        '0x44': False,
        '0x55': False,
        '0x66': False,
    }

    db_chain_events = db.execute(t.chain_events_t.select()).fetchall()
    assert dict(db_chain_events[0]) == split_data
