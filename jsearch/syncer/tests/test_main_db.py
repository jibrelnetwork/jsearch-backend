from jsearch.common import tables as t
from jsearch.syncer.database import MainDBSync, MainDB


async def test_main_db_get_last_synced_block_empty(db_connection_string):
    main_db = MainDB(db_connection_string)
    await main_db.connect()
    res = await main_db.get_latest_synced_block_number([1, None])
    assert res is None


async def test_main_db_get_last_synced_block_no_miss(db, db_connection_string):
    db.execute('INSERT INTO blocks (number, hash) values (%s, %s)', [
        (1, 'aa'),
        (2, 'ab'),
        (3, 'ac'),
        (4, 'ad'),
    ])
    main_db = MainDB(db_connection_string)
    await main_db.connect()
    res = await main_db.get_latest_synced_block_number([1, None])
    assert res == 4


async def test_main_db_get_last_synced_block_has_miss(db, db_connection_string):
    db.execute('INSERT INTO blocks (number, hash) values (%s, %s)', [
        (1, 'aa'),
        (2, 'ab'),
        (4, 'ad'),
    ])
    main_db = MainDB(db_connection_string)
    await main_db.connect()
    res = await main_db.get_latest_synced_block_number([1, None])
    assert res == 4


async def test_main_db_get_missed_blocks_empty(db, db_connection_string):
    main_db = MainDB(db_connection_string)
    await main_db.connect()
    res = await main_db.get_missed_blocks_numbers(10)
    assert res == []


async def test_main_db_get_missed_blocks(db, db_connection_string):
    db.execute('INSERT INTO blocks (number, hash) values (%s, %s)', [
        (1, 'aa'),
        (2, 'ab'),
        (4, 'ad'),
        (5, 'ac'),
        (7, 'ae'),
    ])
    main_db = MainDB(db_connection_string)
    await main_db.connect()
    res = await main_db.get_missed_blocks_numbers(10)
    assert res == [3, 6]


async def test_main_db_get_missed_blocks_limit2(db, db_connection_string):
    db.execute('INSERT INTO blocks (number, hash) values (%s, %s)', [
        (1, 'aa'),
        (2, 'ab'),
        (4, 'ad'),
        (5, 'ac'),
        (7, 'ae'),
        (9, 'af'),
    ])
    main_db = MainDB(db_connection_string)
    await main_db.connect()
    res = await main_db.get_missed_blocks_numbers(2)
    assert res == [3, 6]


def test_maindb_write_block_data(db, main_db_dump, db_connection_string):
    main_db = MainDBSync(db_connection_string)
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

    accounts = []
    for state in accounts_states:
        base = accounts_bases[state['address']]

        account = {}
        account.update(state)
        account.update(base)

        accounts.append(account)

    main_db.write_block_data(
        block_data=block,
        uncles_data=uncles,
        transactions_data=transactions,
        receipts_data=receipts,
        logs_data=logs,
        accounts_data=accounts,
        internal_txs_data=internal_txs
    )

    db_blocks = db.execute(t.blocks_t.select()).fetchall()
    db_transactions = db.execute(t.transactions_t.select()).fetchall()
    db_receipts = db.execute(t.receipts_t.select()).fetchall()
    db_logs = db.execute(t.logs_t.select()).fetchall()
    db_accounts_base = db.execute(t.accounts_base_t.select()).fetchall()
    db_accounts_state = db.execute(t.accounts_state_t.select()).fetchall()

    result_transactions = []
    for tx in transactions:
        rt1 = {}
        rt1.update(tx)
        rt1['address'] = tx['from']
        rt2 = {}
        rt2.update(tx)
        rt2['address'] = tx['to']
        result_transactions.append(rt1)
        result_transactions.append(rt2)

    assert dict(db_blocks[0]) == block
    assert [dict(tx) for tx in db_transactions] == result_transactions
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
            'balance': accounts[0]['balance'],
            'is_forked': False,

        },
        {
            'block_number': accounts[1]['block_number'],
            'block_hash': accounts[1]['block_hash'],
            'address': accounts[1]['address'],
            'nonce': accounts[1]['nonce'],
            'root': accounts[1]['root'],
            'balance': accounts[1]['balance'],
            'is_forked': False,
        },
    ]


async def test_main_db_is_block_exist(db, db_connection_string):
    db.execute('INSERT INTO blocks (number, hash) values (%s, %s)', [
        (1, 'aa'),
    ])
    main_db = MainDBSync(db_connection_string)
    main_db.connect()
    res = main_db.is_block_exist('aa')
    assert res is True
    res = main_db.is_block_exist('xx')
    assert res is False


async def test_apply_chain_split(db, db_connection_string):
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

    main_db = MainDB(db_connection_string)
    await main_db.connect()

    split_data = {
        'common_block_hash': '0x3',
        'common_block_number': 3,
        'add_block_hash': '0x66',
        'drop_block_hash': '0x5',
        'add_length': 3,
        'drop_length': 2
    }
    await main_db.apply_chain_split(split_data)

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