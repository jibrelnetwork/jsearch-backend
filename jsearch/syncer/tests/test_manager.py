from jsearch.common.tables import blocks_t, chain_splits_t
from jsearch.syncer.database import RawDB, MainDB
from jsearch.syncer.manager import Manager
from jsearch.syncer.processor import SyncProcessor


pytest_plugins = [
    'jsearch.tests.plugins.databases.main_db',
    'jsearch.tests.plugins.databases.dumps',
    'jsearch.tests.plugins.databases.raw_db',
]


async def test_process_chain_split(raw_db_sample, db, raw_db_connection_string, db_connection_string):
    s = raw_db_sample
    processor = SyncProcessor(raw_db_connection_string, db_connection_string)
    for h in s['headers']:
        processor.sync_block(h['block_hash'])
    raw_db = RawDB(raw_db_connection_string)
    main_db = MainDB(db_connection_string)
    await main_db.connect()
    await raw_db.connect()
    manager = Manager(None, main_db, raw_db, '6000000-')

    splits = raw_db_sample['chain_splits']
    await manager.process_chain_split(splits[0])

    b3f = db.execute(blocks_t.select().where(blocks_t.c.hash == s['headers'][2]['block_hash'])).fetchone()
    b3 = db.execute(blocks_t.select().where(blocks_t.c.hash == s['headers'][3]['block_hash'])).fetchone()
    b4f = db.execute(blocks_t.select().where(blocks_t.c.hash == s['headers'][4]['block_hash'])).fetchone()
    b4 = db.execute(blocks_t.select().where(blocks_t.c.hash == s['headers'][5]['block_hash'])).fetchone()
    b5 = db.execute(blocks_t.select().where(blocks_t.c.hash == s['headers'][6]['block_hash'])).fetchone()
    b5f = db.execute(blocks_t.select().where(blocks_t.c.hash == s['headers'][7]['block_hash'])).fetchone()

    assert b3f.is_forked is True
    assert b3.is_forked is False
    assert b4f.is_forked is True
    assert b4.is_forked is False
    assert b5.is_forked is False
    assert b5f.is_forked is False

    await manager.process_chain_split(splits[1])

    b3f = db.execute(blocks_t.select().where(blocks_t.c.hash == s['headers'][2]['block_hash'])).fetchone()
    b3 = db.execute(blocks_t.select().where(blocks_t.c.hash == s['headers'][3]['block_hash'])).fetchone()
    b4f = db.execute(blocks_t.select().where(blocks_t.c.hash == s['headers'][4]['block_hash'])).fetchone()
    b4 = db.execute(blocks_t.select().where(blocks_t.c.hash == s['headers'][5]['block_hash'])).fetchone()
    b5 = db.execute(blocks_t.select().where(blocks_t.c.hash == s['headers'][6]['block_hash'])).fetchone()
    b5f = db.execute(blocks_t.select().where(blocks_t.c.hash == s['headers'][7]['block_hash'])).fetchone()

    assert b3f.is_forked is True
    assert b3.is_forked is False
    assert b4f.is_forked is True
    assert b4.is_forked is False
    assert b5.is_forked is False
    assert b5f.is_forked is True

    maindb_splits = db.execute(chain_splits_t.select()).fetchall()
    assert dict(maindb_splits[0]) == splits[0]
    assert dict(maindb_splits[1]) == splits[1]

    # assert blocks[1].number == 6000003
    # assert blocks[1].hash.endswith('000')
    # assert blocks[1].is_forked is True
    #
    # assert blocks[2].hash == s['headers'][5]['block_hash']
    # assert blocks[2].is_forked is False
    #
    # assert blocks[3].hash == s['headers'][6]['block_hash']
    # assert blocks[3].is_forked is True

    # assert transactions[0].hash == s['bodies'][0]['fields']['Transactions'][0]['hash']
    # assert transactions[0].block_hash == s['headers'][0]['block_hash']
    # assert transactions[0].block_number == s['headers'][0]['block_number']
    # assert transactions[0].is_forked is False
    # assert transactions[0].transaction_index == 0
    #
    # assert receipts[0].transaction_hash == s['bodies'][0]['fields']['Transactions'][0]['hash']
    # assert receipts[0].block_hash == s['headers'][0]['block_hash']
    # assert receipts[0].block_number == s['headers'][0]['block_number']
    # assert receipts[0].is_forked is False
    # assert receipts[0].transaction_index == 0
    # assert receipts[1].transaction_hash == s['bodies'][0]['fields']['Transactions'][1]['hash']
    # assert receipts[1].transaction_index == 1
    #
    # assert logs[0].transaction_hash == s['bodies'][0]['fields']['Transactions'][2]['hash']
    # assert logs[0].block_hash == s['headers'][0]['block_hash']
    # assert logs[0].block_number == s['headers'][0]['block_number']
    # assert logs[0].is_forked is False
    # assert logs[0].log_index == 0
    #
    # assert logs[1].transaction_hash == s['bodies'][0]['fields']['Transactions'][2]['hash']
    # assert logs[1].block_hash == s['headers'][0]['block_hash']
    # assert logs[1].block_number == s['headers'][0]['block_number']
    # assert logs[1].is_forked is False
    # assert logs[1].log_index == 1
    #
    # assert accounts_base[0]['address'] == s['accounts_state'][0]['address'].lower()
    # assert accounts_state[0]['address'] == s['accounts_state'][0]['address'].lower()
    # assert accounts_state[0]['balance'] == int(s['accounts_state'][0]['fields']['balance'])
    # assert accounts_state[0].is_forked is False

    # TODO: extend test cases
