from jsearch.common.tables import blocks_t, transactions_t, receipts_t, logs_t, accounts_base_t, accounts_state_t
from jsearch.syncer.processor import SyncProcessor

pytest_plugins = [
    'jsearch.tests.plugins.databases.main_db',
    'jsearch.tests.plugins.databases.dumps',
    'jsearch.tests.plugins.databases.raw_db',
]


def test_sync_block(raw_db_sample, db, raw_db_connection_string, db_connection_string):
    s = raw_db_sample
    processor = SyncProcessor(raw_db_connection_string, db_connection_string)
    processor.sync_block(s['headers'][0]['block_hash'])

    blocks = db.execute(blocks_t.select()).fetchall()
    transactions = db.execute(transactions_t.select()).fetchall()
    receipts = db.execute(receipts_t.select()).fetchall()
    logs = db.execute(logs_t.select()).fetchall()
    accounts_base = db.execute(accounts_base_t.select()).fetchall()
    accounts_state = db.execute(accounts_state_t.select()).fetchall()

    assert blocks[0].hash == s['headers'][0]['block_hash']
    assert blocks[0].is_forked is False

    assert transactions[0].hash == s['bodies'][0]['fields']['Transactions'][0]['hash']
    assert transactions[0].block_hash == s['headers'][0]['block_hash']
    assert transactions[0].block_number == s['headers'][0]['block_number']
    assert transactions[0].is_forked is False
    assert transactions[0].transaction_index == 0

    assert receipts[0].transaction_hash == s['bodies'][0]['fields']['Transactions'][0]['hash']
    assert receipts[0].block_hash == s['headers'][0]['block_hash']
    assert receipts[0].block_number == s['headers'][0]['block_number']
    assert receipts[0].is_forked is False
    assert receipts[0].transaction_index == 0
    assert receipts[1].transaction_hash == s['bodies'][0]['fields']['Transactions'][1]['hash']
    assert receipts[1].transaction_index == 1

    assert logs[0].transaction_hash == s['bodies'][0]['fields']['Transactions'][2]['hash']
    assert logs[0].block_hash == s['headers'][0]['block_hash']
    assert logs[0].block_number == s['headers'][0]['block_number']
    assert logs[0].is_forked is False
    assert logs[0].log_index == 0

    assert logs[1].transaction_hash == s['bodies'][0]['fields']['Transactions'][2]['hash']
    assert logs[1].block_hash == s['headers'][0]['block_hash']
    assert logs[1].block_number == s['headers'][0]['block_number']
    assert logs[1].is_forked is False
    assert logs[1].log_index == 1

    assert accounts_base[0]['address'] == s['accounts'][0]['address'].lower()
    assert accounts_state[0]['address'] == s['accounts'][0]['address'].lower()
    assert accounts_state[0]['balance'] == int(s['accounts'][0]['fields']['balance'])
    assert accounts_state[0].is_forked is False

    # TODO: extend test cases
