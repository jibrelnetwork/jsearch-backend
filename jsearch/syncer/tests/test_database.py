import os

from jsearch.common import tables as t
from jsearch.syncer.database import MainDBSync


def test_maindb_write_block_data(db, main_db_dump):
    main_db = MainDBSync(os.environ['JSEARCH_MAIN_DB_TEST'])
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
    accounts = for_block(d['accounts'])
    internal_txs = for_block(d['internal_transactions'])

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
    db_accounts = db.execute(t.accounts_t.select()).fetchall()

    assert dict(db_blocks[0]) == block
    assert [dict(tx) for tx in db_transactions] == transactions
    assert [dict(r) for r in db_receipts] == receipts
    assert [dict(l) for l in db_logs] == logs
    assert [dict(a) for a in db_accounts] == accounts

