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
    db_accounts_base = db.execute(t.accounts_base_t.select()).fetchall()
    db_accounts_state = db.execute(t.accounts_state_t.select()).fetchall()

    assert dict(db_blocks[0]) == block
    assert [dict(tx) for tx in db_transactions] == transactions
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
