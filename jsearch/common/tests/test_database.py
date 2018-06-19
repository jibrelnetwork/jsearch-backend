import os

import pytest
from sqlalchemy import select

from jsearch.common import database, tables as t

@pytest.mark.asyncio
async def test_process_token_transfer(db, contracts, transactions, logs, main_db_data):
    main_db = database.MainDB(os.environ['JSEARCH_MAIN_DB_TEST'])
    await main_db.connect()
    tx_hash = main_db_data['transactions'][2]['hash']
    await main_db.process_token_transfer(main_db_data['transactions'][2]['hash'])
    q = select([t.transactions_t]).where(t.transactions_t.c.hash == tx_hash)
    rows = db.execute(q).fetchall()
    assert rows[0]['is_token_transfer'] == True
    assert rows[0]['token_amount'] == 10
    assert rows[0]['contract_call_description'] == {
        'args': [main_db_data['accounts'][1]['address'], 1000],
        'function': 'transfer'}

    q = select([t.logs_t]).where(t.logs_t.c.transaction_hash == tx_hash)
    rows = db.execute(q).fetchall()
    assert rows[0]['event_type'] == 'Transfer'
    assert rows[0]['event_args'] == {
        'to': main_db_data['accounts'][1]['address'],
        'from': main_db_data['accounts'][0]['address'],
        'value': 1000}