import os

import pytest
from sqlalchemy import select

from jsearch.common import database, tables as t
from jsearch.common.contracts import NULL_ADDRESS


@pytest.mark.asyncio
async def test_process_token_transfer(db, contracts, transactions, logs, main_db_data):
    main_db = database.MainDB(os.environ['JSEARCH_MAIN_DB_TEST'])
    await main_db.connect()
    tx_hash = main_db_data['transactions'][2]['hash']
    token_address = main_db_data['accounts'][2]['address']
    holders_q = select([t.token_holders_t]).where(t.token_holders_t.c.token_address == token_address)

    q = t.token_holders_t.insert({
        'token_address': token_address,
        'account_address': main_db_data['accounts'][0]['address'],
        'balance': 100})
    db.execute(q)

    await main_db.process_token_transfer(tx_hash)
    q = select([t.transactions_t]).where(t.transactions_t.c.hash == tx_hash)
    rows = db.execute(q).fetchall()
    assert rows[0]['is_token_transfer'] is True
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

    holders = db.execute(holders_q).fetchall()
    assert len(holders) == 2
    assert dict(holders[0]) == {
        'balance': 10,
        'account_address': main_db_data['accounts'][1]['address'],
        'token_address': token_address,
    }
    assert dict(holders[1]) == {
        'balance': 90,
        'account_address': main_db_data['accounts'][0]['address'],
        'token_address': token_address,
    }
    db.execute(t.token_holders_t.delete())


@pytest.mark.asyncio
async def test_process_token_transfer_constructor(db, contracts, transactions, logs, main_db_data):
    main_db = database.MainDB(os.environ['JSEARCH_MAIN_DB_TEST'])
    tx_hash = main_db_data['transactions'][0]['hash']
    token_address = main_db_data['accounts'][2]['address']
    holders_q = select([t.token_holders_t]).where(t.token_holders_t.c.token_address == token_address)

    await main_db.connect()

    await main_db.process_token_transfer(tx_hash)
    q = select([t.transactions_t]).where(t.transactions_t.c.hash == tx_hash)
    rows = db.execute(q).fetchall()
    assert rows[0]['is_token_transfer'] == False
    # assert rows[0]['token_amount'] == 10000
    # assert rows[0]['contract_call_description'] == {
    #     'args': [main_db_data['accounts'][1]['address'], 1000],
    #     'function': 'transfer'}

    q = select([t.logs_t]).where(t.logs_t.c.transaction_hash == tx_hash)
    rows = db.execute(q).fetchall()
    assert rows[0]['event_type'] == 'Transfer'
    assert rows[0]['event_args'] == {
        'to': main_db_data['accounts'][0]['address'],
        'from': NULL_ADDRESS,
        'value': 100000000000000000000000000}

    holders = db.execute(holders_q).fetchall()
    assert len(holders) == 1
    assert dict(holders[0]) == {
        'balance': 1000000000000000000000000,
        'account_address': main_db_data['accounts'][0]['address'],
        'token_address': token_address,
    }
    db.execute(t.token_holders_t.delete())
