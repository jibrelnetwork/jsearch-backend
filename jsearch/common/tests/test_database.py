import os
from unittest import mock

from sqlalchemy import select

from jsearch.common import database, tables as t
from jsearch.common.contracts import NULL_ADDRESS

pytest_plugins = [
    'jsearch.tests.plugins.databases.dumps',
    'jsearch.tests.plugins.databases.main_db',
]


def test_process_token_transfers_ok(db, main_db_data):
    contracts = main_db_data['contracts']

    with mock.patch('jsearch.common.database.tasks') as mock_tasks:
        main_db = database.MainDBSync(os.environ['JSEARCH_MAIN_DB_TEST'])
        main_db.connect()
        tx_hash = main_db_data['transactions'][2]['hash']
        token_address = main_db_data['accounts'][2]['address']
        main_db.get_contract = mock.Mock()
        main_db.get_contract.return_value = contracts[0]
        holders_q = select([t.token_holders_t]).where(t.token_holders_t.c.token_address == token_address)

        main_db.process_token_transfers(tx_hash)
        main_db.disconnect()
        q = select([t.transactions_t]).where(t.transactions_t.c.hash == tx_hash)
        rows = db.execute(q).fetchall()
        assert rows[0]['is_token_transfer'] is True
        assert rows[0]['token_amount'] == 10
        assert rows[0]['contract_call_description'] == {
            'args': [main_db_data['accounts'][1]['address'], 1000],
            'function': 'transfer'}

        q = select([t.logs_t]).where(t.logs_t.c.transaction_hash == tx_hash)
        rows = db.execute(q).fetchall()
        assert dict(rows[0])['event_type'] == 'Transfer'
        assert rows[0]['event_args'] == {
            'to': main_db_data['accounts'][1]['address'],
            'from': main_db_data['accounts'][0]['address'],
            'value': 1000}

        holders = db.execute(holders_q).fetchall()
        assert len(holders) == 0
        mock_tasks.update_token_holder_balance_task.delay.assert_has_calls([
            mock.call(token_address, main_db_data['accounts'][1]['address'], rows[0]['block_number']),
            mock.call(token_address, main_db_data['accounts'][0]['address'], rows[0]['block_number']),
        ])


def test_process_token_transfers_constructor(db, db_connection_string, main_db_data):
    contracts = main_db_data['contracts']

    main_db = database.MainDBSync(db_connection_string)
    tx_hash = main_db_data['transactions'][0]['hash']
    token_address = main_db_data['accounts'][2]['address']
    holders_q = select([t.token_holders_t]).where(t.token_holders_t.c.token_address == token_address)

    main_db.connect()

    main_db.get_contract = mock.Mock()
    main_db.get_contract.return_value = contracts[0]

    main_db.process_token_transfers(tx_hash)
    main_db.disconnect()

    q = select([t.transactions_t]).where(t.transactions_t.c.hash == tx_hash)
    rows = db.execute(q).fetchall()
    assert rows[0]['is_token_transfer'] == False

    q = select([t.logs_t]).where(t.logs_t.c.transaction_hash == tx_hash)
    rows = db.execute(q).fetchall()
    assert rows[0]['event_type'] == 'Transfer'
    assert rows[0]['event_args'] == {
        'to': main_db_data['accounts'][0]['address'],
        'from': NULL_ADDRESS,
        'value': 100000000000000000000000000
    }

    holders = db.execute(holders_q).fetchall()
    assert len(holders) == 0
