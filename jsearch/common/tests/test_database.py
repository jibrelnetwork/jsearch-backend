from decimal import Decimal

from sqlalchemy import select

pytest_plugins = [
    'jsearch.tests.plugins.databases.dumps',
    'jsearch.tests.plugins.databases.main_db',
]


def test_process_logs_transfers_ok(db, db_connection_string, main_db_data, mocker, post_processing):
    # given
    mocker.patch('time.sleep')
    from jsearch.common import tables as t

    tx_hash = main_db_data['transactions'][2]['hash']
    token_address = main_db_data['accounts_state'][2]['address']

    # when run system under test
    post_processing(main_db_data)

    # then check results
    q = select([t.logs_t]).where(t.logs_t.c.transaction_hash == tx_hash)
    log = [dict(row) for row in db.execute(q).fetchall()][0]

    assert log['is_token_transfer'] is True
    assert log['token_transfer_to'] == main_db_data['accounts_state'][1]['address']
    assert log['token_transfer_from'] == main_db_data['accounts_state'][0]['address']
    assert log['token_amount'] == 10
    assert log['event_type'] == 'Transfer'
    assert log['event_args'] == {
        'to': main_db_data['accounts_state'][1]['address'],
        'from': main_db_data['accounts_state'][0]['address'],
        'value': 1000
    }

    holders_q = select([t.token_holders_t]).where(t.token_holders_t.c.token_address == token_address)
    holders = [dict(item) for item in db.execute(holders_q).fetchall()]

    assert sorted(holders, key=sort_token_holders_key) == sorted([
        {
            'token_address': token_address,
            'account_address': main_db_data['accounts_state'][0]['address'],
            'balance': Decimal(100)
        },
        {
            'token_address': token_address,
            'account_address': main_db_data['accounts_state'][1]['address'],
            'balance': Decimal(100)
        },
    ], key=sort_token_holders_key)


def sort_token_holders_key(x):
    return x['token_address'], x['account_address'], x['balance']
