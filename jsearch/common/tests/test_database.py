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


def test_process_token_transfers_constructor(db, db_connection_string, main_db_data, mocker):
    # given
    mocker.patch('time.sleep')
    mocker.patch(
        'jsearch.common.processing.erc20_transfer_logs.fetch_erc20_token_decimal_bulk',
        lambda updates: [setattr(update, 'decimals', 2) for update in updates] and updates
    )
    mocker.patch(
        'jsearch.common.processing.erc20_transfer_logs.fetch_erc20_balance_bulk',
        lambda updates: [setattr(update, 'value', 100) for update in updates] and updates
    )
    mocker.patch('jsearch.common.processing.transactions.get_contract', return_value=main_db_data['contracts'][0])
    mocker.patch('jsearch.common.processing.erc20_transfer_logs.get_contract',
                 return_value=main_db_data['contracts'][0])

    from jsearch.common import tables as t
    from jsearch.common.contracts import NULL_ADDRESS
    from jsearch.common.database import MainDBSync
    from jsearch.common.processing.transactions import process_token_transfers_for_transaction

    tx_hash = main_db_data['transactions'][0]['hash']
    token_address = main_db_data['accounts_state'][2]['address']

    # when run system under tests
    with MainDBSync(connection_string=db_connection_string) as db_wrapper:
        process_token_transfers_for_transaction(db_wrapper, tx_hash=tx_hash)

    # then check results
    q = select([t.logs_t]).where(t.logs_t.c.transaction_hash == tx_hash)
    log = [dict(row) for row in db.execute(q).fetchall()][0]

    assert log['is_token_transfer'] is True
    assert log['token_transfer_to'] == main_db_data['accounts_state'][0]['address']
    assert log['token_transfer_from'] == NULL_ADDRESS
    assert log['token_amount'] == Decimal('1000000000000000000000000')
    assert log['event_type'] == 'Transfer'
    assert log['event_args'] == {
        'to': main_db_data['accounts_state'][0]['address'],
        'from': NULL_ADDRESS,
        'value': 100000000000000000000000000
    }

    holders_q = select([t.token_holders_t]).where(t.token_holders_t.c.token_address == token_address)
    holders = [dict(item) for item in db.execute(holders_q).fetchall()]
    assert holders == [
        {
            'token_address': token_address,
            'account_address': main_db_data['accounts_state'][0]['address'],
            'balance': Decimal(100)
        }
    ]


def sort_token_holders_key(x):
    return x['token_address'], x['account_address'], x['balance']
