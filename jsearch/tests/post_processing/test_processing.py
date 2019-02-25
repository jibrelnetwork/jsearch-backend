from decimal import Decimal

import pytest
from sqlalchemy import select

pytest_plugins = [
    'jsearch.tests.plugins.databases.dumps',
    'jsearch.tests.plugins.databases.main_db',
    'jsearch.tests.plugins.post_processing',
    'jsearch.tests.plugins.service_bus'
]


@pytest.mark.usefixtures('mock_service_bus')
async def test_post_processing_logs(mocker, main_db_data, post_processing_logs):
    # given
    mocker.patch('time.sleep')

    tx_hash = main_db_data['transactions'][2]['hash']
    logs = [log for log in main_db_data['logs'] if log['transaction_hash'] == tx_hash]

    # when run system under test
    logs, transfers = await post_processing_logs(logs)

    # then check results
    assert len(logs) == 1
    assert len(transfers) == 1

    for log in logs:
        assert log['is_token_transfer'] is True
        assert log['token_transfer_to'] == main_db_data['accounts_state'][1]['address']
        assert log['token_transfer_from'] == main_db_data['accounts_state'][0]['address']
        assert log['token_amount'] == Decimal(1000)
        assert log['event_type'] == 'Transfer'
        assert log['event_args'] == {
            'to': main_db_data['accounts_state'][1]['address'],
            'from': main_db_data['accounts_state'][0]['address'],
            'value': 1000
        }


@pytest.mark.usefixtures('mock_service_bus')
async def test_post_processing_transfers(mocker, main_db_data, post_processing):
    from jsearch.common.processing.erc20_transfers import logs_to_transfers
    # given
    mocker.patch('time.sleep')

    tx_hash = main_db_data['transactions'][2]['hash']
    logs = [log for log in main_db_data['logs'] if log['transaction_hash'] == tx_hash]

    # when run system under test
    _, transfers = await post_processing(logs)

    # then
    block_hashes = [log['block_hash'] for log in logs]
    blocks = {block['hash']: block for block in main_db_data['blocks'] if block['hash'] in block_hashes}

    addresses = [log['address'] for log in logs]
    contracts = {x['address']: {'decimals': 10, **x} for x in main_db_data['contracts'] if x['address'] in addresses}

    assert len(transfers) == 2

    items = sorted(transfers, key=lambda x: x['address'])
    expected = sorted(logs_to_transfers(logs, blocks, contracts), key=lambda x: x['address'])

    assert items == expected


@pytest.mark.usefixtures('mock_service_bus')
async def test_post_processing_account_balance(mocker, db_connection_string, main_db_data, post_processing):
    from jsearch.common.tables import token_holders_t
    from jsearch.syncer.database import MainDBSync
    # given
    mocker.patch('time.sleep')

    tx_hash = main_db_data['transactions'][2]['hash']
    token_address = main_db_data['accounts_state'][2]['address']
    logs = [log for log in main_db_data['logs'] if log['transaction_hash'] == tx_hash]

    # when run system under test
    _, transfers = await post_processing(logs)

    # then
    with MainDBSync(db_connection_string) as db:
        holders_q = select(token_holders_t.c).where(
            token_holders_t.c.token_address == token_address
        )
        holders = [dict(item) for item in db.execute(holders_q).fetchall()]

    assert sorted(holders, key=sort_token_holders_key) == sorted([
        {
            'token_address': token_address,
            'account_address': main_db_data['accounts_state'][0]['address'],
            'balance': Decimal(100),
            'decimals': 10,
        },
        {
            'token_address': token_address,
            'account_address': main_db_data['accounts_state'][1]['address'],
            'balance': Decimal(100),
            'decimals': 10
        },
    ], key=sort_token_holders_key)


def sort_token_holders_key(x):
    return x['token_address'], x['account_address'], x['balance']
