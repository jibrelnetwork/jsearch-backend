from decimal import Decimal
from functools import partial

import pytest
from sqlalchemy import select

pytest_plugins = [
    'jsearch.tests.plugins.databases.dumps',
    'jsearch.tests.plugins.databases.main_db',
    'jsearch.tests.plugins.post_processing.transfers',
    'jsearch.tests.plugins.service_bus',
    'jsearch.tests.plugins.executor',
    'jsearch.tests.plugins.last_block'
]


@pytest.mark.usefixtures('mock_service_bus', 'mock_executor')
async def test_post_processing_logs_to_transfer_transition(transaction,
                                                           logs,
                                                           transfers,
                                                           load_transfers,
                                                           mock_last_block_consumer,
                                                           get_erc20_transfers_from_kafka,
                                                           post_processing_transfers,
                                                           token_address):
    # given
    mock_last_block_consumer({"number": 7000000})

    # when
    await post_processing_transfers(logs)

    # then

    loaded_transfers = load_transfers(transaction['hash'], transaction['block_hash'])
    assert len(loaded_transfers) == 2
    # check transfers
    for transfer in loaded_transfers:
        assert transfer['token_value'] == 1000

    sort_items = partial(sorted, key=lambda x: x['address'])

    result = sort_items(loaded_transfers)
    expected_transfers = sort_items(transfers)

    assert result == expected_transfers


@pytest.mark.usefixtures('mock_service_bus', 'mock_executor')
async def test_post_processing_account_balance(mocker,
                                               mock_last_block_consumer,
                                               db_connection_string,
                                               main_db_data,
                                               post_processing_transfers,
                                               token_address,
                                               processed_logs):
    from jsearch.common.tables import token_holders_t
    from jsearch.syncer.database import MainDBSync
    # given
    # get transfers from logs
    mock_last_block_consumer({"number": 7000000})
    mocker.patch('time.sleep')

    # when run system under test
    await post_processing_transfers(processed_logs)

    # then
    with MainDBSync(db_connection_string) as db:
        holders_q = select(token_holders_t.c).where(token_holders_t.c.token_address == token_address)
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
