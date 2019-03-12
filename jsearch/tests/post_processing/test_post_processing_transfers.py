from decimal import Decimal

import pytest
from sqlalchemy import select

from jsearch.tests.post_processing.test_post_processing_logs import sort_token_holders_key

pytest_plugins = [
    'jsearch.tests.plugins.databases.dumps',
    'jsearch.tests.plugins.databases.main_db',
    'jsearch.tests.plugins.post_processing.transfers',
    'jsearch.tests.plugins.service_bus',
    'jsearch.tests.plugins.executor',
    'jsearch.tests.plugins.last_block'
]


@pytest.mark.usefixtures('mock_service_bus', 'mock_executor')
async def test_post_processing_account_balance(mocker,
                                               mock_last_block_consumer,
                                               db_connection_string,
                                               main_db_data,
                                               post_processing_transfers,
                                               token_address,
                                               transfers):
    from jsearch.common.tables import token_holders_t
    from jsearch.syncer.database import MainDBSync
    # given
    # get transfers from logs
    mock_last_block_consumer({"number": 6000000})
    mocker.patch('time.sleep')

    # when run system under test
    await post_processing_transfers(transfers)

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
