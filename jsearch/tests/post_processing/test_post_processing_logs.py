from decimal import Decimal

import pytest

pytest_plugins = [
    'jsearch.tests.plugins.databases.dumps',
    'jsearch.tests.plugins.databases.main_db',
    'jsearch.tests.plugins.post_processing.logs',
    'jsearch.tests.plugins.service_bus',
    'jsearch.tests.plugins.executor'
]


@pytest.mark.usefixtures('mock_service_bus', 'mock_executor')
async def test_post_processing_logs(mocker,
                                    main_db_data,
                                    transaction,
                                    logs,
                                    transfers,
                                    post_processing_logs,
                                    load_logs,
                                    load_transfers,
                                    get_erc20_transfers_from_kafka):
    # given
    mocker.patch('time.sleep')

    # when run system under test
    await post_processing_logs(logs)

    # then check results
    loaded_logs = load_logs(transaction['hash'], transaction['block_hash'])
    loaded_transfers = load_transfers(transaction['hash'], transaction['block_hash'])

    assert len(loaded_logs) == 1
    assert len(loaded_transfers) == 2

    # check logs was updated
    for log in loaded_logs:
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

    # check transfers
    for transfer in loaded_transfers:
        assert transfer['token_value'] == 1000

    transfers = sorted(transfers, key=lambda x: x['address'])

    items = sorted(loaded_transfers, key=lambda x: x['address'])
    assert items == transfers

    # check transfers was send to service bus
    items = sorted(get_erc20_transfers_from_kafka(), key=lambda x: x['address'])
    assert items == transfers


def sort_token_holders_key(x):
    return x['token_address'], x['account_address'], x['balance']
