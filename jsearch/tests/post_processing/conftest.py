import pytest

from jsearch.common.processing.erc20_transfers import logs_to_transfers


@pytest.fixture()
def token_address(main_db_data):
    return main_db_data['accounts_state'][2]['address']


@pytest.fixture()
def transaction(main_db_data):
    return main_db_data['transactions'][2]


@pytest.fixture()
def receipt(main_db_data):
    return main_db_data['receipts'][2]


@pytest.fixture()
def logs(main_db_data):
    tx_hash = main_db_data['transactions'][2]['hash']

    logs = []
    for item in main_db_data['logs']:
        if item['transaction_hash'] == tx_hash:
            item = dict(item, **{
                'status': 1,
            })
            logs.append(item)
    return logs


@pytest.fixture()
def processed_logs(logs):
    from jsearch.common.processing.logs import process_log_event
    return [process_log_event(log) for log in logs]


@pytest.fixture()
def transfers(main_db_data, processed_logs):
    block_hashes = [log['block_hash'] for log in processed_logs]
    blocks = {block['hash']: block for block in main_db_data['blocks'] if block['hash'] in block_hashes}

    addresses = [log['address'] for log in processed_logs]
    contracts = {x['address']: {'decimals': 10, **x} for x in main_db_data['contracts'] if x['address'] in addresses}

    return logs_to_transfers(processed_logs, blocks, contracts)
