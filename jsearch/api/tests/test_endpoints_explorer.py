import pytest

pytestmark = pytest.mark.usefixtures('disable_metrics_setup')


async def test_get_transaction(cli, main_db_data):
    tx = main_db_data['transactions'][0]
    resp = await cli.get('/v1/transactions/' + tx['hash'])
    assert resp.status == 200
    assert (await resp.json())['data'] == {
        'blockHash': tx['block_hash'],
        'blockNumber': tx['block_number'],
        'timestamp': None,
        'from': tx['from'],
        'gas': tx['gas'],
        'gasPrice': tx['gas_price'],
        'hash': tx['hash'],
        'input': tx['input'],
        'nonce': tx['nonce'],
        'r': tx['r'],
        's': tx['s'],
        'to': tx['to'],
        'status': 0,
        'transactionIndex': tx['transaction_index'],
        'v': tx['v'],
        'value': tx['value'],
    }


async def test_get_receipt(cli, main_db_data):
    r = main_db_data['receipts'][0]
    resp = await cli.get('/v1/receipts/' + r['transaction_hash'])
    assert resp.status == 200
    res = (await resp.json())['data']
    assert res == {
        'blockHash': r['block_hash'],
        'blockNumber': r['block_number'],
        'contractAddress': r['contract_address'],
        'cumulativeGasUsed': str(r['cumulative_gas_used']),
        'from': r['from'],
        'gasUsed': str(r['gas_used']),
        'logs': [
            {'address': main_db_data['logs'][0]['address'],
             'blockHash': main_db_data['logs'][0]['block_hash'],
             'blockNumber': main_db_data['logs'][0]['block_number'],
             'timestamp': main_db_data['logs'][0]['timestamp'],
             'data': main_db_data['logs'][0]['data'],
             'logIndex': main_db_data['logs'][0]['log_index'],
             'removed': main_db_data['logs'][0]['removed'],
             'topics': main_db_data['logs'][0]['topics'],
             'transactionHash': main_db_data['logs'][0]['transaction_hash'],
             'transactionIndex': main_db_data['logs'][0]['transaction_index'],
             }],
        'logsBloom': r['logs_bloom'],
        'root': r['root'],
        'status': r['status'],
        'to': r['to'],
        'transactionHash': r['transaction_hash'],
        'transactionIndex': r['transaction_index'],
    }
