from aiohttp.test_utils import TestClient

from jsearch.tests.plugins.databases.factories.logs import LogFactory
from jsearch.tests.plugins.databases.factories.receipts import ReceiptFactory
from jsearch.tests.plugins.databases.factories.transactions import TransactionFactory


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


async def test_get_transaction_does_not_return_data_from_fork(
        cli: TestClient,
        transaction_factory: TransactionFactory,
) -> None:
    transaction = transaction_factory.create(is_forked=True)

    resp = await cli.get('/v1/transactions/' + transaction.hash)
    resp_json = await resp.json()

    assert resp_json == {
        "status": {
            "success": False,
            "errors": [
                {
                    "code": "NOT_FOUND",
                    "message": "Resource not found",
                }
            ],
        },
        "data": None,
    }


async def test_get_receipt_does_not_return_data_from_fork(
        cli: TestClient,
        receipt_factory: ReceiptFactory,
) -> None:
    receipt = receipt_factory.create(is_forked=True)

    resp = await cli.get('/v1/receipts/' + receipt.transaction_hash)
    resp_json = await resp.json()

    assert resp_json == {
        "status": {
            "success": False,
            "errors": [
                {
                    "code": "NOT_FOUND",
                    "message": "Resource not found",
                }
            ],
        },
        "data": None,
    }


async def test_get_receipt_does_not_return_logs_data_from_fork(
        cli: TestClient,
        receipt_factory: ReceiptFactory,
        log_factory: LogFactory,
) -> None:
    receipt = receipt_factory.create(is_forked=False)
    log_factory.create_for_receipt(receipt, is_forked=True)

    resp = await cli.get('/v1/receipts/' + receipt.transaction_hash)
    resp_json = await resp.json()

    assert resp_json["data"]["logs"] == []
