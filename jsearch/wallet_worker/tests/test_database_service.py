import factory
import pytest

from jsearch.common.tables import (
    assets_summary_t,
    wallet_events_t,
)
from jsearch.wallet_worker.__main__ import DatabaseService

pytest_plugins = [
    'jsearch.tests.plugins.service_bus',
    'jsearch.tests.plugins.databases.main_db',
    'jsearch.tests.plugins.databases.factories.transactions',
    'jsearch.tests.plugins.databases.factories.token_transfers',
]


@pytest.fixture
async def database_service(db, db_connection_string, loop, mock_service_bus):
    service = DatabaseService(dsn=db_connection_string)
    await service.on_start()
    yield service
    await service.on_stop()


async def test_add_or_update_asset_summary_balance(db, database_service):
    # given
    asset_update = {
        'asset_address': '0xc1',
        'address': '0xa1',
        'value': 100,
        'decimals': 10,
    }
    await database_service.add_or_update_asset_summary_balance(asset_update)

    res = db.execute(assets_summary_t.select()).fetchall()
    assert len(res) == 1
    assert dict(res[0]) == {
        'address': asset_update['address'],
        'asset_address': asset_update['asset_address'],
        'value': asset_update['value'],
        'decimals': asset_update['decimals'],
        'tx_number': 1,
        'nonce': None
    }

    res = db.execute(assets_summary_t.select()).fetchall()
    assert len(res) == 1
    assert dict(res[0]) == {
        'address': asset_update['address'],
        'asset_address': asset_update['asset_address'],
        'value': asset_update['value'],
        'decimals': asset_update['decimals'],
        'tx_number': 1,
        'nonce': None
    }


@pytest.mark.usefixtures('mock_service_bus')
async def test_add_or_update_asset_summary_transfer(db, database_service):
    asset_transfer = {
        'token_address': '0xc2',
        'address': '0xa2',
    }
    await database_service.add_or_update_asset_summary_from_transfer(asset_transfer)

    res = db.execute(assets_summary_t.select()).fetchall()
    assert len(res) == 1
    assert dict(res[0]) == {
        'address': asset_transfer['address'],
        'asset_address': asset_transfer['token_address'],
        'value': None,
        'decimals': None,
        'tx_number': 1,
        'nonce': None
    }

    await database_service.add_or_update_asset_summary_from_transfer(asset_transfer)

    res = db.execute(assets_summary_t.select()).fetchall()
    assert len(res) == 1
    assert dict(res[0]) == {
        'address': asset_transfer['address'],
        'asset_address': asset_transfer['token_address'],
        'value': None,
        'decimals': None,
        'tx_number': 2,
        'nonce': None
    }


async def test_add_wallet_event_token_transfer(db, transfer_factory, transaction_factory, database_service):
    # given
    tx = transaction_factory.create()
    tx_data = tx.to_dict()
    del tx_data['address']
    tx_data['receipt_status'] = 1
    transfer_data = factory.build(
        dict,
        FACTORY_CLASS=transfer_factory,
        transaction_hash=tx.hash,
        token_value=5,
        token_decimals=18
    )
    transfer_data['status'] = 1

    # when
    await database_service.add_wallet_event_token_transfer(transfer_data)

    # then
    res = db.execute(wallet_events_t.select()).fetchall()
    del tx_data['receipt_status']
    assert len(res) == 2

    first, second = map(dict, res)
    assert first == {
        'address': transfer_data['from_address'],
        'block_hash': tx_data['block_hash'],
        'block_number': tx_data['block_number'],
        'event_data': {
            'recipient': transfer_data['to_address'],
            'sender': transfer_data['from_address'],
            'amount': '5',
            'decimals': '18',
            'asset': transfer_data['token_address'],
            'status': 1
        },
        'event_index': 1000 * tx_data['block_number'] + tx_data['transaction_index'],
        'is_forked': False,
        'tx_data': tx_data,
        'tx_hash': tx_data['hash'],
        'type': 'erc20-transfer'
    }
    assert second == {
        'address': transfer_data['to_address'],
        'block_hash': tx_data['block_hash'],
        'block_number': tx_data['block_number'],
        'event_data': {
            'recipient': transfer_data['to_address'],
            'sender': transfer_data['from_address'],
            'amount': '5',
            'decimals': '18',
            'asset': transfer_data['token_address'],
            'status': 1
        },
        'event_index': 1000 * tx_data['block_number'] + tx_data['transaction_index'],
        'is_forked': False,
        'tx_data': tx_data,
        'tx_hash': tx_data['hash'],
        'type': 'erc20-transfer'
    }


async def test_add_wallet_event_tx(db, transaction_factory, database_service):
    # given
    tx = transaction_factory.create(input='0x', value='0xffff')
    tx_data = tx.to_dict()
    del tx_data['address']
    tx_data['to_contract'] = False
    tx_data['receipt_status'] = 1

    # when
    await database_service.add_wallet_event_tx(tx_data)

    # then
    res = db.execute(wallet_events_t.select()).fetchall()
    assert len(res) == 2

    first, second = map(dict, res)
    assert first == {
        'address': tx_data['from'],
        'block_hash': tx_data['block_hash'],
        'block_number': tx_data['block_number'],
        'event_data': {
            'recipient': tx_data['to'],
            'sender': tx_data['from'],
            'amount': '65535',
            'status': 1
        },
        'event_index': 1000 * tx_data['block_number'] + tx_data['transaction_index'],
        'is_forked': False,
        'tx_data': tx_data,
        'tx_hash': tx_data['hash'],
        'type': 'eth-transfer'
    }
    assert second == {
        'address': tx_data['to'],
        'block_hash': tx_data['block_hash'],
        'block_number': tx_data['block_number'],
        'event_data': {
            'recipient': tx_data['to'],
            'sender': tx_data['from'],
            'amount': '65535',
            'status': 1
        },
        'event_index': 1000 * tx_data['block_number'] + tx_data['transaction_index'],
        'is_forked': False,
        'tx_data': tx_data,
        'tx_hash': tx_data['hash'],
        'type': 'eth-transfer'
    }


async def test_add_wallet_event_tx_internal(db, transaction_factory, database_service):
    # given
    tx = transaction_factory.create(input='0x', value='0xffff')
    tx_data = tx.to_dict()
    del tx_data['address']
    tx_data['to_contract'] = False
    tx_data['receipt_status'] = 1

    internal_tx_data = {
        'status': 'success',
        'value': 12000,
        'from': '0xaa',
        'to': '0xbb',
    }

    # when
    await database_service.add_wallet_event_tx_internal(tx_data, internal_tx_data)

    # then
    res = db.execute(wallet_events_t.select()).fetchall()
    assert len(res) == 2

    first, second = map(dict, res)
    assert first == {
        'address': internal_tx_data['from'],
        'block_hash': tx_data['block_hash'],
        'block_number': tx_data['block_number'],
        'event_data': {
            'recipient': internal_tx_data['to'],
            'sender': internal_tx_data['from'],
            'amount': '12000',
            'status': 1
        },
        'event_index': 1000 * tx_data['block_number'] + tx_data['transaction_index'],
        'is_forked': False,
        'tx_data': tx_data,
        'tx_hash': tx_data['hash'],
        'type': 'eth-transfer'
    }
    assert second == {
        'address': internal_tx_data['to'],
        'block_hash': tx_data['block_hash'],
        'block_number': tx_data['block_number'],
        'event_data': {
            'recipient': internal_tx_data['to'],
            'sender': internal_tx_data['from'],
            'amount': '12000',
            'status': 1
        },
        'event_index': 1000 * tx_data['block_number'] + tx_data['transaction_index'],
        'is_forked': False,
        'tx_data': tx_data,
        'tx_hash': tx_data['hash'],
        'type': 'eth-transfer'
    }
