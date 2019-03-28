import pytest
import factory

from jsearch.common.tables import (
    assets_summary_t,
    assets_transfers_t,
)

from jsearch.wallet_worker.__main__ import DatabaseService

pytest_plugins = [
    'jsearch.tests.plugins.service_bus',
    'jsearch.tests.plugins.databases.main_db',
    'jsearch.tests.plugins.databases.factories.transactions',
    'jsearch.tests.plugins.databases.factories.token_transfers',
]


@pytest.mark.usefixtures('mock_service_bus')
async def test_add_or_update_asset_summary_balance(db):
    s = DatabaseService()
    await s.on_start()

    asset_update = {
        'asset_address': '0xc1',
        'address': '0xa1',
        'balance': 100
    }
    await s.add_or_update_asset_summary_balance(asset_update)

    res = db.execute(assets_summary_t.select()).fetchall()
    assert len(res) == 1
    assert dict(res[0]) == {'address': asset_update['address'],
                            'asset_address': asset_update['asset_address'],
                            'balance': str(asset_update['balance']),
                            'tx_number': 1,
                            'nonce': None
                            }

    asset_update = {
        'asset_address': '0xc1',
        'address': '0xa1',
        'balance': 100
    }
    await s.add_or_update_asset_summary_balance(asset_update)

    res = db.execute(assets_summary_t.select()).fetchall()
    assert len(res) == 1
    assert dict(res[0]) == {'address': asset_update['address'],
                            'asset_address': asset_update['asset_address'],
                            'balance': str(asset_update['balance']),
                            'tx_number': 1,
                            'nonce': None
                            }


@pytest.mark.usefixtures('mock_service_bus')
async def test_add_or_update_asset_summary_transfer(db):
    s = DatabaseService()
    await s.on_start()

    asset_transfer = {
        'token_address': '0xc2',
        'address': '0xa2',
    }
    await s.add_or_update_asset_summary_transfer(asset_transfer)

    res = db.execute(assets_summary_t.select()).fetchall()
    assert len(res) == 1
    assert dict(res[0]) == {'address': asset_transfer['address'],
                            'asset_address': asset_transfer['token_address'],
                            'balance': None,
                            'tx_number': 1,
                            'nonce': None
                            }

    await s.add_or_update_asset_summary_transfer(asset_transfer)

    res = db.execute(assets_summary_t.select()).fetchall()
    assert len(res) == 1
    assert dict(res[0]) == {'address': asset_transfer['address'],
                            'asset_address': asset_transfer['token_address'],
                            'balance': None,
                            'tx_number': 2,
                            'nonce': None
                            }


async def test_add_assets_transfer_tx(db, transaction_factory):
    s = DatabaseService()
    await s.on_start()

    tx_data = factory.build(dict, FACTORY_CLASS=transaction_factory)
    await s.add_assets_transafer_tx(tx_data)

    res = db.execute(assets_transfers_t.select()).fetchall()
    assert len(res) == 2
    assert dict(res[0]) == {
        'address': tx_data['from'],
        'type': 'eth-transfer',
        'from': tx_data['from'],
        'to': tx_data['to'],
        'asset_address': None,
        'amount': tx_data['value'],
        'tx_data': tx_data,
        'is_forked': False,
        'block_number': tx_data['block_number'],
        'block_hash': tx_data['block_hash'],
        'ordering': 0
    }
    assert dict(res[1]) == {
        'address': tx_data['to'],
        'type': 'eth-transfer',
        'from': tx_data['from'],
        'to': tx_data['to'],
        'asset_address': None,
        'amount': tx_data['value'],
        'tx_data': tx_data,
        'is_forked': False,
        'block_number': tx_data['block_number'],
        'block_hash': tx_data['block_hash'],
        'ordering': 0
    }


async def test_add_assets_transfer_token_transfer(db, transfer_factory, transaction_factory):
    s = DatabaseService()
    await s.on_start()
    tx = transaction_factory.create()
    tx_data = tx.to_dict()
    del tx_data['address']
    transfer_data = factory.build(dict, FACTORY_CLASS=transfer_factory,
                                  transaction_hash=tx.hash, token_value=5 * 10 ** 18, token_decimals=18)
    await s.add_assets_transfer_token_transfer(transfer_data)

    res = db.execute(assets_transfers_t.select()).fetchall()
    assert len(res) == 2
    assert dict(res[0]) == {'address': transfer_data['from_address'],
                            'amount': '5.0',
                            'asset_address': transfer_data['token_address'],
                            'block_hash': transfer_data['block_hash'],
                            'block_number': transfer_data['block_number'],
                            'from': transfer_data['from_address'],
                            'is_forked': False,
                            'ordering': 0,
                            'to': transfer_data['to_address'],
                            'tx_data': tx_data,
                            'type': 'erc20-transfer'}
    assert dict(res[1]) == {'address': transfer_data['to_address'],
                            'amount': '5.0',
                            'asset_address': transfer_data['token_address'],
                            'block_hash': transfer_data['block_hash'],
                            'block_number': transfer_data['block_number'],
                            'from': transfer_data['from_address'],
                            'is_forked': False,
                            'ordering': 0,
                            'to': transfer_data['to_address'],
                            'tx_data': tx_data,
                            'type': 'erc20-transfer'}
