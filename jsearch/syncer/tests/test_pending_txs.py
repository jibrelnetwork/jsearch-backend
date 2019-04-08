from jsearch.common.tables import pending_transactions_t
from jsearch.syncer.database import MainDB
from jsearch.syncer.manager import Manager


pytest_plugins = (
    'jsearch.tests.plugins.databases.main_db',
    'jsearch.tests.plugins.databases.factories.pending_transactions',
)


pending_tx_fields = {
    'r': '0xf337e2c696ea289fd209ec0fc64d29ab74c56d1ca6c334de406f345c11498b66',
    's': '0x6b30c6fde2aa5a3353e812aae44aea1dd53c1194c849fc0e46d8129e21ac5b80',
    'v': '0x1c',
    'to': '0xa15c7ebe1f07caf6bff097d8a589fb8ac49ae5b3',
    'gas': '0x55730',
    'from': '0x19e0466a18e5a375621b327ce781c64b84443a1b',
    'hash': '0xdf0237a2edf8f0a5bcdee4d806c7c3c899188d7b8a65dd9d3a4d39af1451a9bc',
    'input': (
        '0xa9059cbb0000000000000000000000001754d1e1b85c6097017fb6b1b995dc7d0ad5'
        '59dc000000000000000000000000000000000000000000000000f84e3ebf2b6ce5f0'
    ),
    'nonce': '0x5426d',
    'value': '0x0',
    'gasPrice': '0x165a0bc00',
}


async def test_pending_tx_is_saved_to_main_db(db, db_connection_string):
    main_db_wrapper = await MainDB(db_connection_string).connect()
    manager = Manager(None, main_db_wrapper, None, None)

    await manager.process_pending_txs(
        [
            {
                'id': 48283958,
                'tx_hash': '0xdf0237a2edf8f0a5bcdee4d806c7c3c899188d7b8a65dd9d3a4d39af1451a9bc',
                'status': '',
                'fields': pending_tx_fields,
                'timestamp': '2019-04-05 12:23:22.321599',
                'removed': False,
                'node_id': 1
            }
        ]
    )

    pending_txs = db.execute(pending_transactions_t.select()).fetchall()
    pending_txs = [dict(tx) for tx in pending_txs]

    assert pending_txs == [
        {
            'last_synced_id': 48283958,
            'hash': '0xdf0237a2edf8f0a5bcdee4d806c7c3c899188d7b8a65dd9d3a4d39af1451a9bc',
            'status': '',
            'timestamp': '2019-04-05 12:23:22.321599',
            'removed': False,
            'node_id': 1,
            'r': pending_tx_fields['r'],
            's': pending_tx_fields['s'],
            'v': pending_tx_fields['v'],
            'to': pending_tx_fields['to'],
            'from': pending_tx_fields['from'],
            'gas': pending_tx_fields['gas'],
            'gas_price': pending_tx_fields['gasPrice'],
            'input': pending_tx_fields['input'],
            'nonce': pending_tx_fields['nonce'],
            'value': pending_tx_fields['value'],
        },
    ]


async def test_pending_tx_is_marked_as_removed(db, db_connection_string, pending_transaction_factory):
    main_db_wrapper = await MainDB(db_connection_string).connect()
    manager = Manager(None, main_db_wrapper, None, None)

    pending_transaction_factory.create(
        **{
            'last_synced_id': 48283958,
            'hash': '0xdf0237a2edf8f0a5bcdee4d806c7c3c899188d7b8a65dd9d3a4d39af1451a9bc',
            'status': '',
            'timestamp': '2019-04-05 12:23:22.321599',
            'removed': False,
            'node_id': 1,
            'r': pending_tx_fields['r'],
            's': pending_tx_fields['s'],
            'v': pending_tx_fields['v'],
            'to': pending_tx_fields['to'],
            'from': pending_tx_fields['from'],
            'gas': pending_tx_fields['gas'],
            'gas_price': pending_tx_fields['gasPrice'],
            'input': pending_tx_fields['input'],
            'nonce': pending_tx_fields['nonce'],
            'value': pending_tx_fields['value'],
        }
    )

    await manager.process_pending_txs(
        [
            {
                'id': 48285272,
                'tx_hash': '0xdf0237a2edf8f0a5bcdee4d806c7c3c899188d7b8a65dd9d3a4d39af1451a9bc',
                'status': '',
                'fields': dict(),
                'timestamp': '2019-04-05 12:24:29.112052',
                'removed': True,
                'node_id': 1
            }
        ]
    )

    pending_txs = db.execute(pending_transactions_t.select()).fetchall()
    pending_txs = [dict(tx) for tx in pending_txs]

    assert pending_txs == [
        {
            'last_synced_id': 48285272,
            'hash': '0xdf0237a2edf8f0a5bcdee4d806c7c3c899188d7b8a65dd9d3a4d39af1451a9bc',
            'status': '',
            'timestamp': '2019-04-05 12:23:22.321599',
            'removed': True,
            'node_id': 1,
            'r': pending_tx_fields['r'],
            's': pending_tx_fields['s'],
            'v': pending_tx_fields['v'],
            'to': pending_tx_fields['to'],
            'from': pending_tx_fields['from'],
            'gas': pending_tx_fields['gas'],
            'gas_price': pending_tx_fields['gasPrice'],
            'input': pending_tx_fields['input'],
            'nonce': pending_tx_fields['nonce'],
            'value': pending_tx_fields['value'],
        },
    ]
