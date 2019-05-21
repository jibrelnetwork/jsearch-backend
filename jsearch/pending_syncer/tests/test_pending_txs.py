import asyncio
import datetime
import pytest
from psycopg2._json import Json
from sqlalchemy.engine import Engine

from jsearch.common.tables import pending_transactions_t
from jsearch.pending_syncer.services import PendingSyncerService

pytest_plugins = (
    'jsearch.tests.plugins.databases.raw_db',
    'jsearch.tests.plugins.databases.main_db',
    'jsearch.tests.plugins.databases.factories.pending_transactions',
    'jsearch.tests.plugins.service_bus',
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


pytestmark = pytest.mark.asyncio


@pytest.fixture()
async def pending_syncer_service(
        event_loop: asyncio.AbstractEventLoop,
        db_connection_string: str,
        raw_db_connection_string: str,
) -> PendingSyncerService:

    service = PendingSyncerService(
        raw_db_dsn=raw_db_connection_string,
        main_db_dsn=db_connection_string,
        loop=event_loop,
    )

    await service.on_start()
    yield service
    await service.on_stop()


@pytest.mark.usefixtures("mock_service_bus")
async def test_pending_tx_is_not_saved_if_there_is_none(
        db: Engine,
        raw_db_connection_string: str,
        db_connection_string: str,
        pending_syncer_service: PendingSyncerService
) -> None:
    # No pending TXs are in DB.

    await pending_syncer_service.sync_pending_txs()

    pending_txs = db.execute(pending_transactions_t.select()).fetchall()
    pending_txs = [dict(tx) for tx in pending_txs]

    assert pending_txs == []


@pytest.mark.usefixtures("mock_service_bus")
async def test_pending_tx_is_saved_to_main_db(
        db: Engine,
        raw_db: Engine,
        pending_syncer_service: PendingSyncerService
) -> None:
    raw_db.execute(
        """
        INSERT INTO pending_transactions (
          "id",
          "tx_hash",
          "status",
          "fields",
          "timestamp",
          "removed",
          "node_id"
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, [
            (
                48283958,
                '0xdf0237a2edf8f0a5bcdee4d806c7c3c899188d7b8a65dd9d3a4d39af1451a9bc',
                '',
                Json(pending_tx_fields),
                datetime.datetime(2019, 4, 5, 12, 23, 22, 321599),
                False,
                '1',
            ),
        ]
    )

    await pending_syncer_service.sync_pending_txs()

    pending_txs = db.execute(pending_transactions_t.select()).fetchall()
    pending_txs = [dict(tx) for tx in pending_txs]

    assert pending_txs == [
        {
            'last_synced_id': 48283958,
            'hash': '0xdf0237a2edf8f0a5bcdee4d806c7c3c899188d7b8a65dd9d3a4d39af1451a9bc',
            'status': '',
            'timestamp': datetime.datetime(2019, 4, 5, 12, 23, 22, 321599),
            'removed': False,
            'node_id': '1',
            'r': pending_tx_fields['r'],
            's': pending_tx_fields['s'],
            'v': pending_tx_fields['v'],
            'to': pending_tx_fields['to'],
            'from': pending_tx_fields['from'],
            'gas': int(pending_tx_fields['gas'], 16),
            'gas_price': int(pending_tx_fields['gasPrice'], 16),
            'input': pending_tx_fields['input'],
            'nonce': int(pending_tx_fields['nonce'], 16),
            'value': str(int(pending_tx_fields['value'], 16)),
        },
    ]


@pytest.mark.usefixtures("mock_service_bus")
async def test_pending_tx_is_marked_as_removed(
        db: Engine,
        raw_db: Engine,
        pending_syncer_service: PendingSyncerService
) -> None:

    raw_db.execute(
        """
        INSERT INTO pending_transactions (
          "id",
          "tx_hash",
          "status",
          "fields",
          "timestamp",
          "removed",
          "node_id"
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, [
            (
                1,
                '0xdf0237a2edf8f0a5bcdee4d806c7c3c899188d7b8a65dd9d3a4d39af1451a9bc',
                '',
                Json(pending_tx_fields),
                str(datetime.datetime(2019, 4, 5, 12, 23, 22, 321599)),
                False,
                '1',
            ),
            (
                2,
                '0xdf0237a2edf8f0a5bcdee4d806c7c3c899188d7b8a65dd9d3a4d39af1451a9bc',
                '',
                Json({}),
                datetime.datetime(2019, 4, 5, 12, 24, 29, 112052),
                True,
                '1',
            ),
        ]
    )

    await pending_syncer_service.sync_pending_txs()

    pending_txs = db.execute(pending_transactions_t.select()).fetchall()
    pending_txs = [dict(tx) for tx in pending_txs]

    assert pending_txs == [
        {
            'last_synced_id': 2,
            'hash': '0xdf0237a2edf8f0a5bcdee4d806c7c3c899188d7b8a65dd9d3a4d39af1451a9bc',
            'status': '',
            'timestamp': datetime.datetime(2019, 4, 5, 12, 24, 29, 112052),
            'removed': True,
            'node_id': '1',
            'r': pending_tx_fields['r'],
            's': pending_tx_fields['s'],
            'v': pending_tx_fields['v'],
            'to': pending_tx_fields['to'],
            'from': pending_tx_fields['from'],
            'gas': int(pending_tx_fields['gas'], 16),
            'gas_price': int(pending_tx_fields['gasPrice'], 16),
            'input': pending_tx_fields['input'],
            'nonce': int(pending_tx_fields['nonce'], 16),
            'value': str(int(pending_tx_fields['value'], 16)),
        },
    ]


@pytest.mark.usefixtures("mock_service_bus")
async def test_pending_tx_can_be_saved_with_a_big_value(
        db: Engine,
        raw_db: Engine,
        pending_syncer_service: PendingSyncerService
) -> None:

    raw_db.execute(
        """
        INSERT INTO pending_transactions (
          "id",
          "tx_hash",
          "status",
          "fields",
          "timestamp",
          "removed",
          "node_id"
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, [
            (
                1,
                '0xdf0237a2edf8f0a5bcdee4d806c7c3c899188d7b8a65dd9d3a4d39af1451a9bc',
                '',
                Json({**pending_tx_fields, **{'value': '0x314dc6448d9338c15b0a00000000'}}),
                str(datetime.datetime(2019, 4, 5, 12, 23, 22, 321599)),
                False,
                '1',
            ),
        ]
    )

    await pending_syncer_service.sync_pending_txs()

    pending_txs = db.execute(pending_transactions_t.select()).fetchall()
    pending_txs = [dict(tx) for tx in pending_txs]

    assert pending_txs == [
        {
            'last_synced_id': 1,
            'hash': '0xdf0237a2edf8f0a5bcdee4d806c7c3c899188d7b8a65dd9d3a4d39af1451a9bc',
            'status': '',
            'timestamp': datetime.datetime(2019, 4, 5, 12, 23, 22, 321599),
            'removed': False,
            'node_id': '1',
            'r': pending_tx_fields['r'],
            's': pending_tx_fields['s'],
            'v': pending_tx_fields['v'],
            'to': pending_tx_fields['to'],
            'from': pending_tx_fields['from'],
            'gas': int(pending_tx_fields['gas'], 16),
            'gas_price': int(pending_tx_fields['gasPrice'], 16),
            'input': pending_tx_fields['input'],
            'nonce': int(pending_tx_fields['nonce'], 16),
            'value': '1000000000000000000000000000000000',
        },
    ]
