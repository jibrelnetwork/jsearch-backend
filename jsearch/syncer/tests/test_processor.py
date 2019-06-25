import decimal

import pytest

from jsearch.common.processing.accounts import accounts_to_state_and_base_data
from jsearch.common.tables import (
    blocks_t,
    transactions_t,
    receipts_t,
    logs_t,
    accounts_state_t,
    accounts_base_t,
    wallet_events_t,
    internal_transactions_t,
)
from jsearch.common.wallet_events import WalletEventType
from jsearch.syncer.database import RawDB, MainDB
from jsearch.syncer.processor import SyncProcessor, dict_keys_case_convert

pytest_plugins = [
    'jsearch.tests.plugins.databases.main_db',
    'jsearch.tests.plugins.databases.dumps',
    'jsearch.tests.plugins.databases.raw_db',
]


async def call_system_under_test(raw_db_dsn: str, db_dsn: str, block_hash: str):
    async with MainDB(db_dsn) as main_db, RawDB(raw_db_dsn) as raw_db:
        processor = SyncProcessor(raw_db, main_db)
        await processor.sync_block(block_hash=block_hash)


@pytest.fixture
def block_hash(raw_db_sample):
    headers = raw_db_sample['headers']
    return headers[0]['block_hash']


@pytest.fixture
def fixture_bodies(raw_db_sample, block_hash):
    return raw_db_sample['bodies']


@pytest.fixture
def fixture_internal_transactions(raw_db_sample, block_hash):
    return raw_db_sample['internal_transactions']


@pytest.fixture
def fixture_txs(fixture_bodies):
    txs = []
    for body in fixture_bodies:
        for index, tx in enumerate(body['fields']['Transactions']):
            txs.append({
                **tx,
                **{
                    'transaction_index': index,
                    'block_hash': body['block_hash'],
                    'block_number': body['block_number']
                }
            })
    return txs


@pytest.fixture
def fixture_receipts(raw_db_sample, fixture_txs):
    tx_indexes = {tx['hash']: tx['transaction_index'] for tx in fixture_txs}

    receipts = []
    for block in raw_db_sample['receipts']:
        for receipt in block['fields']['Receipts']:
            tx_hash = receipt['transactionHash']
            transaction_index = tx_indexes[tx_hash]

            receipts.append({
                **receipt,
                **{
                    'block_number': block['block_number'],
                    'block_hash': block['block_hash'],
                    'transaction_index': transaction_index
                }
            })
    return receipts


@pytest.fixture
def fixture_logs(fixture_txs, fixture_receipts):
    logs = []
    for receipt in fixture_receipts:
        for log in receipt.get('logs') or []:
            logs.append({
                **log,
                **{
                    'transaction_index': int(log['transactionIndex'], 16),
                    'log_index': int(log['logIndex'], 16),
                    'block_number': int(log['blockNumber'], 16)
                }
            })
    return logs


@pytest.fixture
def fixture_accounts(raw_db_sample):
    accounts = []
    for item in raw_db_sample['accounts']:
        item['address'] = item['address'].lower()
        fields = item.pop('fields')
        accounts.append({
            **item,
            **dict_keys_case_convert(fields)
        })
    return accounts


@pytest.fixture
def block_body(fixture_bodies, block_hash):
    for item in fixture_bodies:
        if item['block_hash'] == block_hash:
            return item


@pytest.fixture
def block_txs(fixture_txs, block_hash):
    return [tx for tx in fixture_txs if tx['block_hash'] == block_hash]


@pytest.fixture
def fixture_block_receipts(fixture_receipts, block_hash):
    return [tx for tx in fixture_receipts if tx['block_hash'] == block_hash]


@pytest.fixture
def block_tx_logs(fixture_logs, block_hash):
    return [tx for tx in fixture_logs if tx['blockHash'] == block_hash]


@pytest.fixture
def block_accounts(fixture_accounts, block_hash):
    return [tx for tx in fixture_accounts if tx['block_hash'] == block_hash]


@pytest.fixture
def block_internal_txs(fixture_internal_transactions, block_hash):
    return [tx for tx in fixture_internal_transactions if tx['block_hash'] == block_hash]


async def test_sync_block_check_blocks(db, raw_db_sample, raw_db_dsn, db_dsn, block_hash, block_body):
    # when
    await call_system_under_test(raw_db_dsn, db_dsn, block_hash)

    # then
    blocks = db.execute(blocks_t.select().order_by(blocks_t.c.number)).fetchall()

    assert len(blocks) == 1
    for block in blocks:
        assert block.hash == block_body['block_hash']
        assert block.number == block_body['block_number']
        assert block.is_forked is False


async def test_sync_block_check_txs(db, raw_db_sample, raw_db_dsn, db_dsn, block_hash, block_txs):
    """
    We test on 6000001 block.
    We can calculate transactions count for test blocks.
    And we can check transactions count in this tests.

    In blocks 6000001 we have:
        119 transactions

    It is historical data and we can freeze it.

    Notes:
        We have denormalization for transactions.
        It leads to records doubling.

        Each events produce two record, which changed by
        address:
          - one record has address from "from" column
          - another record has address from "to" column
    """
    # when
    await call_system_under_test(raw_db_dsn, db_dsn, block_hash)

    # then
    transactions = db.execute(
        transactions_t.select().order_by(transactions_t.c.block_number, transactions_t.c.transaction_index)
    ).fetchall()

    assert len(transactions) == 119 * 2

    for i, origin in zip(range(0, len(block_txs) * 2, 2), block_txs):
        assert transactions[i].hash == origin['hash']
        assert transactions[i].block_hash == origin['block_hash']
        assert transactions[i].block_number == origin['block_number']
        assert transactions[i].transaction_index == origin['transaction_index']
        assert transactions[i].is_forked is False

        # because each tx record create 2 record in main db
        assert transactions[i + 1].hash == origin['hash']
        assert transactions[i + 1].block_hash == origin['block_hash']
        assert transactions[i + 1].block_number == origin['block_number']
        assert transactions[i + 1].transaction_index == origin['transaction_index']
        assert transactions[i + 1].is_forked is False


async def test_sync_block_check_receipts(db, raw_db_sample, raw_db_dsn, db_dsn, block_hash, block_txs):
    # when
    await call_system_under_test(raw_db_dsn, db_dsn, block_hash)

    # then
    receipts = db.execute(
        receipts_t.select().order_by(receipts_t.c.block_number, receipts_t.c.transaction_index)
    ).fetchall()

    for i, origin in enumerate(block_txs):
        assert receipts[i].transaction_hash == origin['hash']
        assert receipts[i].block_hash == origin['block_hash']
        assert receipts[i].block_number == origin['block_number']
        assert receipts[i].is_forked is False


async def test_sync_block_check_logs(db, raw_db_sample, raw_db_dsn, db_dsn, block_hash, block_tx_logs):
    # when
    await call_system_under_test(raw_db_dsn, db_dsn, block_hash)

    # then
    logs = db.execute(
        logs_t.select().order_by(logs_t.c.block_number, logs_t.c.transaction_index, logs_t.c.log_index)
    ).fetchall()

    for i, origin in enumerate(block_tx_logs):
        assert logs[i].transaction_hash == origin['transactionHash']
        assert logs[i].block_hash == origin['blockHash']
        assert logs[i].block_number == origin['block_number']
        assert logs[i].log_index == origin['log_index']
        assert logs[i].is_forked is False


async def test_sync_block_check_accounts(db, raw_db_sample, raw_db_dsn, db_dsn, block_hash, block_accounts):
    # when
    await call_system_under_test(raw_db_dsn, db_dsn, block_hash)

    # then
    accounts_base = db.execute(
        accounts_base_t.select().order_by(accounts_base_t.c.address)
    ).fetchall()

    accounts_state = db.execute(
        accounts_state_t.select().order_by(accounts_state_t.c.block_number, accounts_state_t.c.address)
    ).fetchall()

    block_accounts_state, block_accounts_base = accounts_to_state_and_base_data(block_accounts)

    block_accounts_state = sorted(block_accounts_state, key=lambda x: (x['block_number'], x['address']))
    block_accounts_base = sorted(block_accounts_base, key=lambda x: x['address'])

    for i, origin in enumerate(block_accounts_base):
        assert accounts_base[i]['address'] == origin['address'].lower()

    for i, origin in enumerate(block_accounts_state):
        assert accounts_state[i]['address'] == origin['address'].lower()
        assert accounts_state[i]['balance'] == int(origin['balance'])
        assert accounts_state[i].is_forked is False


async def test_sync_block_check_assets_balances(db, raw_db_sample, raw_db_dsn, db_dsn, block_hash):
    """
    We test on 6000001 block.
    We can calculate events count for test blocks.
    And we can check events count in this tests.

    In blocks 6000001 we have:
        - 20 transfers
        - 69 ether transfers
        - 37 ether transfers from internal txs
        - 35 calls
        - - - - - - - - -
        119 transactions

    It is historical data and we can freeze it.

    Notes:
        We have denormalization for wallet events.
        It leads to records doubling.

        Each events produce two record, which changed by
        address:
          - one record has address from "from" column
          - another record has address from "to" column
    """
    # when
    await call_system_under_test(raw_db_dsn, db_dsn, block_hash)

    # then
    wallet_events = db.execute(
        wallet_events_t.select().order_by(wallet_events_t.c.address, wallet_events_t.c.type)
    ).fetchall()
    wallet_events = list(map(dict, wallet_events))

    ether_transfers = [item for item in wallet_events if item['type'] == WalletEventType.ETH_TRANSFER]
    token_transfers = [item for item in wallet_events if item['type'] == WalletEventType.ERC20_TRANSFER]
    calls = [item for item in wallet_events if item['type'] == WalletEventType.CONTRACT_CALL]

    assert len(token_transfers) == 20 * 2
    assert len(ether_transfers) == (69 + 37) * 2
    assert len(calls) == 35 * 2


async def test_sync_block_check_internal_txs(db, raw_db_sample, raw_db_dsn, db_dsn, block_hash, block_internal_txs):
    # when
    await call_system_under_test(raw_db_dsn, db_dsn, block_hash)

    # then
    internal_txs = db.execute(internal_transactions_t.select()).fetchall()
    internal_txs = [dict(tx) for tx in internal_txs]

    internal_txs = sorted(internal_txs, key=lambda x: (x['parent_tx_hash'], x['transaction_index']))
    block_internal_txs = sorted(block_internal_txs, key=lambda x: (x['parent_tx_hash'], x['index']))

    for i, origin in enumerate(block_internal_txs):
        assert internal_txs[i] == {
            'block_number': origin['block_number'],
            'block_hash': origin['block_hash'],
            'parent_tx_hash': origin['parent_tx_hash'],
            'tx_origin': origin['fields']['TxOrigin'],
            'op': origin['fields']['Operation'],
            'call_depth': origin['fields']['CallDepth'],
            'timestamp': origin['fields']['TimeStamp'],
            'from': origin['fields']['From'],
            'to': origin['fields']['To'],
            'value': decimal.Decimal(origin['fields']['Value']),
            'gas_limit': origin['fields']['GasLimit'],
            'payload': origin['fields']['Payload'],
            'status': origin['fields']['Status'],
            'transaction_index': origin['fields']['Index'],
            'is_forked': False,
        }
