import pytest
from sqlalchemy import and_

from jsearch.common.tables import blocks_t, chain_splits_t, assets_transfers_t, wallet_events_t
from jsearch.syncer.database import RawDB, MainDB
from jsearch.syncer.manager import Manager
from jsearch.syncer.processor import SyncProcessor

pytest_plugins = (
    'jsearch.tests.plugins.databases.main_db',
    'jsearch.tests.plugins.databases.dumps',
    'jsearch.tests.plugins.databases.raw_db',
    'jsearch.tests.plugins.service_bus',
    'jsearch.tests.plugins.databases.main_db',
    'jsearch.tests.plugins.databases.factories.assets_transfers',
    'jsearch.tests.plugins.databases.factories.token_holder',
    'jsearch.tests.plugins.databases.factories.accounts',
    'jsearch.tests.plugins.databases.factories.blocks',
    'jsearch.tests.plugins.databases.factories.token_transfers',
    'jsearch.tests.plugins.databases.factories.contracts',
    'jsearch.tests.plugins.databases.factories.reorgs',
    'jsearch.tests.plugins.databases.factories.wallet_events',
)


@pytest.mark.usefixtures('mock_service_bus_sync_client')
async def test_process_chain_split(raw_db_sample, db, raw_db_connection_string, db_connection_string):
    s = raw_db_sample
    processor = SyncProcessor(raw_db_connection_string, db_connection_string)
    for h in s['headers']:
        processor.sync_block(h['block_hash'])
    raw_db = RawDB(raw_db_connection_string)
    main_db = MainDB(db_connection_string)
    await main_db.connect()
    await raw_db.connect()
    manager = Manager(None, main_db, raw_db, '6000000-')

    splits = raw_db_sample['chain_splits']
    await manager.process_chain_split(splits[0])

    b3f = db.execute(blocks_t.select().where(blocks_t.c.hash == s['headers'][2]['block_hash'])).fetchone()
    b3 = db.execute(blocks_t.select().where(blocks_t.c.hash == s['headers'][3]['block_hash'])).fetchone()
    b4f = db.execute(blocks_t.select().where(blocks_t.c.hash == s['headers'][4]['block_hash'])).fetchone()
    b4 = db.execute(blocks_t.select().where(blocks_t.c.hash == s['headers'][5]['block_hash'])).fetchone()
    b5 = db.execute(blocks_t.select().where(blocks_t.c.hash == s['headers'][6]['block_hash'])).fetchone()
    b5f = db.execute(blocks_t.select().where(blocks_t.c.hash == s['headers'][7]['block_hash'])).fetchone()

    assert b3f.is_forked is True
    assert b3.is_forked is False
    assert b4f.is_forked is True
    assert b4.is_forked is False
    assert b5.is_forked is False
    assert b5f.is_forked is False

    await manager.process_chain_split(splits[1])

    b3f = db.execute(blocks_t.select().where(blocks_t.c.hash == s['headers'][2]['block_hash'])).fetchone()
    b3 = db.execute(blocks_t.select().where(blocks_t.c.hash == s['headers'][3]['block_hash'])).fetchone()
    b4f = db.execute(blocks_t.select().where(blocks_t.c.hash == s['headers'][4]['block_hash'])).fetchone()
    b4 = db.execute(blocks_t.select().where(blocks_t.c.hash == s['headers'][5]['block_hash'])).fetchone()
    b5 = db.execute(blocks_t.select().where(blocks_t.c.hash == s['headers'][6]['block_hash'])).fetchone()
    b5f = db.execute(blocks_t.select().where(blocks_t.c.hash == s['headers'][7]['block_hash'])).fetchone()

    assert b3f.is_forked is True
    assert b3.is_forked is False
    assert b4f.is_forked is True
    assert b4.is_forked is False
    assert b5.is_forked is False
    assert b5f.is_forked is True

    maindb_splits = db.execute(chain_splits_t.select()).fetchall()
    assert dict(maindb_splits[0]) == splits[0]
    assert dict(maindb_splits[1]) == splits[1]

    # assert blocks[1].number == 6000003
    # assert blocks[1].hash.endswith('000')
    # assert blocks[1].is_forked is True
    #
    # assert blocks[2].hash == s['headers'][5]['block_hash']
    # assert blocks[2].is_forked is False
    #
    # assert blocks[3].hash == s['headers'][6]['block_hash']
    # assert blocks[3].is_forked is True

    # assert transactions[0].hash == s['bodies'][0]['fields']['Transactions'][0]['hash']
    # assert transactions[0].block_hash == s['headers'][0]['block_hash']
    # assert transactions[0].block_number == s['headers'][0]['block_number']
    # assert transactions[0].is_forked is False
    # assert transactions[0].transaction_index == 0
    #
    # assert receipts[0].transaction_hash == s['bodies'][0]['fields']['Transactions'][0]['hash']
    # assert receipts[0].block_hash == s['headers'][0]['block_hash']
    # assert receipts[0].block_number == s['headers'][0]['block_number']
    # assert receipts[0].is_forked is False
    # assert receipts[0].transaction_index == 0
    # assert receipts[1].transaction_hash == s['bodies'][0]['fields']['Transactions'][1]['hash']
    # assert receipts[1].transaction_index == 1
    #
    # assert logs[0].transaction_hash == s['bodies'][0]['fields']['Transactions'][2]['hash']
    # assert logs[0].block_hash == s['headers'][0]['block_hash']
    # assert logs[0].block_number == s['headers'][0]['block_number']
    # assert logs[0].is_forked is False
    # assert logs[0].log_index == 0
    #
    # assert logs[1].transaction_hash == s['bodies'][0]['fields']['Transactions'][2]['hash']
    # assert logs[1].block_hash == s['headers'][0]['block_hash']
    # assert logs[1].block_number == s['headers'][0]['block_number']
    # assert logs[1].is_forked is False
    # assert logs[1].log_index == 1
    #
    # assert accounts_base[0]['address'] == s['accounts_state'][0]['address'].lower()
    # assert accounts_state[0]['address'] == s['accounts_state'][0]['address'].lower()
    # assert accounts_state[0]['balance'] == int(s['accounts_state'][0]['fields']['balance'])
    # assert accounts_state[0].is_forked is False

    # TODO: extend test cases


@pytest.mark.usefixtures('mock_service_bus_sync_client', 'mock_service_bus')
@pytest.mark.parametrize("is_forked", [True, False])
async def test_reorganization_for_token_transfers_is_forked_state(
        db,
        account_factory,
        token_factory,
        block_factory,
        transfer_factory,
        reorg_factory,
        raw_db_connection_string,
        db_connection_string,
        is_forked,
):
    from jsearch.common.tables import token_transfers_t
    # given
    # create reorganization event
    token = token_factory.create()
    block = block_factory.create()

    from_account = account_factory.create()
    to_account = account_factory.create()

    transfer_factory.create(
        address=to_account.address,
        from_address=from_account.address,
        to_address=to_account.address,
        block_hash=block.hash,
        block_number=block.number,
        token_address=token.address,
        token_decimals=token.token_decimals,
        token_symbol=token.token_symbol,
        token_name=token.token_name,
        is_forked=not is_forked
    )
    transfer_factory.create(
        address=from_account.address,
        from_address=from_account.address,
        to_address=to_account.address,
        block_hash=block.hash,
        block_number=block.number,
        token_address=token.address,
        token_decimals=token.token_decimals,
        token_symbol=token.token_symbol,
        token_name=token.token_name,
        is_forked=not is_forked
    )

    reorg = reorg_factory.stub(block_hash=block.hash, block_number=block.number, reinserted=not is_forked)

    # when
    # apply reorganization record
    async with RawDB(raw_db_connection_string) as raw_db, MainDB(db_connection_string) as main_db:
        manager = Manager(None, main_db, raw_db, '6000000-')
        await manager.process_reorgs([{
            'block_hash': reorg.block_hash,
            'block_number': reorg.block_number,
            'reinserted': reorg.reinserted,
            'id': reorg.id,
            'node_id': reorg.node_id,
            'header': 'header'  # why do we need pop header?
        }])

    # then
    # check transfer fork status
    result = db.execute(token_transfers_t.select(whereclause=and_(
        token_transfers_t.c.address == to_account.address,
        token_transfers_t.c.to_address == to_account.address,
        token_transfers_t.c.from_address == from_account.address
    ))).fetchone()

    assert result['is_forked'] == is_forked

    result = db.execute(token_transfers_t.select(whereclause=and_(
        token_transfers_t.c.address == from_account.address,
        token_transfers_t.c.to_address == to_account.address,
        token_transfers_t.c.from_address == from_account.address
    ))).fetchone()

    assert result['is_forked'] == is_forked


@pytest.mark.usefixtures('mock_service_bus_sync_client', 'mock_service_bus')
@pytest.mark.parametrize("is_forked", [True, False])
async def test_reorganization_for_assets_transfers_is_forked_state(
        db,
        account_factory,
        assets_transfers_factory,
        token_factory,
        block_factory,
        reorg_factory,
        raw_db_connection_string,
        db_connection_string,
        is_forked,
):
    # given
    # create reorganization event
    token = token_factory.create()
    block = block_factory.create()

    from_account = account_factory.create()
    to_account = account_factory.create()

    assets_transfers_factory.create(
        address=from_account.address,
        from_=from_account.address,
        to=to_account.address,
        block_hash=block.hash,
        block_number=block.number,
        asset_address=token.address,
    )
    assets_transfers_factory.create(
        address=to_account.address,
        from_=from_account.address,
        to=to_account.address,
        block_hash=block.hash,
        block_number=block.number,
        asset_address=token.address,
    )

    reorg = reorg_factory.stub(block_hash=block.hash, block_number=block.number, reinserted=not is_forked)

    # when
    # apply reorganization record
    async with RawDB(raw_db_connection_string) as raw_db, MainDB(db_connection_string) as main_db:
        manager = Manager(None, main_db, raw_db, '6000000-')
        await manager.process_reorgs([{
            'block_hash': reorg.block_hash,
            'block_number': reorg.block_number,
            'reinserted': reorg.reinserted,
            'id': reorg.id,
            'node_id': reorg.node_id,
            'header': 'header'  # why do we need pop header?
        }])

    # then
    # check transfer fork status
    result = db.execute(assets_transfers_t.select(whereclause=and_(
        assets_transfers_t.c.address == to_account.address,
        assets_transfers_t.c.to == to_account.address,
        getattr(assets_transfers_t.c, 'from') == from_account.address
    ))).fetchone()

    assert result['is_forked'] == is_forked

    result = db.execute(assets_transfers_t.select(whereclause=and_(
        assets_transfers_t.c.address == from_account.address,
        assets_transfers_t.c.to == to_account.address,
        getattr(assets_transfers_t.c, 'from') == from_account.address
    ))).fetchone()

    assert result['is_forked'] == is_forked


@pytest.mark.usefixtures('mock_service_bus_sync_client', 'mock_service_bus')
@pytest.mark.parametrize("is_forked", [True, False], ids=("is_true", "is_false"))
async def test_reorganization_for_wallet_events_is_forked_state(
        db,
        block_factory,
        reorg_factory,
        transaction_factory,
        wallet_events_factory,
        raw_db_connection_string,
        db_connection_string,
        is_forked,
):
    # given
    # create reorganization event
    block = block_factory.create()

    tx = transaction_factory.create(block_number=block.number, block_hash=block.hash)
    wallet_events_factory.create(tx_hash=tx.hash, block_hash=block.hash, block_number=block.number)

    reorg = reorg_factory.stub(block_hash=block.hash, block_number=block.number, reinserted=not is_forked)

    # when
    # apply reorganization record
    async with RawDB(raw_db_connection_string) as raw_db, MainDB(db_connection_string) as main_db:
        manager = Manager(None, main_db, raw_db, '6000000-')
        await manager.process_reorgs([{
            'block_hash': reorg.block_hash,
            'block_number': reorg.block_number,
            'reinserted': reorg.reinserted,
            'id': reorg.id,
            'node_id': reorg.node_id,
            'header': 'header'
        }])

    # then
    # check wallet event status
    result = db.execute(wallet_events_t.select(whereclause=and_(
        wallet_events_t.c.tx_hash == tx.hash,
    ))).fetchone()

    assert result['is_forked'] == is_forked
