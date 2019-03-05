from decimal import Decimal

import factory
import pytest
from sqlalchemy import and_

pytest_plugins = (
    'jsearch.tests.plugins.databases.main_db',
    'jsearch.tests.plugins.databases.factories.token_holder',
    'jsearch.tests.plugins.databases.factories.accounts',
    'jsearch.tests.plugins.databases.factories.blocks',
    'jsearch.tests.plugins.databases.factories.token_transfers',
    'jsearch.tests.plugins.databases.factories.contracts',
    'jsearch.tests.plugins.databases.factories.reorgs',
)


@pytest.mark.usefixtures('worker')
async def test_handle_reorganization(db,
                                     account_factory,
                                     block_factory,
                                     token_holder_factory: factory.Factory,
                                     token_factory,
                                     transfer_factory,
                                     reorg_factory,
                                     mock_fetch_erc20_balance_bulk,
                                     mock_service_bus_get_contracts):
    from jsearch.common.tables import token_holders_t
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
        is_forked=False
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
        is_forked=False
    )

    token_holder_factory.create(token_address=token.address, account_address=to_account.address)
    token_holder_factory.create(token_address=token.address, account_address=from_account.address)

    reorg = reorg_factory.create(block_hash=block.hash, block_number=block.number)

    mock_service_bus_get_contracts[token.address] = {
        'address': token.address,
        'abi': token.abi,
        'token_decimals': token.token_decimals
    }
    mock_fetch_erc20_balance_bulk[token.address][to_account.address] = 100
    mock_fetch_erc20_balance_bulk[token.address][from_account.address] = 150

    # when
    # handle reorganization event
    from jsearch.worker.__main__ import handle_block_reorganization

    await handle_block_reorganization(
        block_number=reorg.block_number,
        block_hash=reorg.block_hash,
        reinserted=reorg.reinserted
    )

    # then
    # check what balance was updated
    to_account_balance = db.execute(
        token_holders_t.select(whereclause=and_(
            token_holders_t.c.token_address == token.address,
            token_holders_t.c.account_address == to_account.address
        ))
    ).fetchone()
    assert to_account_balance['balance'] == Decimal(100), "balance wasn't updated due to reorg event applying"

    from_account_balance = db.execute(
        token_holders_t.select(whereclause=and_(
            token_holders_t.c.token_address == token.address,
            token_holders_t.c.account_address == from_account.address
        ))
    ).fetchone()
    assert from_account_balance['balance'] == Decimal(150), "balance wasn't updated due to reorg event applying"
