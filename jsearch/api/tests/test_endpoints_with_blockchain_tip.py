from typing import Callable, Awaitable

import pytest
from aiohttp.test_utils import TestClient

from jsearch.api.storage import Storage
from jsearch.api.structs import BlockchainTip, BlockInfo
from jsearch.tests.plugins.databases.factories.accounts import AccountStateFactory
from jsearch.tests.plugins.databases.factories.blocks import BlockFactory
from jsearch.tests.plugins.databases.factories.chain_splits import ChainSplitFactory
from jsearch.tests.plugins.databases.factories.reorgs import ReorgFactory

TipGetter = Callable[[bool], Awaitable[BlockchainTip]]

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.usefixtures('disable_metrics_setup'),
]


@pytest.fixture()
def _get_tip(
        storage: Storage,
        block_factory: BlockFactory,
        chain_split_factory: ChainSplitFactory,
        reorg_factory: ReorgFactory,
) -> Callable[[bool], Awaitable[BlockchainTip]]:

    async def inner(is_forked: bool) -> BlockchainTip:
        common_block = block_factory.create(number=0, hash='0x111')

        canonical_block = block_factory.create(parent_hash=common_block.hash, number=1, hash='0x222a')
        forked_block = block_factory.create(parent_hash=common_block.hash, number=1, hash='0x222b')

        chain_splits = chain_split_factory.create(
            common_block_hash=common_block.hash,
            common_block_number=common_block.number,
        )
        reorg_factory.create(
            block_hash=forked_block.hash,
            block_number=forked_block.number,
            split_id=chain_splits.id,
        )

        tip_block = forked_block if is_forked else canonical_block
        tip_block_info = BlockInfo(number=tip_block.number, hash=tip_block.hash)
        tip = await storage.get_blockchain_tip(tip_block_info)

        return tip

    return inner


@pytest.mark.parametrize('is_tip_stale', (False,))
async def test_get_accounts_balances_with_tip(
        cli: TestClient,
        account_state_factory: AccountStateFactory,
        is_tip_stale: bool,
        _get_tip: TipGetter,
) -> None:

    tip = await _get_tip(is_tip_stale)

    # Make sure, that data is not historical.
    account_state_block_number = tip.tip_number + 1
    account_state = account_state_factory.create(
        address='0xcd424c53f5dc7d22cdff536309c24ad87a97e6af',
        block_number=account_state_block_number,
        balance=256391824440000,
    )

    response = await cli.get(f'/v1/accounts/balances?addresses={account_state.address}&blockchain_tip={tip.tip_hash}')
    response_json = await response.json()

    data = [] if is_tip_stale else [
        {
            "balance": hex(256391824440000),
            "address": "0xcd424c53f5dc7d22cdff536309c24ad87a97e6af"
        },
    ]

    assert response_json == {
        "status": {
            "success": True,
            "errors": []
        },
        "data": data,
    }
