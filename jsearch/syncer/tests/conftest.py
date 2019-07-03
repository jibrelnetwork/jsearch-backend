import pytest
from typing import List, Tuple, Optional

from jsearch.syncer.structs import TokenHolder


@pytest.fixture(autouse=True)
async def mock_node_calls(mocker):
    async def get_balances(
            token_holders: List[TokenHolder],
            batch_size: int,
            block: Optional[int] = None
    ) -> List[Tuple[TokenHolder, int]]:
        results = []
        for holder in token_holders:
            results.append((holder, 1))
        return results

    async def get_decimals(addresses, batch_size):
        return {address: 18 for address in addresses}

    mocker.patch('jsearch.common.processing.wallet.get_balances', get_balances)
    mocker.patch('jsearch.common.processing.decimals_cache.get_decimals', get_decimals)
