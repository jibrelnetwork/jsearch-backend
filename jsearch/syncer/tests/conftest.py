import pytest
from typing import List, Tuple, Optional


@pytest.fixture(autouse=True)
async def mock_node_calls(mocker):
    async def get_balances(
            owners: List[Tuple[str, str]],
            batch_size: int,
            block: Optional[int] = None
    ) -> List[Tuple[str, str, int]]:
        results = []
        for owner, token in owners:
            results.append((owner, token, 1))
        return results

    async def get_decimals(addresses, batch_size):
        return {address: 18 for address in addresses}

    mocker.patch('jsearch.common.processing.wallet.get_balances', get_balances)
    mocker.patch('jsearch.common.processing.decimals_cache.get_decimals', get_decimals)
