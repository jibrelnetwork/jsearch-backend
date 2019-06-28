import pytest
from typing import List, Tuple, Optional

pytest_plugins = [
    'jsearch.tests.plugins.databases.main_db',
]


@pytest.fixture
async def mock_node_balance_call(mocker):
    async def get_balances(
            owners: List[Tuple[str, str]],
            batch_size: int,
            block: Optional[int] = None
    ) -> List[Tuple[str, str, int]]:
        results = []
        for owner, token in owners:
            results.append((owner, token, 1))
        return results

    mocker.patch('jsearch.common.processing.wallet.get_balances', get_balances)
