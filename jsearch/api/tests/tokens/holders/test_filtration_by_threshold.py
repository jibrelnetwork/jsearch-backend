from typing import Callable, Iterable, Any, Set, NamedTuple, List

import pytest
import yarl
from aiohttp.test_utils import TestClient

from jsearch.tests.plugins.databases.factories.token_descriptions import TokenDescriptionsFactory

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.api,
    pytest.mark.token_holders,
    pytest.mark.filtration
]


class Threshold(NamedTuple):
    block_number: int
    total_supply: int
    is_forked: bool


@pytest.mark.parametrize(
    "thresholds, balances, awaited_balances",
    (
            ([], (500, 1000), {500, 1000}),
            (
                    [
                        Threshold(block_number=1, total_supply=100000, is_forked=False)
                    ],
                    (500, 1000),
                    {1000}),
            (
                    [
                        Threshold(block_number=1, total_supply=10000, is_forked=False),
                        Threshold(block_number=2, total_supply=100000, is_forked=False)
                    ],
                    (500, 1000),
                    {1000}
            ),
            (
                    [
                        Threshold(block_number=2, total_supply=10000, is_forked=True),
                        Threshold(block_number=2, total_supply=100000, is_forked=False)
                    ],
                    (500, 1000),
                    {1000}
            )
    ),
    ids=(
            "None",
            "low",
            "threshold_history",
            "forked"
    )
)
async def test_filter_by_threshold(
        cli: TestClient,
        token_address: str,
        thresholds: List[Threshold],
        balances: Iterable[int],
        awaited_balances: Set[int],
        create_token_holders: Callable[..., Any],
        token_descriptions_factory: TokenDescriptionsFactory,
        url: yarl.URL
):
    # given
    create_token_holders(token_address, balances)
    for threshold in thresholds:
        token_descriptions_factory.create(
            address=token_address,
            total_supply=threshold.total_supply,
            block_number=threshold.block_number,
            is_forked=threshold.is_forked
        )

    # when
    resp = await cli.get(str(url))
    resp_json = await resp.json()

    # then
    gotten_balances = {item['balance'] for item in resp_json['data']}
    assert gotten_balances == awaited_balances
