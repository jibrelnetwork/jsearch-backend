import pytest
from aiohttp.test_utils import TestClient

from jsearch.tests.plugins.databases.factories.token_holder import TokenHolderFactory

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.api,
    pytest.mark.token_holders,
    pytest.mark.filtration
]


async def test_get_account_balances_multi_with_zero_balance_is_not_included(
        cli: TestClient,
        token_holder_factory: TokenHolderFactory,
):
    # given
    token_holder_factory.create(
        account_address='0x80a7e048f37a50500351c204cb407766fa3bae7f',
        token_address='0x4fcf946bb60aa62e03827c32e8ef94f0943dad35',
        balance=0,
    )

    # when
    resp = await cli.get(
        'v1/accounts/0x80a7e048f37a50500351c204cb407766fa3bae7f/token_balances?'
        'contract_addresses=0x4fcf946bb60aa62e03827c32e8ef94f0943dad35'
    )
    resp_json = await resp.json()

    # then
    assert resp.status == 200
    assert resp_json['data'] == []
