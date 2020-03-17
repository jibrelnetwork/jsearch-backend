from typing import Callable, List

import pytest
import yarl
from aiohttp.test_utils import TestClient

from jsearch.tests.plugins.databases.factories.accounts import AccountStateFactory

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.api,
    pytest.mark.accounts,
    pytest.mark.filtration
]


@pytest.mark.parametrize(
    'all_accounts, filtered_addresses',
    [
        (
                ('0x1', '0x2'),
                ('0x1',)
        ),
        (
                ('0x1', '0x2'),
                ('0x1', '0x3')
        )
    ],
    ids=[
        'one-from-two',
        'one-and-empty-from-two'
    ]
)
async def test_filtration_by_address(
        cli: TestClient,
        get_url: Callable[[List[str]], yarl.URL],
        account_state_factory: AccountStateFactory,
        all_accounts: List[str],
        filtered_addresses: List[str]
):
    # given
    for address in all_accounts:
        account_state_factory.create(address=address)

    url = get_url(filtered_addresses)

    # when
    resp = await cli.get(str(url))
    resp_json = (await resp.json())['data']

    # then
    assert resp.status == 200
    assert {x['address'] for x in resp_json} == {*filtered_addresses}
