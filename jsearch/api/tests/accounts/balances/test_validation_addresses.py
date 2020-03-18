from typing import Callable, List, Optional, Dict, Any

import pytest
import yarl
from aiohttp.test_utils import TestClient

from jsearch.tests.plugins.databases.factories.accounts import AccountStateFactory
from jsearch.tests.plugins.databases.factories.common import generate_address

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.api,
    pytest.mark.accounts,
    pytest.mark.validation
]


async def test_addresses_validation_extra_spaces_between_addresses(
        cli: TestClient,
        get_url: Callable[[List[str]], yarl.URL],
        account_state_factory: AccountStateFactory
):
    # given
    addresses = [generate_address() for _ in range(3)]
    for address in addresses:
        account_state_factory.create(address=address)

    url = get_url([', '.join(addresses)])

    # when
    resp = await cli.get(str(url))
    resp_json = (await resp.json())['data']

    assert resp.status == 200
    assert {x['address'] for x in resp_json} == {*addresses}


@pytest.mark.parametrize(
    "addresses, status, errors",
    [
        (
                [f'0x0{x}' for x in range(10)],
                200,
                []
        ),
        (
                [f'0x0{x}' for x in range(11)],
                400,
                [
                    {
                        'field': 'addresses',
                        'code': 'INVALID_VALUE',
                        'message': f'Too many items. Must be no more than 10.'
                    }
                ]
        ),
        (
                [],
                400,
                [
                    {
                        'field': "addresses",
                        'message': "Missing data for required field.",
                        'code': "INVALID_VALUE"
                    }

                ]
        )
    ],
    ids=[
        'ok',
        'too_many',
        'empty'
    ]
)
async def test_addresses_validation(
        cli: TestClient,
        get_url: Callable[[List[str]], yarl.URL],
        account_state_factory: AccountStateFactory,
        addresses: List[str],
        status: int,
        errors: Optional[List[Dict[str, Any]]]
):
    # given
    for address in addresses:
        account_state_factory.create(address=address)

    url = get_url(addresses)

    # when
    resp = await cli.get(str(url))
    resp_json = await resp.json()

    # then
    assert resp.status == status
    assert resp_json['status'].get('errors', []) == errors
