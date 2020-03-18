from typing import Callable, List

import pytest
import yarl
from aiohttp.test_utils import TestClient

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.api,
    pytest.mark.accounts,
    pytest.mark.smoke
]


@pytest.mark.parametrize(
    'addresses',
    [
        ('0xempty',),
        ('0xempty', '0xfoo'),
        ('0xempty', '0xfoo', '0xbar')
    ],
    ids=[
        'empty',
        'empty x2',
        'empty x3'
    ]
)
async def test_200_empty_address(
        cli: TestClient,
        get_url: Callable[[List[str]], yarl.URL],
        addresses: List[str]
) -> None:
    # given
    url = get_url(addresses)

    # when
    response = await cli.get(str(url))

    # then
    response_json = await response.json()

    assert response.status == 200
    assert response_json['status']['success'] is True

    data = response_json['data']

    assert {x['address'] for x in data} == set(addresses)
    assert {x['balance'] for x in data} == {"0" for _ in range(len(addresses))}
