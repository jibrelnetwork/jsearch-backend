import logging
from urllib.parse import urlencode

import pytest
from aiohttp.test_utils import TestClient
from typing import List, Callable, Tuple, Optional, Dict

from jsearch.api.tests.utils import parse_url
from jsearch.tests.plugins.databases.factories.common import generate_address

logger = logging.getLogger(__name__)


@pytest.fixture()
def create_token_holders(
        token_holder_factory
) -> Callable[[str], None]:
    def create_env(token_address: str,
                   balances=(100, 200, 300),
                   holders_per_balance=2) -> None:

        for balance in balances:
            for _ in range(holders_per_balance):
                token_holder_factory.create(balance=balance, token_address=token_address)

    return create_env


URL = '/v1/tokens/address/holders?{params}'


@pytest.mark.parametrize(
    "url, holders, next_link, link",
    [
        (
                URL.format(params=urlencode({'limit': 3})),
                [(300, 5), (300, 4), (200, 3)],
                URL.format(params=urlencode({'limit': 3, 'order': 'desc', 'balance': 200, 'id': 2})),
                URL.format(params=urlencode({'limit': 3, 'order': 'desc', 'balance': 300, 'id': 5})),
        ),
        (
                URL.format(params=urlencode({'limit': 3, 'order': 'asc'})),
                [(100, 0), (100, 1), (200, 2)],
                URL.format(params=urlencode({'limit': 3, 'order': 'asc', 'balance': 200, 'id': 3})),
                URL.format(params=urlencode({'limit': 3, 'order': 'asc', 'balance': 100, 'id': 0})),
        ),
        (
                URL.format(params=urlencode({'limit': 3, 'balance': 200})),
                [(200, 3), (200, 2), (100, 1)],
                URL.format(params=urlencode({'limit': 3, 'order': 'desc', 'balance': 100, 'id': 0})),
                URL.format(params=urlencode({'limit': 3, 'order': 'desc', 'balance': 200, 'id': 3})),
        ),
        (
                URL.format(params=urlencode({'limit': 3, 'balance': 200, 'id': 2})),
                [(200, 2), (100, 1), (100, 0)],
                None,
                URL.format(params=urlencode({'limit': 3, 'order': 'desc', 'balance': 200, 'id': 2})),
        ),
    ],
    ids=[
        URL.format(params=urlencode({'limit': 3})),
        URL.format(params=urlencode({'limit': 3, 'order': 'asc'})),
        URL.format(params=urlencode({'limit': 3, 'balance': 200})),
        URL.format(params=urlencode({'limit': 3, 'balance': 200, 'id': 2})),
    ]
)
async def test_get_token_holders_pagination(
        cli,
        create_token_holders,
        url: str,
        holders: List[Tuple[int, int]],
        next_link: str,
        link: str
) -> None:
    # given
    token_address = generate_address()
    create_token_holders(token_address)

    # when
    resp = await cli.get(url.replace("/address/", f"/{token_address}/"))
    resp_json = await resp.json()

    # then
    assert resp.status == 200
    assert resp_json['status']['success']

    next_link = resp_json['paging']['next']
    curr_link = resp_json['paging']['link']

    expected_curr_link = link and link.replace("/address/", f"/{token_address}/") or link
    expected_next_link = next_link and next_link.replace("/address/", f"/{token_address}/") or next_link

    items = [(holder['balance'], holder['id']) for holder in resp_json['data']]

    assert parse_url(next_link) == parse_url(expected_next_link)
    assert parse_url(curr_link) == parse_url(expected_curr_link)

    assert items == holders


async def test_get_token_holders_pagination_large_balances_does_not_converted_to_e_notation(cli, create_token_holders):
    # See: JSEARCH-499.
    token_address = generate_address()
    create_token_holders(token_address, balances=(1000000000000000000,))

    resp = await cli.get(f'/v1/tokens/{token_address}/holders?limit=1')
    resp_json = await resp.json()

    assert resp_json['paging'] == {
        'link': f'/v1/tokens/{token_address}/holders?'
                f'balance=1000000000000000000&'
                f'id=1&'
                f'order=desc&'
                f'limit=1',
        'link_kwargs': {
            'balance': '1000000000000000000',
            'id': '1',
            'order': 'desc',
            'limit': '1'
        },
        'next': f'/v1/tokens/{token_address}/holders?'
                f'balance=1000000000000000000&'
                f'id=0&'
                f'order=desc&'
                f'limit=1',
        'next_kwargs': {
            'balance': '1000000000000000000',
            'id': '0',
            'order': 'desc',
            'limit': '1'
        }
    }


async def test_get_token_holders(cli, token_holder_factory):
    # given
    data = {
        'account_address': '0xa3dress',
        'decimals': 2,
        'balance': 3000,
        'token_address': '0xt1ken'
    }
    transfer = token_holder_factory.create(**data)

    # when
    resp = await cli.get(f'/v1/tokens/{transfer.token_address}/holders')
    resp_json = await resp.json()

    # then
    assert resp.status == 200
    assert resp_json['data'] == [
        {
            'accountAddress': '0xa3dress',
            'decimals': 2,
            'balance': 3000,
            'contractAddress': '0xt1ken',
            'id': transfer.id
        },
    ]


@pytest.mark.parametrize(
    "target_limit, expected_items_count, expected_errors",
    (
            (None, 20, []),
            (19, 19, []),
            (20, 20, []),
            (21, None, [
                {
                    "field": "limit",
                    "message": "Must be between 1 and 20.",
                    "code": "INVALID_LIMIT_VALUE",
                }
            ]),
    ),
    ids=[
        "limit=None --- 20 rows returned",
        "limit=19   --- 19 rows returned",
        "limit=20   --- 20 rows returned",
        "limit=21   --- error is returned",
    ],
)
async def test_get_token_holders_limits(
        cli: TestClient,
        create_token_holders,
        target_limit: Optional[int],
        expected_items_count: int,
        expected_errors: List[Dict[str, str]],
):
    # given
    token_address = generate_address()
    create_token_holders(token_address, holders_per_balance=7)  # ...therefore there are more than 20 holders.

    # when
    reqv_params = 'block_number=latest'

    if target_limit is not None:
        reqv_params += f'&limit={target_limit}'

    resp = await cli.get(f'/v1/tokens/{token_address}/holders?{reqv_params}')
    resp_json = await resp.json()

    # then
    observed_errors = resp_json['status']['errors']
    if resp_json['data'] is None:
        observed_items_count = None
    else:
        observed_items_count = len(resp_json['data'])

    assert (observed_errors, observed_items_count) == (expected_errors, expected_items_count)


@pytest.mark.parametrize(
    "parameter, value, status",
    (
            ('balance', 2 ** 128, 200),
            ('id', 2 ** 128, 400),
            ('id', 2 ** 8, 200),
    ),
    ids=(
            "balance_with_normal_value",
            "id_with_too_big_value",
            "id_with_normal_value",
    )
)
async def test_get_token_holders_by_big_value(
        cli: TestClient,
        parameter: str,
        value: int,
        status: int
):
    # given
    address = generate_address()
    params = urlencode({parameter: value})
    url = f"/v1/tokens/{address}/holders?{params}"

    # when
    resp = await cli.get(url)

    # then
    assert status == resp.status
