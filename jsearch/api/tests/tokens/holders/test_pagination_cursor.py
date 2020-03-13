from typing import List, Tuple, Callable, Any, Dict
from urllib.parse import urlencode

import pytest
import yarl
from aiohttp.test_utils import TestClient

from jsearch.api.tests.utils import parse_url

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.api,
    pytest.mark.token_holders,
    pytest.mark.pagination
]


@pytest.mark.parametrize(
    "url_query, holders, next_link, link",
    [
        (
                {'limit': 3},
                [(300, 5), (300, 4), (200, 3)],
                {'limit': 3, 'order': 'desc', 'balance': 200, 'id': 2},
                {'limit': 3, 'order': 'desc', 'balance': 300, 'id': 5},
        ),
        (
                {'limit': 3, 'order': 'asc'},
                [(100, 0), (100, 1), (200, 2)],
                {'limit': 3, 'order': 'asc', 'balance': 200, 'id': 3},
                {'limit': 3, 'order': 'asc', 'balance': 100, 'id': 0},
        ),
        (
                {'limit': 3, 'balance': 200},
                [(200, 3), (200, 2), (100, 1)],
                {'limit': 3, 'order': 'desc', 'balance': 100, 'id': 0},
                {'limit': 3, 'order': 'desc', 'balance': 200, 'id': 3},
        ),
        (
                {'limit': 3, 'balance': 200, 'id': 2},
                [(200, 2), (100, 1), (100, 0)],
                None,
                {'limit': 3, 'order': 'desc', 'balance': 200, 'id': 2},
        ),
    ],
    ids=[
        'limit=3',
        'limit=3, order=asc',
        'limit=3, balance=200',
        'limit=3, balance=200, id=2',
    ]
)
async def test_cursor_pagination(
        cli: TestClient,
        create_token_holders: Callable[..., Any],
        token_address: str,
        url: yarl.URL,
        url_query: Dict[str, Any],
        holders: List[Tuple[int, int]],
        next_link: str,
        link: str
) -> None:
    # given
    create_token_holders(token_address)
    url = url.with_query(url_query)

    # when
    resp = await cli.get(str(url))
    resp_json = await resp.json()

    # then
    assert resp.status == 200
    assert resp_json['status']['success']

    next_link = resp_json['paging']['next']
    curr_link = resp_json['paging']['link']

    items = [(holder['balance'], holder['id']) for holder in resp_json['data']]

    assert parse_url(next_link) == parse_url(next_link)
    assert parse_url(curr_link) == parse_url(curr_link)

    assert items == holders
