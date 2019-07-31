import logging
from urllib.parse import parse_qs

import pytest
from typing import List, Dict, Any, Tuple


logger = logging.getLogger(__name__)

pytestmark = pytest.mark.usefixtures('disable_metrics_setup')


def parse_url(url: str) -> Tuple[str, Dict[str, Any]]:
    if url:
        path, params = url.split("?")
        return path, parse_qs(params)


@pytest.mark.parametrize(
    "url, uncles, uncles_on_page, next_link, link",
    [
        (
                "/v1/uncles?limit=3",
                10,
                [9, 8, 7],
                "/v1/uncles?limit=3&uncle_number=6&order=desc",
                "/v1/uncles?limit=3&uncle_number=9&order=desc",
        ),
        (
                "/v1/uncles?limit=3&order=asc",
                10,
                [9],
                None,
                "/v1/uncles?limit=3&uncle_number=9&order=asc",
        ),
        (
                "/v1/uncles?limit=3&uncle_number=5",
                10,
                [5, 4, 3],
                "/v1/uncles?limit=3&uncle_number=2&order=desc",
                "/v1/uncles?limit=3&uncle_number=5&order=desc",
        ),
        (
                "/v1/uncles?limit=3&uncle_number=latest",
                10,
                [9, 8, 7],
                "/v1/uncles?limit=3&uncle_number=6&order=desc",
                "/v1/uncles?limit=3&uncle_number=9&order=desc",
        ),
    ],
    ids=[
        "/v1/uncles?limit=3",
        "/v1/uncles?limit=3&order=asc",
        "/v1/uncles?limit=3&uncle_number=5",
        "/v1/uncles?limit=3&uncle_number=latest",
    ]
)
async def test_get_uncles(cli,
                          block_factory,
                          uncle_factory,
                          url: str,
                          uncles: int,
                          uncles_on_page: List[int],
                          next_link: str,
                          link: str) -> None:
    # given
    block_factory.create_batch(10)
    uncle_factory.create_batch(uncles)

    resp = await cli.get(url)
    resp_json = await resp.json()

    assert resp.status == 200
    assert resp_json['status']['success']

    assert parse_url(resp_json['paging']['next']) == parse_url(next_link)
    assert parse_url(resp_json['paging']['link']) == parse_url(link)

    assert [block['number'] for block in resp_json['data']] == uncles_on_page


@pytest.mark.parametrize(
    "url, errors",
    [
        ('/v1/uncles?uncle_number=aaaa', [
            {
                "field": "uncle_number",
                "message": "Not a valid number or tag.",
                "code": "INVALID_VALUE"
            }
        ]),
        ('/v1/uncles?timestamp=aaaa', [
            {
                "field": "timestamp",
                "message": "Not a valid number or tag.",
                "code": "INVALID_VALUE"
            }
        ]),
        ('/v1/uncles?uncle_number=10&timestamp=10', [
            {
                "field": "__all__",
                "message": "Filtration should be either by number or by timestamp",
                "code": "VALIDATION_ERROR"
            }
        ]),
        ('/v1/uncles?limit=100', [
            {
                "field": "limit",
                "message": "Must be between 1 and 20.",
                "code": "INVALID_LIMIT_VALUE"
            }
        ]),
        ('/v1/uncles?order=ascending', [
            {
                "field": "order",
                "message": 'Ordering can be either "asc" or "desc".',
                "code": "INVALID_ORDER_VALUE"
            }
        ]),
    ],
    ids=[
        "invalid_tag",
        "invalid_timestamp",
        "either_number_or_timestamp",
        "invalid_limit",
        "invalid_order"
    ]
)
async def test_get_uncles_errors(cli, block_factory, uncle_factory, url, errors):
    # given
    block_factory.create_batch(10)
    uncle_factory.create_batch(10)

    # when
    resp = await cli.get(url)
    resp_json = await resp.json()

    # then
    assert resp.status == 400
    assert not resp_json['status']['success']
    assert resp_json['status']['errors'] == errors
