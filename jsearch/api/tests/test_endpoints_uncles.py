import logging
from urllib.parse import parse_qs, urlencode

import pytest
from aiohttp.test_utils import TestClient
from typing import List, Dict, Any, Tuple, Optional

from jsearch.tests.plugins.databases.factories.blocks import BlockFactory
from jsearch.tests.plugins.databases.factories.uncles import UncleFactory

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
                [0, 1, 2],
                "/v1/uncles?limit=3&uncle_number=3&order=asc",
                "/v1/uncles?limit=3&uncle_number=0&order=asc",
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
async def test_get_uncles_limits(
        cli: TestClient,
        block_factory: BlockFactory,
        uncle_factory: UncleFactory,
        target_limit: Optional[int],
        expected_items_count: int,
        expected_errors: List[Dict[str, str]],
):
    # given
    block_factory.create_batch(25)
    uncle_factory.create_batch(25)

    # when
    reqv_params = 'uncle_number=latest'

    if target_limit is not None:
        reqv_params += f'&limit={target_limit}'

    resp = await cli.get(f'/v1/uncles?{reqv_params}')
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
            ('uncle_number', 2 ** 128, 400),
            ('uncle_number', 2 ** 8, 200),
            ('timestamp', 2 ** 128, 400),
            ('timestamp', 2 ** 8, 200)
    ),
    ids=(
            "block_number_with_too_big_value",
            "block_number_with_normal_value",
            "timestamp_with_too_big_value",
            "timestamp_with_normal_value"
    )
)
async def test_get_uncles_filter_by_big_value(
        cli: TestClient,
        parameter: str,
        value: int,
        status: int
):
    # given
    params = urlencode({parameter: value})
    url = f'/v1/uncles?{params}'

    # when
    resp = await cli.get(url)

    # then
    assert status == resp.status
