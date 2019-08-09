import logging
from urllib.parse import parse_qs
from urllib.parse import urlencode

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


URL = '/v1/accounts/address/mined_uncles?{params}'


@pytest.mark.parametrize(
    "url, uncles, uncles_on_page, next_link, link",
    [
        (
                URL.format(params=urlencode({'limit': 3})),
                10,
                [9, 8, 7],
                URL.format(params=urlencode({
                    'limit': 3,
                    'uncle_number': 6,
                    'order': 'desc'
                })),
                URL.format(params=urlencode({
                    'limit': 3,
                    'uncle_number': 9,
                    'order': 'desc'
                })),
        ),
        (
                URL.format(params=urlencode({
                    'limit': 3,
                    'order': 'asc'
                })),
                10,
                [9],
                None,
                URL.format(params=urlencode({
                    'limit': 3,
                    'uncle_number': 9,
                    'order': 'asc'
                })),
        ),
        (
                URL.format(params=urlencode({
                    'limit': 3,
                    'uncle_number': 5
                })),
                10,
                [5, 4, 3],
                URL.format(params=urlencode({
                    'limit': 3,
                    'uncle_number': 2,
                    'order': 'desc'
                })),
                URL.format(params=urlencode({
                    'limit': 3,
                    'uncle_number': 5,
                    'order': 'desc'
                })),
        ),
        (
                URL.format(params=urlencode({
                    'limit': 3,
                    'uncle_number': 'latest',
                })),
                10,
                [9, 8, 7],
                URL.format(params=urlencode({
                    'limit': 3,
                    'uncle_number': 6,
                    'order': 'desc'
                })),
                URL.format(params=urlencode({
                    'limit': 3,
                    'uncle_number': 9,
                    'order': 'desc'
                })),
        ),
    ],
    ids=[
        "/v1/accounts/address/mined_uncles?limit=3",
        "/v1/accounts/address/mined_uncles?limit=3&order=asc",
        "/v1/accounts/address/mined_uncles?limit=3&uncle_number=5",
        "/v1/accounts/address/mined_uncles?limit=3&uncle_number=latest",
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
    miner = '0x1111111111111111111111111111111111111111'
    block_factory.create_batch(uncles)
    uncle_factory.create_batch(uncles, miner=miner)

    resp = await cli.get(url.replace('/address/', f'/{miner}/'))
    resp_json = await resp.json()

    assert resp.status == 200
    assert resp_json['status']['success']

    link = link and link.replace('/address/', f'/{miner}/')
    next_link = next_link and next_link.replace('/address/', f'/{miner}/')

    assert parse_url(resp_json['paging']['next']) == parse_url(next_link)
    assert parse_url(resp_json['paging']['link']) == parse_url(link)

    assert [block['number'] for block in resp_json['data']] == uncles_on_page


@pytest.mark.parametrize(
    "url, errors",
    [
        (URL.format(params=urlencode({'uncle_number': 'aaaa'})), [
            {
                "field": "uncle_number",
                "message": "Not a valid number or tag.",
                "code": "INVALID_VALUE"
            }
        ]),
        (URL.format(params=urlencode({'timestamp': 'aaaa'})), [
            {
                "field": "timestamp",
                "message": "Not a valid number or tag.",
                "code": "INVALID_VALUE"
            }
        ]),
        (URL.format(params=urlencode({'uncle_number': 10, 'timestamp': 10})), [
            {
                "field": "__all__",
                "message": "Filtration should be either by number or by timestamp",
                "code": "VALIDATION_ERROR"
            }
        ]),
        (URL.format(params=urlencode({'order': 'ascending'})), [
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
        "invalid_order"
    ]
)
async def test_get_uncles_errors(cli, block_factory, uncle_factory, url, errors):
    # given
    miner = '0x1111111111111111111111111111111111111111'
    block_factory.create_batch(10)
    uncle_factory.create_batch(10, miner=miner)

    # when
    resp = await cli.get(url.replace('/address/', f'/{miner}/'))
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
        (21, 0, [
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
    miner = '0x1111111111111111111111111111111111111111'
    block_factory.create_batch(25)
    uncle_factory.create_batch(25, miner=miner)

    # when
    reqv_params = {'uncle_number': 'latest'}

    if target_limit is not None:
        reqv_params['limit'] = target_limit

    resp = await cli.get(URL.replace('/address/', f'/{miner}/').format(params=urlencode(reqv_params)))
    resp_json = await resp.json()

    # then
    observed_errors = resp_json['status']['errors']
    observed_items_count = len(resp_json['data'])

    assert (observed_errors, observed_items_count) == (expected_errors, expected_items_count)
