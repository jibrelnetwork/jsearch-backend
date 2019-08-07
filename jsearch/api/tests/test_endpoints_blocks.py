import logging
from urllib.parse import parse_qs

import pytest
from typing import List, Dict, Any, Tuple

from jsearch.api.models import InternalTransaction

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.usefixtures('disable_metrics_setup')


def parse_url(url: str) -> Tuple[str, Dict[str, Any]]:
    if url:
        path, params = url.split("?")
        return path, parse_qs(params)


@pytest.mark.parametrize(
    "url, blocks, blocks_on_page, next_link, link",
    [
        (
                "/v1/blocks?limit=3",
                10,
                [9, 8, 7],
                "/v1/blocks?limit=3&block_number=6&order=desc",
                "/v1/blocks?limit=3&block_number=9&order=desc",
        ),
        (
                "/v1/blocks?limit=3&order=asc",
                10,
                [0, 1, 2],
                "/v1/blocks?limit=3&block_number=3&order=asc",
                "/v1/blocks?limit=3&block_number=0&order=asc",
        ),
        (
                "/v1/blocks?limit=3&block_number=5",
                10,
                [5, 4, 3],
                "/v1/blocks?limit=3&block_number=2&order=desc",
                "/v1/blocks?limit=3&block_number=5&order=desc",
        ),
        (
                "/v1/blocks?limit=3&block_number=latest",
                10,
                [9, 8, 7],
                "/v1/blocks?limit=3&block_number=6&order=desc",
                "/v1/blocks?limit=3&block_number=9&order=desc",
        ),
    ],
    ids=[
        "/v1/blocks?limit=3",
        "/v1/blocks?limit=3&order=asc",
        "/v1/blocks?limit=3&block_number=5",
        "/v1/blocks?limit=3&block_number=latest",
    ]
)
async def test_get_blocks(cli,
                          block_factory,
                          url: str,
                          blocks: int,
                          blocks_on_page: List[int],
                          next_link: str,
                          link: str) -> None:
    # given
    block_factory.create_batch(blocks)

    resp = await cli.get(url)
    resp_json = await resp.json()

    assert resp.status == 200
    assert resp_json['status']['success']

    assert parse_url(resp_json['paging']['next']) == parse_url(next_link)
    assert parse_url(resp_json['paging']['link']) == parse_url(link)

    assert [block['number'] for block in resp_json['data']] == blocks_on_page


@pytest.mark.parametrize(
    "url, errors",
    [
        ('/v1/blocks?block_number=aaaa', [
            {
                "field": "block_number",
                "message": "Not a valid number or tag.",
                "code": "INVALID_VALUE"
            }
        ]),
        ('/v1/blocks?timestamp=aaaa', [
            {
                "field": "timestamp",
                "message": "Not a valid number or tag.",
                "code": "INVALID_VALUE"
            }
        ]),
        ('/v1/blocks?block_number=10&timestamp=10', [
            {
                "field": "__all__",
                "message": "Filtration should be either by number or by timestamp",
                "code": "VALIDATION_ERROR"
            }
        ]),
        ('/v1/blocks?limit=100', [
            {
                "field": "limit",
                "message": "Must be between 1 and 20.",
                "code": "INVALID_LIMIT_VALUE"
            }
        ]),
        ('/v1/blocks?order=ascending', [
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
async def test_get_blocks_errors(cli, block_factory, url, errors):
    # given
    block_factory.create_batch(10)

    # when
    resp = await cli.get(url)
    resp_json = await resp.json()

    # then
    assert resp.status == 400
    assert not resp_json['status']['success']
    assert resp_json['status']['errors'] == errors


@pytest.fixture
def blocks_422(block_factory):
    return [
        block_factory.create(),
        block_factory.create(),
        block_factory.create(),
    ]


@pytest.fixture
def internal_txs_422(blocks_422, internal_transaction_factory):
    return [
        internal_transaction_factory.create(block_hash=blocks_422[0].hash, block_number=blocks_422[0].number),
        internal_transaction_factory.create(block_hash=blocks_422[0].hash, block_number=blocks_422[0].number),
        internal_transaction_factory.create(block_hash=blocks_422[1].hash, block_number=blocks_422[1].number),
        internal_transaction_factory.create(block_hash=blocks_422[1].hash, block_number=blocks_422[1].number),
        internal_transaction_factory.create(block_hash=blocks_422[1].hash, block_number=blocks_422[2].number,
                                            is_forked=True),
        internal_transaction_factory.create(block_hash=blocks_422[2].hash, block_number=blocks_422[2].number,
                                            parent_tx_hash='0xt1'),
        internal_transaction_factory.create(block_hash=blocks_422[2].hash, block_number=blocks_422[2].number),
    ]


async def test_get_block_internal_txs_bynumber_ok(cli, blocks_422, internal_txs_422):

    url = "v1/blocks/{}/internal_transactions".format(blocks_422[2].number)
    resp = await cli.get(url)
    assert resp.status == 200
    resp_json = await resp.json()

    assert len(resp_json['data']) == 2
    assert resp_json['data'][0] == InternalTransaction(**internal_txs_422[5].as_dict()).to_dict()
    assert resp_json['data'][1] == InternalTransaction(**internal_txs_422[6].as_dict()).to_dict()

    url = "v1/blocks/{}/internal_transactions?parent_tx_hash=0xt1".format(blocks_422[2].number)
    resp = await cli.get(url)
    assert resp.status == 200
    resp_json = await resp.json()

    assert len(resp_json['data']) == 1
    assert resp_json['data'][0] == InternalTransaction(**internal_txs_422[5].as_dict()).to_dict()


async def test_get_block_internal_txs_byhash_ok(cli, blocks_422, internal_txs_422):
    url = "v1/blocks/{}/internal_transactions".format(blocks_422[2].hash)
    resp = await cli.get(url)
    assert resp.status == 200
    resp_json = await resp.json()

    assert len(resp_json['data']) == 2
    assert resp_json['data'][0] == InternalTransaction(**internal_txs_422[5].as_dict()).to_dict()
    assert resp_json['data'][1] == InternalTransaction(**internal_txs_422[6].as_dict()).to_dict()

    url = "v1/blocks/{}/internal_transactions?parent_tx_hash=0xt1".format(blocks_422[2].hash)
    resp = await cli.get(url)
    assert resp.status == 200
    resp_json = await resp.json()

    assert len(resp_json['data']) == 1
    assert resp_json['data'][0] == InternalTransaction(**internal_txs_422[5].as_dict()).to_dict()


async def test_get_block_internal_txs_latest_ok(cli, blocks_422, internal_txs_422, block_factory):

    url = "v1/blocks/latest/internal_transactions"
    resp = await cli.get(url)
    assert resp.status == 200
    resp_json = await resp.json()

    assert len(resp_json['data']) == 2
    assert resp_json['data'][0] == InternalTransaction(**internal_txs_422[5].as_dict()).to_dict()
    assert resp_json['data'][1] == InternalTransaction(**internal_txs_422[6].as_dict()).to_dict()

    block_factory.create()
    resp = await cli.get(url)
    assert resp.status == 200
    resp_json = await resp.json()

    assert len(resp_json['data']) == 0
