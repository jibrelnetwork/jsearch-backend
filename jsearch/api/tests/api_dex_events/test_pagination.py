from typing import Callable, Dict, Any, List

import pytest
import yarl
from aiohttp.test_utils import TestClient

from jsearch.api.tests.utils import parse_url
from jsearch.common.wallet_events import make_event_index_for_log

pytestmark = [pytest.mark.asyncio, pytest.mark.api, pytest.mark.dex, pytest.mark.pagination]


@pytest.mark.parametrize(
    "url_params, link_params, next_params, selector",
    [
        (
                {},
                {
                    'event_index': [str(make_event_index_for_log(29, 29, 29))],
                    'limit': ['20'],
                    'order': ['desc']
                },
                {
                    'event_index': [str(make_event_index_for_log(9, 9, 9))],
                    'limit': ['20'],
                    'order': ['desc']
                },
                lambda x: x[-1:-21:-1]
        ),
    ],
    ids=[
        'block_number-first_page',
    ]
)
async def test_paginate(
        cli: TestClient,
        url: yarl.URL,
        url_params: Dict[str, Any],
        link_params: Dict[str, Any],
        next_params: Dict[str, Any],
        random_events: Callable[[int], None],
        selector: Callable[..., List[Dict[str, Any]]]
) -> None:
    # given
    events = random_events(30)
    url = url.with_query(url_params)

    # when
    response = await cli.get(str(url))
    resp_json = await response.json()

    # then
    assert response.status == 200
    assert resp_json['status']['success']

    next_url = resp_json['paging']['next']
    link_url = resp_json['paging']['link']

    assert (next_url and parse_url(next_url)[1] or None) == next_params
    assert (link_url and parse_url(link_url)[1] or None) == link_params

    indexes = [item['event_data']['event_index'] for item in resp_json['data']]
    expected_indexes = [item['event_index'] for item in selector(events)]
    assert expected_indexes == indexes
