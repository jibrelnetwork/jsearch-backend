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
                lambda x: x[30:9:-1]
        ),
        (
                {
                    'timestamp': 8 * 1000
                },
                {
                    'event_index': [str(make_event_index_for_log(24, 24, 24))],
                    'limit': ['20'],
                    'order': ['desc']
                },
                {
                    'event_index': [str(make_event_index_for_log(4, 4, 4))],
                    'limit': ['20'],
                    'order': ['desc']
                },
                lambda x: x[24:4:-1]
        ),
        (
                {
                    'timestamp': 8 * 1000,
                    'order': 'asc'
                },
                {
                    'event_index': [str(make_event_index_for_log(22, 22, 22))],
                    'limit': ['20'],
                    'order': ['asc']
                },
                None,
                lambda x: x[22:]
        ),
        (
                {
                    'timestamp': 8 * 1000,
                    'event_index': str(make_event_index_for_log(23, 23, 23)),
                    'order': 'asc'
                },
                {
                    'event_index': [str(make_event_index_for_log(23, 23, 23))],
                    'limit': ['20'],
                    'order': ['asc']
                },
                None,
                lambda x: x[23:]
        ),
        (
                {
                    'block_number': 8
                },
                {
                    'event_index': [str(make_event_index_for_log(24, 24, 24))],
                    'limit': ['20'],
                    'order': ['desc']
                },
                {
                    'event_index': [str(make_event_index_for_log(4, 4, 4))],
                    'limit': ['20'],
                    'order': ['desc']
                },
                lambda x: x[24:4:-1]
        ),
        (
                {
                    'block_number': 8,
                    'order': 'asc'
                },
                {
                    'event_index': [str(make_event_index_for_log(22, 22, 22))],
                    'limit': ['20'],
                    'order': ['asc']
                },
                None,
                lambda x: x[22:]
        ),

        (
                {
                    'timestamp': 8 * 1000,
                    'event_index': str(make_event_index_for_log(23, 23, 23)),
                    'order': 'asc'
                },
                {
                    'event_index': [str(make_event_index_for_log(23, 23, 23))],
                    'limit': ['20'],
                    'order': ['asc']
                },
                None,
                lambda x: x[23:]
        ),
        (
                {
                    'event_index': str(make_event_index_for_log(9, 9, 9)),
                },
                {
                    'event_index': [str(make_event_index_for_log(9, 9, 9))],
                    'limit': ['20'],
                    'order': ['desc']
                },
                None,
                lambda x: x[9::-1]
        ),
        (
                {
                    'event_index': str(make_event_index_for_log(21, 21, 21)),
                    'order': 'asc',
                },
                {
                    'event_index': [str(make_event_index_for_log(21, 21, 21))],
                    'limit': ['20'],
                    'order': ['asc']
                },
                None,
                lambda x: x[21:]
        )
    ],
    ids=[
        'by-empty-default',
        'by-timestamp-default-desc',
        'by-timestamp-asc',
        'by-timestamp-index-asc',
        'by-block_number-default-desc',
        'by-block_number-asc',
        'by-block_number-index-asc',
        'by-index-default-desc',
        'by-index-asc',
    ]
)
async def test_cursor_pagination(
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

    indexes = [item['event_data']['event_index'] for item in resp_json['data']]
    expected_indexes = [item['event_index'] for item in selector(events)]
    assert expected_indexes == indexes

    next_url = resp_json['paging']['next']
    link_url = resp_json['paging']['link']

    assert (next_url and parse_url(next_url)[1] or None) == next_params
    assert (link_url and parse_url(link_url)[1] or None) == link_params

