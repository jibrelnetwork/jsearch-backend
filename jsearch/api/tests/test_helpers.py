
import json

import pytest

from aiohttp.test_utils import make_mocked_request
from aiohttp import web

from jsearch.api import helpers


def test_validate_request_ok():
    req = make_mocked_request('GET', '/')
    params = helpers.validate_params(req)
    assert params == {'order': helpers.DEFAULT_ORDER, 'limit': helpers.DEFAULT_LIMIT, 'offset': helpers.DEFAULT_OFFSET}

    req = make_mocked_request('GET', '/?order=asc&limit=10&offset=20')
    params = helpers.validate_params(req)
    assert params == {'order': 'asc', 'limit': 10, 'offset': 20}

    req = make_mocked_request('GET', '/?order=asc&limit=999999&offset=20')
    params = helpers.validate_params(req)
    assert params == {'order': 'asc', 'limit': helpers.MAX_LIMIT, 'offset': 20}

    req = make_mocked_request('GET', '/?order=foo&limit=aaa&offset=xxx')
    with pytest.raises(web.HTTPBadRequest) as ex:
        params = helpers.validate_params(req)
    assert json.loads(ex.value.text) == {
        'order': 'Order value should be one of "asc", "desc", got "foo"',
        'limit': 'Limit value should be valid integer, got "aaa"',
        'offset': 'Offset value should be valid integer, got "xxx"',
    }
