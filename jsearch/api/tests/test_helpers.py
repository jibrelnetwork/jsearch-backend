
import json

import pytest

from aiohttp.test_utils import make_mocked_request
from aiohttp import web

from jsearch.api import helpers
from jsearch.api.error_code import ErrorCode


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
        'status': {
            'success': False,
            'errors':[
                {'field':'limit',
                 'error_message': 'Limit value should be valid integer, got "aaa"',
                 'error_code': ErrorCode.INVALID_LIMIT_VALUE},
                {'field':'offset',
                 'error_message': 'Offset value should be valid integer, got "xxx"',
                 'error_code': ErrorCode.INVALID_OFFSET_VALUE},
                {'field': 'order',
                 'error_message': 'Order value should be one of "asc", "desc", got "foo"',
                 'error_code': ErrorCode.INVALID_ORDER_VALUE},
            ]},
        'data': None
    }


