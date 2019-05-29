import json

import asyncpgsa
from aiohttp import web
from asyncpg import Connection
from functools import partial
from sqlalchemy import asc, desc, Column
from sqlalchemy.orm import Query
from typing import Any, Dict, List, Optional

from jsearch.api.error_code import ErrorCode

DEFAULT_LIMIT = 20
MAX_LIMIT = 20
DEFAULT_OFFSET = 0
MAX_OFFSET = 10000

ORDER_ASC = 'asc'
ORDER_DESC = 'desc'
DEFAULT_ORDER = ORDER_DESC


class Tag:
    """
    Block tag, can be block number, block hash or 'latest' label
    """
    LATEST = 'latest'
    NUMBER = 'number'
    HASH = 'hash'

    __types = [LATEST, NUMBER, HASH]

    def __init__(self, type_, value):
        assert type_ in self.__types, 'Invalid tag type: {}'.format(type_)
        self.type = type_
        self.value = value

    def is_number(self):
        return self.type == self.NUMBER

    def is_hash(self):
        return self.type == self.HASH

    def is_latest(self):
        return self.type == self.LATEST


def get_tag(request):
    tag_value = request.match_info.get('tag') or request.query.get('tag', Tag.LATEST)
    if tag_value.isdigit():
        value = int(tag_value)
        type_ = Tag.NUMBER
    elif tag_value == Tag.LATEST:
        value = tag_value
        type_ = Tag.LATEST
    else:
        value = tag_value
        type_ = Tag.HASH
    return Tag(type_, value)


def validate_params(request, max_limit=None, max_offset=None, default_order=None):
    # todo: need to refactoring this function.
    # May be split to a few smaller or rewrite to marshmallow
    default_order = default_order or DEFAULT_ORDER

    params = {}
    errors = []

    limit = request.query.get('limit')
    if limit and limit.isdigit():
        params['limit'] = min(int(limit), max_limit or MAX_LIMIT)

    elif limit and not limit.isdigit():
        errors.append({'field': 'limit',
                       'error_code': ErrorCode.INVALID_LIMIT_VALUE,
                       'error_message': 'Limit value should be valid integer, got "{}"'.format(limit)
                       })
    else:
        params['limit'] = max_limit or DEFAULT_LIMIT

    offset = request.query.get('offset')
    if offset and offset.isdigit():
        params['offset'] = int(offset)
    elif offset and not offset.isdigit():
        errors.append({'field': 'offset',
                       'error_code': ErrorCode.INVALID_OFFSET_VALUE,
                       'error_message': 'Offset value should be valid integer, got "{}"'.format(offset)
                       })
    else:
        params['offset'] = DEFAULT_OFFSET

    if params.get('offset') and params['offset'] > (max_offset or MAX_OFFSET):
        errors.append({
            'field': 'offset',
            'error_code': ErrorCode.TOO_BIG_OFFSET_VALUE,
            'error_message': 'Offset value should be less then "{}"'.format(MAX_OFFSET)
        })

    order = request.query.get('order', '').lower()
    if order and order in [ORDER_ASC, ORDER_DESC]:
        params['order'] = order
    elif order:
        errors.append({'field': 'order',
                       'error_code': ErrorCode.INVALID_ORDER_VALUE,
                       'error_message': 'Order value should be one of "asc", "desc", got "{}"'.format(order)
                       })
    else:
        params['order'] = default_order

    if errors:
        body = {
            'status': {'success': False, 'errors': errors},
            'data': None
        }
        raise web.HTTPBadRequest(text=json.dumps(body), content_type='application/json')
    return params


def get_from_joined_string(joined_string: Optional[str], separator: str = ',') -> List[str]:
    """Lowers, splits and strips the joined string."""
    if joined_string is None:
        return list()

    strings_list = joined_string.lower().split(separator)
    strings_list = [string.strip() for string in strings_list]
    strings_list = [string for string in strings_list if string]

    return strings_list


def api_success(data):
    body = {
        'status': {'success': True, 'errors': []},
        'data': data
    }
    return web.json_response(body)


def api_error(status, errors, data=None):
    body = {
        'status': {'success': False, 'errors': errors},
        'data': data
    }
    return web.json_response(body, status=status)


api_error_400 = partial(api_error, status=400)
api_error_404 = partial(api_error, status=404, errors=[
    {
        'code': ErrorCode.RESOURCE_NOT_FOUND,
        'message': 'Resource not found'
    }
])


def proxy_response(resp):
    if 'error' in resp:
        err = {
            'field': 'non_field_error',
            'error_code': resp['error']['code'],
            'error_message': resp['error']['message']
        }
        status = {'success': False, 'errors': [err]}
    else:
        status = {'success': True, 'errors': []}

    body = {'status': status, 'data': resp.pop('result', None)}
    return web.json_response(body)


def get_order(columns: List[Column], direction: Optional[str]) -> List[Column]:
    operator = None

    if direction == 'asc':
        operator = asc

    if direction == 'desc':
        operator = desc

    if operator:
        columns = [operator(column) for column in columns]

    return columns


async def fetch(connection: Connection, query: Query) -> List[Dict[str, Any]]:
    query, params = asyncpgsa.compile_query(query)
    result = await connection.fetch(query, *params)
    return [dict(item) for item in result]


async def fetch_row(connection: Connection, query: Query) -> Optional[Dict[str, Any]]:
    query, params = asyncpgsa.compile_query(query)
    result = await connection.fetchrow(query, *params)
    if result is not None:
        return dict(result)


class ApiError(Exception):

    """
    @ApiError.catch
    async def get_api(request):
        raise ApiError(status=404, error={'field': 'Not found'})
    """

    def __init__(self, error: Dict[str, str], status: int = 400) -> None:
        self.error = error
        self.status = status

    @classmethod
    def catch(cls, func):

        async def _wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except ApiError as exc:

                return api_error(status=exc.status, errors=[exc.error], data={})

        return _wrapper
