import json

import asyncpgsa
from aiohttp import web
from asyncpg import Connection
from functools import partial
from sqlalchemy import asc, desc, Column
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import Query
from typing import Any, Dict, List, Optional, Union, Callable, TypeVar

from jsearch.api.error_code import ErrorCode
from jsearch.api.ordering import DEFAULT_ORDER, ORDER_ASC, ORDER_DESC
from jsearch.api.pagination import Page
from jsearch.common.utils import async_timeit
from jsearch.typing import AnyCoroutine, ProgressPercent

DEFAULT_LIMIT = 20
MAX_LIMIT = 20
DEFAULT_OFFSET = 0
MAX_OFFSET = 10000


class Tag:
    """
    Block tag, can be block number, block hash or 'latest' label
    """
    LATEST = 'latest'
    TIP = 'tip'
    NUMBER = 'number'
    HASH = 'hash'

    __types = [LATEST, NUMBER, HASH, TIP]

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


def api_success(
        data: Union[Dict[str, Any], Any],
        page: Optional[Page] = None,
        progress: Optional[ProgressPercent] = None,
        meta: Optional[Dict[str, Any]] = None
):
    body = {
        'status': {
            'success': True,
            'errors': []
        },
        'data': data
    }

    if page:
        body['paging'] = page.to_dict()
        if page.next_link and progress is not None:
            body['paging']['progress'] = progress

    if meta:
        body['meta'] = meta

    return web.json_response(body)


def api_error(errors, data=None):
    return {
        'status': {
            'success': False,
            'errors': errors
        },
        'data': data
    }


def api_error_response(status, errors, data=None):
    body = api_error(errors, data)
    return web.json_response(body, status=status)


api_error_response_400 = partial(api_error_response, status=400)
api_error_response_404 = partial(api_error_response, status=404, errors=[
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


async def estimate_query(connection: Connection, query: Query) -> int:
    query = query.with_only_columns('*').limit(None)
    query = query.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True})

    query = f"SELECT row_estimator($${query}$$);"
    result = await fetch_row(connection, query)

    if result:
        return result.get('row_estimator', 0)

    return 0


@async_timeit(name='Pages left query')
async def get_cursor_percent(
        connection: Connection,
        query: Query,
        reverse_query: Query,
) -> Optional[ProgressPercent]:
    query_estimation = await estimate_query(connection, query)
    reverse_estimation = await estimate_query(connection, reverse_query)

    if query_estimation:
        total = (reverse_estimation + query_estimation)
        progress = round((query_estimation / total) * 100, 2)
        if progress >= 100:
            return 99.99
        return round(100 - progress, 2)
    return 0


async def fetch(connection: Connection, query: Union[Query, str]) -> List[Dict[str, Any]]:
    query, params = asyncpgsa.compile_query(query)
    result = await connection.fetch(query, *params)
    return [dict(item) for item in result]


async def fetch_row(connection: Connection, query: Union[Query, str]) -> Optional[Dict[str, Any]]:
    query, params = asyncpgsa.compile_query(query)
    result = await connection.fetchrow(query, *params)
    if result is not None:
        return dict(result)


def get_positive_number(
        request: web.Request,
        attr: str,
        tags=Optional[Dict[str, int]],
        is_required=False
) -> Optional[Union[int, str]]:
    value = request.query.get(attr, "").lower()

    if value.isdigit():
        number = int(value)
        if number >= 0:
            return number

    elif value and tags and value in tags:
        return tags[value]

    elif value and tags and value not in tags:
        msg_allowed_tags = tags and f" or tag ({', '.join(tags.keys())})" or ""
        raise ApiError(
            {
                'field': attr,
                'error_code': ErrorCode.VALIDATION_ERROR,
                'error_message': f'Parameter `{attr}` must be either positive integer{msg_allowed_tags}.'
            },
            status=400
        )

    if is_required:
        raise ApiError(
            {
                'field': attr,
                'error_code': ErrorCode.VALIDATION_ERROR,
                'error_message': f'Query param `{attr}` is required'
            },
            status=400
        )


class ApiError(Exception):
    """
    @ApiError.catch
    async def get_api(request):
        raise ApiError(status=404, error={'field': 'Not found'})
    """

    def __init__(self, errors: Union[List[Dict[str, str]], Dict[str, str]], status: int = 400) -> None:
        if isinstance(errors, dict):
            errors = [errors]

        self.errors = errors
        self.status = status

    @classmethod
    def catch(cls, func: Callable[..., AnyCoroutine]) -> Callable[..., AnyCoroutine]:

        async def _wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except ApiError as exc:
                return api_error_response(status=exc.status, errors=exc.errors, data=None)

        return _wrapper


Json = TypeVar('Json')


async def load_json_or_raise_api_error(request: web.Request) -> Json:
    try:
        return await request.json()
    except ValueError:
        raise ApiError(
            {
                'field': 'non_field_error',
                'error_code': ErrorCode.INVALID_BODY,
                'error_message': 'The provided body is not a valid JSON.'
            },
            status=400,
        )
