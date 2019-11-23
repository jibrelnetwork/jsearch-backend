import asyncpgsa
from aiohttp import web
from asyncpg import Connection
from functools import partial
from sqlalchemy import asc, desc, Column
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import Query
from typing import Any, Dict, List, Optional, Union, Callable, TypeVar

from jsearch.api.error_code import ErrorCode
from jsearch.api.pagination import Page
from jsearch.common.utils import timeit
from jsearch.typing import AnyCoroutine, ProgressPercent

DEFAULT_LIMIT = 20
MAX_LIMIT = 20
DEFAULT_OFFSET = 0
MAX_OFFSET = 10000


class ChainEvent:
    INSERT = 'created'
    REINSERT = 'reinserted'
    SPLIT = 'split'


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


TAG_LATEST = Tag(type_=Tag.LATEST, value='')


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


@timeit(name='[MAIN DB] Pages left query')
async def get_cursor_percent(
        connection: Connection,
        query: Query,
        reverse_query: Query,
) -> Optional[ProgressPercent]:
    query_estimation = await estimate_query(connection, query)
    reverse_estimation = await estimate_query(connection, reverse_query)

    if query_estimation:
        total = (reverse_estimation + query_estimation)
        progress = int((query_estimation / total) * 100)
        if progress >= 100:
            return 99
        return 100 - progress
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

    return None


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

    return None


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


def get_request_canonical_path(request: web.Request) -> str:
    request_path = request.path

    if request.match_info.route.resource is not None:
        # If route is well-known for the server, e.g. `/v1/blocks/{tag}`,
        # replace request's path with request's canonical path. This allows to
        # show metrics by specific endpoint.
        request_path = request.match_info.route.resource.canonical

    return request_path


async def maybe_orphan_request(
        request: web.Request,
        last_chain_insert_id: Optional[int],
        last_data_block: Optional[int],
        last_tip_block: Optional[int],
) -> Optional[web.Response]:
    """Orphans request if data consistency was affected by a chain split."""
    storage = request.app['storage']
    requests_orphaned_metric = request.app['metrics']['REQUESTS_ORPHANED']
    request_path = get_request_canonical_path(request)

    last_block = max((last_data_block, last_tip_block), key=lambda x: x or 0)
    should_orphan = await storage.is_data_affected_by_chain_split(last_chain_insert_id, last_block)

    if should_orphan:
        requests_orphaned_metric.labels(request_path).inc()

        return api_success(data={"isOrphaned": True})

    return None
