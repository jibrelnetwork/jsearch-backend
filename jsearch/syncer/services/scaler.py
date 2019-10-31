import json

from aiohttp import web
from aiohttp.web_app import Application
from aiohttp_apispec import (
    docs,
    request_schema,
    setup_aiohttp_apispec,
    response_schema
)
from functools import partial
from marshmallow import Schema, fields
from marshmallow.fields import Int
from marshmallow.validate import Range
from typing import Any
from webargs.aiohttpparser import use_kwargs

from jsearch import settings
from jsearch.api.middlewares import cors_middleware
from jsearch.common import services
from jsearch.common.structs import BlockRange
from jsearch.syncer.pool import WorkersPool
from jsearch.syncer.utils import get_last_block
from jsearch.utils import parse_range


class ScalerWebService(services.ApiService):
    _pool: WorkersPool

    def __init__(self, pool: WorkersPool, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault('port', settings.SYNCER_API_PORT)
        kwargs.setdefault('app_maker', make_app)

        super(ScalerWebService, self).__init__(*args, **kwargs)

        self.app['pool'] = pool


@docs(
    summary="Get health checks from all workers",
)
async def get_healthchecks(request: web.Request) -> web.Response:
    pool: WorkersPool = request.app['pool']
    checks = await pool.check_healthy()

    is_healthy = all(check['healthy'] for check in checks)
    return web.json_response(data={'healthy': is_healthy, 'workers': checks})


@docs(
    summary="Get state of all workers",
)
async def get_state(request: web.Request) -> web.Response:
    pool: WorkersPool = request.app['pool']
    pool_state = await pool.describe()
    return web.json_response(pool_state, dumps=partial(json.dumps, indent=2))


class SyncRangeField(fields.String):

    def _deserialize(self, value, attr, data):
        value = super(SyncRangeField, self)._deserialize(value, attr, data)
        if value:
            return BlockRange(*parse_range(value))


class ScaleSchema(Schema):
    sync_range = SyncRangeField(required=True)
    workers = Int(validate=Range(min=1, max=100), required=True)


@docs(
    summary="Scale workers and change syncer range",
)
@request_schema(ScaleSchema(strict=True))
@use_kwargs(ScaleSchema())
async def post_scale(request: web.Request, sync_range: BlockRange, workers: int) -> web.Response:
    pool: WorkersPool = request.app['pool']

    last_block = get_last_block()
    await pool.scale(sync_range, workers, last_block)

    return web.json_response({'status': 'ok'})


@docs(
    summary="Show current workers config"
)
@response_schema(ScaleSchema())
async def get_scale_config(request: web.Request):
    pool: WorkersPool = request.app['pool']

    return web.json_response({'workers': pool.workers, 'sync_range': str(pool.sync_range)})


def make_app() -> Application:
    """
    Create and initialize the application instance.
    """
    app = web.Application(middlewares=(cors_middleware,))
    app.router.add_route('GET', '/healthcheck', get_healthchecks)
    app.router.add_route('GET', '/api/state', get_state)
    app.router.add_route('POST', '/api/scale', post_scale)
    app.router.add_route('GET', '/api/scale', get_scale_config)
    app['storage'] = {}

    setup_aiohttp_apispec(
        app=app,
        title="Web syncer scaler API",
        version="v1",
        url="/api/docs/swagger.json",
        swagger_path="/",
    )
    return app
