from asyncio import AbstractEventLoop
from typing import Any

import asyncpg
from aiohttp import web

from jsearch.api.handlers import monitoring
from jsearch.api.middlewares import cors_middleware

from jsearch import settings
from jsearch.common import stats
from jsearch.common import services


class ApiService(services.ApiService):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault('port', settings.SYNCER_PENDING_API_PORT)
        kwargs.setdefault('app_maker', make_app)

        super(ApiService, self).__init__(*args, **kwargs)


def make_app(loop: AbstractEventLoop) -> web.Application:
    application = web.Application(middlewares=[cors_middleware], loop=loop)
    application.router.add_route('GET', '/healthcheck', healthcheck)
    application.router.add_route('GET', '/metrics', monitoring.metrics)

    application.on_startup.append(on_startup)
    application.on_shutdown.append(on_shutdown)

    return application


async def healthcheck(request: web.Request) -> web.Response:
    raw_db_stats = await stats.get_db_stats(request.app['db_pool_raw'])
    main_db_stats = await stats.get_db_stats(request.app['db_pool'])
    loop_stats = await stats.get_loop_stats()

    healthy = all(
        (
            raw_db_stats.is_healthy,
            main_db_stats.is_healthy,
            loop_stats.is_healthy,
        )
    )
    status = 200 if healthy else 400

    data = {
        'healthy': healthy,
        'isRawDbHealthy': raw_db_stats.is_healthy,
        'isMainDbHealthy': main_db_stats.is_healthy,
        'isLoopHealthy': loop_stats.is_healthy,
    }

    return web.json_response(data=data, status=status)


async def on_startup(app: web.Application) -> None:
    app['db_pool'] = await asyncpg.create_pool(settings.JSEARCH_MAIN_DB)
    app['db_pool_raw'] = await asyncpg.create_pool(settings.JSEARCH_RAW_DB)


async def on_shutdown(app: web.Application) -> None:
    await app['db_pool'].close()
    await app['db_pool_raw'].close()
