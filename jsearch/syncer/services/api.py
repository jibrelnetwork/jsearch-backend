import json
from functools import partial
from typing import Any

import aiopg
from aiohttp import web

from jsearch import settings
from jsearch.api.handlers import monitoring
from jsearch.api.middlewares import cors_middleware
from jsearch.common import services
from jsearch.common import stats
from jsearch.syncer.state import SyncerState


class ApiService(services.ApiService):
    def __init__(
            self,
            state: SyncerState,
            *args: Any, **kwargs: Any
    ) -> None:
        kwargs.setdefault('port', settings.SYNCER_API_PORT)
        kwargs.setdefault('app_maker', make_app)

        super(ApiService, self).__init__(*args, **kwargs)

        self.app['state'] = state


def make_app() -> web.Application:
    application = web.Application(middlewares=[cors_middleware])
    application.router.add_route('GET', '/healthcheck', healthcheck)
    application.router.add_route('GET', '/state', get_state)
    application.router.add_route('GET', '/metrics', monitoring.metrics)

    application.on_startup.append(on_startup)
    application.on_shutdown.append(on_shutdown)

    return application


async def get_state(request: web.Request) -> web.Response:
    state: SyncerState = request.app['state']
    return web.json_response(data=state.as_dict(), dumps=partial(json.dumps, indent=2))


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
        'version': settings.VERSION,
        'isRawDbHealthy': raw_db_stats.is_healthy,
        'isMainDbHealthy': main_db_stats.is_healthy,
        'isLoopHealthy': loop_stats.is_healthy,
    }

    return web.json_response(data=data, status=status)


async def on_startup(app: web.Application) -> None:
    app['db_pool'] = await aiopg.sa.create_engine(settings.JSEARCH_MAIN_DB, minsize=1, maxsize=1)
    app['db_pool_raw'] = await aiopg.sa.create_engine(settings.JSEARCH_RAW_DB, minsize=1, maxsize=1)


async def on_shutdown(app: web.Application) -> None:
    app['db_pool'].close()
    app['db_pool_raw'].close()

    await app['db_pool'].wait_closed()
    await app['db_pool_raw'].wait_closed()
