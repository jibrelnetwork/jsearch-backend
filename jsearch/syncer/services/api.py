from asyncio import AbstractEventLoop
from typing import Any

import aiokafka
import asyncpg
from aiohttp import web
from jsearch.api.middlewares import cors_middleware

from jsearch import settings
from jsearch.common import stats
from jsearch.common import services


class ApiService(services.ApiService):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault('port', settings.WALLET_WORKER_API_PORT)
        kwargs.setdefault('app_maker', make_app)

        super(ApiService, self).__init__(*args, **kwargs)


def make_app(loop: AbstractEventLoop) -> web.Application:
    application = web.Application(middlewares=[cors_middleware], loop=loop)
    application.router.add_route('GET', '/healthcheck', healthcheck)

    application.on_startup.append(on_startup)
    application.on_shutdown.append(on_shutdown)

    return application


async def healthcheck(request: web.Request) -> web.Response:
    raw_db_stats = await stats.get_db_stats(request.app['db_pool_raw'])
    main_db_stats = await stats.get_db_stats(request.app['db_pool'])
    kafka_stats = await stats.get_kafka_stats(request.app['kafka_consumer'])
    loop_stats = await stats.get_loop_stats()

    healthy = all(
        (
            raw_db_stats.is_healthy,
            main_db_stats.is_healthy,
            kafka_stats.is_healthy,
            loop_stats.is_healthy,
        )
    )
    status = 200 if healthy else 400

    data = {
        'healthy': healthy,
        'isRawDbHealthy': raw_db_stats.is_healthy,
        'isMainDbHealthy': main_db_stats.is_healthy,
        'isKafkaHealthy': kafka_stats.is_healthy,
        'isLoopHealthy': loop_stats.is_healthy,
        'loopTasksCount': loop_stats.tasks_count,
    }

    return web.json_response(data=data, status=status)


async def on_startup(app: web.Application) -> None:
    app['db_pool'] = await asyncpg.create_pool(settings.JSEARCH_MAIN_DB)
    app['db_pool_raw'] = await asyncpg.create_pool(settings.JSEARCH_RAW_DB)
    app['kafka_consumer'] = aiokafka.AIOKafkaConsumer(
        loop=app.loop,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
    )

    await app['kafka_consumer'].start()


async def on_shutdown(app: web.Application) -> None:
    await app['db_pool'].close()
    await app['db_pool_raw'].close()
    await app['kafka_consumer'].stop()
