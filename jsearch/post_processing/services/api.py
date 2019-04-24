import aiokafka
import asyncpg
from aiohttp import web
from jsearch import settings
from mode import Service

from jsearch.common import stats


class ApiService(Service):
    async def on_start(self) -> None:
        self.app = await make_app(self.loop)
        self.runner = web.AppRunner(self.app)

        await self.runner.setup()
        await web.TCPSite(self.runner, '0.0.0.0', settings.POST_PROCESSING_API_PORT).start()

    async def on_stop(self) -> None:
        await self.runner.cleanup()


async def make_app(loop) -> web.Application:
    application = web.Application(loop=loop)
    application.router.add_route('GET', '/healthcheck', healthcheck)

    application['db_pool'] = await asyncpg.create_pool(settings.JSEARCH_MAIN_DB)
    application['kafka_consumer'] = aiokafka.AIOKafkaConsumer(
        loop=loop,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
    )

    application.on_shutdown.append(on_shutdown)

    await application['kafka_consumer'].start()

    return application


async def healthcheck(request: web.Request) -> web.Response:
    main_db_stats = await stats.get_main_db_stats(request.app['db_pool'])
    kafka_stats = await stats.get_kafka_stats(request.app['kafka_consumer'])
    loop_stats = await stats.get_loop_stats()

    healthy = all(
        (
            main_db_stats.is_healthy,
            kafka_stats.is_healthy,
            loop_stats.is_healthy,
        )
    )
    status = 200 if healthy else 400

    data = {
        'healthy': healthy,
        'isMainDbHealthy': main_db_stats.is_healthy,
        'isKafkaHealthy': kafka_stats.is_healthy,
        'isLoopHealthy': loop_stats.is_healthy,
        'loopTasksCount': loop_stats.tasks_count,
    }

    return web.json_response(data=data, status=status)


async def on_shutdown(app):
    await app['db_pool'].close()
    await app['kafka_consumer'].stop()
