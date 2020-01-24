from aiohttp import web

from jsearch.api.handlers import monitoring
from jsearch.api.middlewares import cors_middleware


def make_app() -> web.Application:
    application = web.Application(middlewares=[cors_middleware])
    application.router.add_route('GET', '/metrics', monitoring.metrics)

    return application
