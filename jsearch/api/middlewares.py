from typing import Callable, Awaitable

from aiohttp import web

from jsearch import settings

Handler = Callable[[web.Request], Awaitable[web.Response]]


@web.middleware
async def cors_middleware(request: web.Request, handler: Handler) -> web.Response:
    response = await handler(request)

    response.headers['Access-Control-Allow-Headers'] = '*'
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Request-Method'] = 'POST, GET, OPTIONS, HEAD'

    return response


@web.middleware
async def prom_middleware(request: web.Request, handler: Handler) -> web.Response:
    request.app['metrics']['REQUESTS_IN_PROGRESS'].labels(settings.PID, request.path, request.method).inc()

    with request.app['metrics']['REQUESTS_LATENCY'].labels(settings.PID, request.path).time():
        response = await handler(request)

    request.app['metrics']['REQUESTS_IN_PROGRESS'].labels(settings.PID, request.path, request.method).dec()
    request.app['metrics']['REQUESTS_TOTAL'].labels(settings.PID, request.path, request.method, response.status).inc()

    return response
