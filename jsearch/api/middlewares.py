from typing import Callable, Awaitable

from aiohttp import web

Handler = Callable[[web.Request], Awaitable[web.StreamResponse]]


@web.middleware
async def cors_middleware(request: web.Request, handler: Handler) -> web.StreamResponse:
    response = await handler(request)

    response.headers['Access-Control-Allow-Headers'] = '*'
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Request-Method'] = 'POST, GET, OPTIONS, HEAD'

    return response


@web.middleware
async def prom_middleware(request: web.Request, handler: Handler) -> web.StreamResponse:
    request_canonical_path = request.match_info.route.resource.canonical

    request.app['metrics']['REQUESTS_IN_PROGRESS'].labels(request_canonical_path, request.method).inc()

    with request.app['metrics']['REQUESTS_LATENCY'].labels(request_canonical_path).time():
        response = await handler(request)

    request.app['metrics']['REQUESTS_IN_PROGRESS'].labels(request_canonical_path, request.method).dec()
    request.app['metrics']['REQUESTS_TOTAL'].labels(request_canonical_path, request.method, response.status).inc()

    return response
