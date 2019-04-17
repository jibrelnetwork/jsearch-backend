from typing import Callable, Awaitable

from aiohttp import web

Handler = Callable[[web.Request], Awaitable[web.Response]]


@web.middleware
async def cors_middleware(request: web.Request, handler: Handler) -> web.Response:
    response = await handler(request)

    response.headers['Access-Control-Allow-Headers'] = '*'
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Request-Method'] = 'POST, GET, OPTIONS, HEAD'

    return response
