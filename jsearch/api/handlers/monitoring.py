import logging

import prometheus_client

from aiohttp import web

from jsearch import settings
from jsearch.common import stats

logger = logging.getLogger(__name__)


async def healthcheck(request: web.Request) -> web.Response:
    main_db_stats = await stats.get_db_stats(request.app['db_pool'])
    node_stats = await stats.get_node_stats(request.app['node_proxy'])
    loop_stats = await stats.get_loop_stats()

    healthy = all(
        (
            main_db_stats.is_healthy,
            node_stats.is_healthy,
            loop_stats.is_healthy,
        )
    )
    status = 200 if healthy else 400

    data = {
        'healthy': healthy,
        'version': settings.VERSION,
        'isMainDbHealthy': main_db_stats.is_healthy,
        'isNodeHealthy': node_stats.is_healthy,
        'isLoopHealthy': loop_stats.is_healthy,
    }

    return web.json_response(data=data, status=status)


async def metrics(request: web.Request) -> web.Response:
    body = prometheus_client.exposition.generate_latest().decode('utf-8')
    content_type = prometheus_client.exposition.CONTENT_TYPE_LATEST

    # WTF: `prometheus_client.exposition.CONTENT_TYPE_LATEST` is a complete
    # header with a charset provided, i.e.:
    #
    #    text/plain; version=0.0.4; charset=utf-8
    #
    # `aiohttp.web.Response` prohibits `content_type` kwarg with charsets, so
    # content type must be set as a header instead.
    return web.Response(body=body, headers={'Content-Type': content_type})
