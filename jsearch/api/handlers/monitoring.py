import logging

from aiohttp import web

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
        'isMainDbHealthy': main_db_stats.is_healthy,
        'isNodeHealthy': node_stats.is_healthy,
        'isLoopHealthy': loop_stats.is_healthy,
        'loopTasksCount': loop_stats.tasks_count,
    }

    return web.json_response(data=data, status=status)
