import logging

from asyncpg.pool import Pool
from uuid import uuid4

from jsearch import settings
from jsearch.common import utils
from jsearch.api.node_proxy import NodeProxy
from jsearch.common.structs import MainDbStats, LoopStats, KafkaStats, NodeStats

from jsearch_service_bus.base import get_async_consumer
from jsearch.service_bus import ROUTE_HEALTHCHECK

logger = logging.getLogger(__name__)


async def get_main_db_stats(db_pool: Pool) -> MainDbStats:
    is_healthy = False

    try:
        async with db_pool.acquire() as conn:
            await conn.fetch('SELECT 1')

        is_healthy = True
    except Exception:
        logger.exception('Cannot check the database')

    return MainDbStats(is_healthy=is_healthy)


async def get_node_stats(node_proxy: NodeProxy) -> NodeStats:
    is_healthy = False

    try:
        await node_proxy.client_version()

        is_healthy = True
    except Exception:
        logger.exception('Cannot check the node')

    return NodeStats(is_healthy=is_healthy)


async def get_kafka_stats() -> KafkaStats:
    is_healthy = False

    try:
        consumer = get_async_consumer(
            group=f'healthchecker_{uuid4()}',
            topic=ROUTE_HEALTHCHECK,
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS
        )

        await consumer._client.check_version()
        await consumer.stop()

        is_healthy = True
    except Exception:
        logger.exception('Cannot check the kafka')

    return KafkaStats(is_healthy=is_healthy)


async def get_loop_stats() -> LoopStats:
    tasks_count = utils.get_loop_tasks_count()

    return LoopStats(
        is_healthy=tasks_count < settings.HEALTH_LOOP_TASKS_COUNT_THRESHOLD,
        tasks_count=tasks_count,
    )
