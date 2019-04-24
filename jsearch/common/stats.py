import asyncio
import logging

import aiokafka
import asyncpg

from jsearch import settings
from jsearch.common import utils
from jsearch.api.node_proxy import NodeProxy
from jsearch.common.structs import MainDbStats, LoopStats, KafkaStats, NodeStats

logger = logging.getLogger(__name__)


async def get_main_db_stats(db_pool: asyncpg.pool.Pool) -> MainDbStats:
    is_healthy = False

    try:
        async with db_pool.acquire() as conn:
            await conn.fetch('SELECT 1')

        is_healthy = True
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.warning('Cannot check the database', extra={'exception': e})

    return MainDbStats(is_healthy=is_healthy)


async def get_node_stats(node_proxy: NodeProxy) -> NodeStats:
    is_healthy = False

    try:
        await node_proxy.client_version()

        is_healthy = True
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.warning('Cannot check the node', extra={'exception': e})

    return NodeStats(is_healthy=is_healthy)


async def get_kafka_stats(consumer: aiokafka.AIOKafkaConsumer) -> KafkaStats:
    try:
        await consumer._client.check_version()
        return KafkaStats(is_healthy=True)
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.warning('Cannot check the kafka', extra={'exception': e})

    return KafkaStats(is_healthy=False)


async def get_loop_stats() -> LoopStats:
    tasks_count = utils.get_loop_tasks_count()

    return LoopStats(
        is_healthy=tasks_count < settings.HEALTH_LOOP_TASKS_COUNT_THRESHOLD,
        tasks_count=tasks_count,
    )
