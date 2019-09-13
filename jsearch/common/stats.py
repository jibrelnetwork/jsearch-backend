import asyncio
import logging

import asyncpg
import prometheus_client

from jsearch import settings
from jsearch.common import utils
from jsearch.api.node_proxy import NodeProxy
from jsearch.common.structs import DbStats, LoopStats, NodeStats, ChainStats, LagStats
from jsearch.common.reference_data import get_ref_blocks

logger = logging.getLogger(__name__)


async def get_db_stats(db_pool: asyncpg.pool.Pool) -> DbStats:
    is_healthy = False

    try:
        async with db_pool.acquire() as conn:
            await conn.fetch('SELECT 1')

        is_healthy = True
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.warning('Cannot check the database', extra={'exception': e})

    return DbStats(is_healthy=is_healthy)


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


async def get_loop_stats() -> LoopStats:
    tasks_count = utils.get_loop_tasks_count()

    return LoopStats(
        is_healthy=tasks_count < settings.HEALTH_LOOP_TASKS_COUNT_THRESHOLD,
    )


async def get_chain_stats(db_pool: asyncpg.pool.Pool) -> ChainStats:
    is_healthy = False
    try:
        async with db_pool.acquire() as conn:
            # stored function check_canonical_chain(depth) returns number, hash, parent_hash
            # for each block N from canonical chain, if N.parent_hash != (N-1).hash
            holes = await conn.fetch('SELECT number, hash, parent_hash FROM check_canonical_chain(1000);')
            if not holes:
                is_healthy = True
            else:
                logger.critical("Chain Health Error: Chain Holes", extra={"holes": str([dict(h) for h in holes])})
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.warning('Cannot check the database', extra={'exception': e})
    return DbStats(is_healthy=is_healthy)


_lag_metrics = {}


async def get_lag_stats(db_pool: asyncpg.pool.Pool) -> LagStats:
    is_healthy = False
    lag = None
    try:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow('SELECT max(number) max_number FROM blocks where is_forked=false;')
            if not row:
                max_block = 0
            else:
                max_block = row['max_number']
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.warning('Cannot check the database', extra={'exception': e})
    else:
        ref_blocks = await get_ref_blocks()

        if _lag_metrics:
            _lag_metrics['etherscan'].set(ref_blocks['etherscan'] - max_block)
            _lag_metrics['infura'].set(ref_blocks['infura'] - max_block)
            _lag_metrics['jwallet'].set(ref_blocks['jwallet'] - max_block)

        max_ref_block = max(ref_blocks.values())
        lag = max_ref_block - max_block
        if lag < settings.HEALTHCHECK_LAG_THRESHOLD:
            is_healthy = True
        else:
            logger.critical("Chain Lag Health Error",
                            extra={"lag": lag, "max_ref_block": max_ref_block})
    return LagStats(is_healthy=is_healthy, lag=lag)


def setup_api_metrics() -> None:
    _setup_loop_tasks_total_metric(settings.METRIC_API_LOOP_TASKS_TOTAL)


def setup_syncer_metrics() -> None:
    _setup_loop_tasks_total_metric(settings.METRIC_SYNCER_LOOP_TASKS_TOTAL)
    _lag_metrics['etherscan'] = prometheus_client.Gauge(settings.METRIC_LAG_ETHERSCAN, 'Chain lag with Etherscan')
    _lag_metrics['infura'] = prometheus_client.Gauge(settings.METRIC_LAG_INFURA, 'Chain lag with Infura')
    _lag_metrics['jwallet'] = prometheus_client.Gauge(settings.METRIC_LAG_JWALLET, 'Chain lag with jWallet')


def setup_pending_syncer_metrics() -> None:
    _setup_loop_tasks_total_metric(settings.METRIC_SYNCER_PENDING_LOOP_TASKS_TOTAL)


def _setup_loop_tasks_total_metric(name: str) -> None:
    loop_tasks_total = prometheus_client.Gauge(name, 'Total amount of tasks in the event loop.')
    loop_tasks_total.set_function(lambda: utils.get_loop_tasks_count())
