import asyncio
import logging
from typing import Any, List, Dict

from aiohttp import web
from aiopg.sa import Engine

from jsearch import settings
from jsearch.api.node_proxy import NodeProxy
from jsearch.common import utils, prom_metrics
from jsearch.common.db import fetch_one, fetch_all
from jsearch.common.reference_data import get_lag_statistics, get_lag_statistics_by_provider
from jsearch.common.structs import DbStats, LoopStats, NodeStats, ChainStats, LagStats

logger = logging.getLogger(__name__)


async def get_db_stats(engine: Engine) -> DbStats:
    is_healthy = False

    try:
        await fetch_one(engine, 'SELECT 1')
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


async def get_chain_stats(engine: Engine) -> ChainStats:
    is_healthy = False
    holes: List[Dict[str, Any]] = []
    try:
        # stored function check_canonical_chain(depth) returns number, hash, parent_hash
        # for each block N from canonical chain, if N.parent_hash != (N-1).hash
        holes = await fetch_all(engine, 'SELECT number, hash, parent_hash FROM check_canonical_chain(1000)')
        if not holes:
            is_healthy = True
        else:
            logger.error("Chain Health Error: Chain Holes", extra={"holes": str([dict(h) for h in holes])})
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.warning('Cannot check the database', extra={'exception': e})
    return ChainStats(is_healthy=is_healthy, chain_holes=holes)


async def get_lag_stats() -> LagStats:
    is_healthy = False

    lag_statistics = get_lag_statistics()
    lag = max(lag_statistics.values())

    if lag < settings.HEALTHCHECK_LAG_THRESHOLD:
        is_healthy = True
    else:
        logger.critical("Chain Lag Health Error", extra={"lag": lag})

    return LagStats(is_healthy=is_healthy, lag=lag)


def setup_api_metrics(app: web.Application) -> None:
    # Automatic metrics, handled by state-less function.
    prom_metrics.METRIC_API_LOOP_TASKS_TOTAL.set_function(lambda: utils.get_loop_tasks_count())

    # Non-automatic metrics, changeable inside a request.
    app['metrics'] = {
        'REQUESTS_ORPHANED': prom_metrics.METRIC_API_REQUESTS_ORPHANED_TOTAL,
        'REQUESTS_LATENCY': prom_metrics.METRIC_API_REQUESTS_LATENCY_SECONDS,
        'REQUESTS_IN_PROGRESS': prom_metrics.METRIC_API_REQUESTS_IN_PROGRESS_TOTAL,
        'REQUESTS_TOTAL': prom_metrics.METRIC_API_REQUESTS_TOTAL,
    }


def setup_syncer_metrics() -> None:
    prom_metrics.METRIC_SYNCER_LOOP_TASKS_TOTAL.set_function(lambda: utils.get_loop_tasks_count())


def setup_monitor_metrics() -> None:
    prom_metrics.METRIC_SYNCER_LAG_ETHERSCAN.set_function(lambda: get_lag_statistics_by_provider('etherscan'))
    prom_metrics.METRIC_SYNCER_LAG_INFURA.set_function(lambda: get_lag_statistics_by_provider('infura'))
    prom_metrics.METRIC_SYNCER_LAG_JWALLET.set_function(lambda: get_lag_statistics_by_provider('jwallet'))


def setup_pending_syncer_metrics() -> None:
    prom_metrics.METRIC_SYNCER_PENDING_LOOP_TASKS_TOTAL.set_function(lambda: utils.get_loop_tasks_count())
