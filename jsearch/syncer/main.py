import asyncio
import logging

import click
import time
from aiopg.sa import create_engine
from typing import Any

from jsearch import settings
from jsearch.common import logs, stats
from jsearch.common import worker
from jsearch.common.structs import BlockRange
from jsearch.syncer import services
from jsearch.syncer.pool import WorkersPool
from jsearch.syncer.state import SyncerState
from jsearch.utils import parse_range

logger = logging.getLogger("syncer")


def run_worker(
        sync_range: BlockRange,
        api_port: int,
        check_lag: bool,
        check_holes: bool,
        resync: bool,
        resync_chain_splits: bool
) -> None:
    syncer_state = SyncerState(started_at=int(time.time()))
    api_worker = services.ApiService(check_lag=check_lag, check_holes=check_holes, port=api_port, state=syncer_state)

    syncer = services.SyncerService(
        sync_range=sync_range,
        resync=resync,
        resync_chain_splits=resync_chain_splits,
        state=syncer_state,
        check_lag=check_lag
    )
    syncer.add_dependency(api_worker)

    worker.Worker(syncer).execute_from_commandline()


def run_workers_pool(sync_range: BlockRange, workers: int, **kwargs: Any) -> None:
    pool = WorkersPool(
        sync_range=sync_range,
        workers=workers,
        worker_kwargs=kwargs
    )

    api_service = services.ScalerWebService(pool=pool)
    pool_service = services.WorkersPoolService(pool=pool)

    api_service.add_dependency(pool_service)
    worker.Worker(api_service).execute_from_commandline()


# TODO (nickgashkov): Remove after schema this migration is applied:
#   20191107070950_add_assets_summary_pairs.sql
async def wait_new_scheme() -> None:
    query = """
    SELECT *
    FROM pg_indexes
    WHERE indexname = 'ix_assets_summary_pairs_address_asset_address';
    """

    engine = await create_engine(dsn=settings.JSEARCH_MAIN_DB, maxsize=1)

    while True:
        try:
            async with engine.acquire() as connection:
                async with connection.execute(query) as cursor:
                    row = await cursor.fetchone()
                    if row is None:
                        logger.info('Wait new scheme, wait 5 seconds...')
                        await asyncio.sleep(5)
                    else:
                        logger.info('New schema have founded, start sync...')
                        break
        except KeyboardInterrupt:
            break

    engine.close()


def wait() -> None:
    loop = asyncio.get_event_loop()
    loop.run_until_complete(wait_new_scheme())


@click.command()
@click.option('--log-level', default=settings.LOG_LEVEL, help="Log level")
@click.option('--sync-range', envvar='SYNC_RANGE', help="Blocks range to sync")
@click.option('--workers', type=int, envvar='SYNCER_WORKERS')
@click.option('--port', type=int, default=settings.SYNCER_API_PORT)
@click.option('--check-lag', type=bool, envvar='SYNCER_CHECK_LAG')
@click.option('--check-holes', type=bool, envvar='SYNCER_CHECK_HOLES')
@click.option('--resync', type=bool, envvar='SYNCER_RESYNC')
@click.option('--resync-chain-splits', type=bool, envvar='SYNCER_RESYNC_CHAIN_SPLITS')
@click.option('--no-json-formatter', is_flag=True, default=settings.NO_JSON_FORMATTER, help='Use default formatter')
def run(
        log_level: str,
        sync_range: str,
        resync: bool,
        resync_chain_splits: bool,
        workers: int,
        port: int,
        check_lag: bool,
        check_holes: bool,
        no_json_formatter: bool,
) -> None:
    stats.setup_syncer_metrics()
    logs.configure(log_level=log_level, formatter_class=logs.select_formatter_class(no_json_formatter))

    wait()

    block_range = BlockRange(*parse_range(sync_range))

    if workers > 1:
        logger.info("Scale... ", extra={"workers": workers})
        run_workers_pool(
            sync_range=block_range,
            workers=workers,
            **{
                'check_lag': 0,
                'check_holes': 0,
                'resync': resync,
                'resync_chain_splits': resync_chain_splits,
                'log_level': log_level,
                'no_json_formatter': no_json_formatter,
            }
        )
    else:
        run_worker(block_range, port, check_lag, check_holes, resync, resync_chain_splits)


if __name__ == '__main__':
    run()
