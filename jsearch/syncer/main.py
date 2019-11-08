import logging

import click
import time
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


def run_worker(sync_range: BlockRange, api_port: int, check_lag: bool, check_holes: bool, resync: bool) -> None:
    syncer_state = SyncerState(started_at=int(time.time()))

    api_worker = services.ApiService(check_lag=check_lag, check_holes=check_holes, port=api_port, state=syncer_state)

    syncer = services.SyncerService(sync_range=sync_range, resync=resync, state=syncer_state, check_lag=check_lag)
    syncer.add_dependency(api_worker)

    worker.Worker(syncer).execute_from_commandline()


def run_workers_pool(sync_range: BlockRange, workers: int, **kwargs: Any):
    pool = WorkersPool(
        sync_range=sync_range,
        workers=workers,
        worker_kwargs=kwargs
    )

    api_service = services.ScalerWebService(pool=pool)
    pool_service = services.WorkersPoolService(pool=pool)

    api_service.add_dependency(pool_service)
    worker.Worker(api_service).execute_from_commandline()


@click.command()
@click.option('--log-level', default=settings.LOG_LEVEL, help="Log level")
@click.option('--sync-range', default=None, help="Blocks range to sync")
@click.option('--workers', type=int, default=1)
@click.option('--port', type=int, default=settings.SYNCER_API_PORT)
@click.option('--check-lag', type=bool, default=True)
@click.option('--check-holes', type=bool, default=True)
@click.option('--resync', type=bool, default=False)
@click.option('--no-json-formatter', is_flag=True, default=settings.NO_JSON_FORMATTER, help='Use default formatter')
def run(
        log_level: str,
        sync_range: str,
        resync: bool,
        workers: int,
        port: int,
        check_lag: bool,
        check_holes: bool,
        no_json_formatter: bool,
):
    stats.setup_syncer_metrics()
    logs.configure(log_level=log_level, formatter_class=logs.select_formatter_class(no_json_formatter))

    sync_range = BlockRange(*parse_range(sync_range))
    if workers > 1:
        logger.info("Scale... ", extra={"workers": workers})
        run_workers_pool(
            sync_range=sync_range,
            workers=workers,
            **{
                'check_lag': 0,
                'check_holes': 0,
                'resync': resync,
                'log_level': log_level,
                'no_json_formatter': no_json_formatter,
            }
        )
    else:
        run_worker(sync_range, port, check_lag, check_holes, resync)


if __name__ == '__main__':
    run()
