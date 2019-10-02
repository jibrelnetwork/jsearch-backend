import asyncio
import logging

import click

from jsearch import settings
from jsearch.common import logs, stats
from jsearch.common import worker
from jsearch.common.structs import SyncRange
from jsearch.syncer import services
from jsearch.syncer.scaler.pool import WorkersPool
from jsearch.syncer.utils import get_last_block
from jsearch.utils import parse_range

logger = logging.getLogger("syncer")


def run_worker(sync_range: SyncRange, api_port: int, resync: bool) -> None:
    api_worker = services.ApiService(port=api_port)

    syncer = services.SyncerService(sync_range=(sync_range.start, sync_range.end), resync=resync)
    syncer.add_dependency(api_worker)

    worker.Worker(syncer).execute_from_commandline()


def run_workers_pool(sync_range: SyncRange, workers: int, resync: bool, no_json_formatter: bool, log_level: str):
    pool = WorkersPool(sync_range, workers)
    last_block = get_last_block()

    loop = asyncio.get_event_loop()
    try:
        run_task = pool.run(last_block, resync, log_level, no_json_formatter)
        loop.run_until_complete(run_task)
        loop.run_until_complete(pool.wait())
    except KeyboardInterrupt:
        loop.run_until_complete(pool.stop())
        loop.run_until_complete(pool.wait())


@click.command()
@click.option('--log-level', default=settings.LOG_LEVEL, help="Log level")
@click.option('--no-json-formatter', is_flag=True, default=settings.NO_JSON_FORMATTER, help='Use default formatter')
@click.option('--sync-range', default=None, help="Blocks range to sync")
@click.option('--resync', type=bool, default=False)
@click.option('--workers', type=int, default=1)
@click.option('--port', type=int, default=settings.SYNCER_API_PORT)
def run(log_level: str, no_json_formatter: bool, sync_range: str, resync: bool, workers: int, port: int):
    stats.setup_syncer_metrics()
    logs.configure(log_level=log_level, formatter_class=logs.select_formatter_class(no_json_formatter))

    sync_range = SyncRange(*parse_range(sync_range))
    if workers > 1:
        logger.info("Scale... ", extra={"workers": workers})
        run_workers_pool(sync_range, workers, resync, no_json_formatter, log_level)
    else:
        run_worker(sync_range, port, resync)


if __name__ == '__main__':
    run()
