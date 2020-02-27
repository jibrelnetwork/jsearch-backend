import logging

import click

from jsearch import settings
from jsearch.common import stats, logs
from jsearch.structs import AppConfig
from jsearch.syncer.workers import run_workers_pool, run_worker
from jsearch.utils import parse_range

logger = logging.getLogger(__name__)


@click.command()
@click.option('-r', '--sync-range', envvar='SYNC_RANGE', help="Blocks range to sync")
@click.option('-w', '--workers', type=int, envvar='SYNCER_WORKERS')
@click.option('-p', '--port', type=int, default=settings.SYNCER_API_PORT)
@click.option('--resync', type=bool, envvar='SYNCER_RESYNC')
@click.option('--resync-chain-splits', type=bool, envvar='SYNCER_RESYNC_CHAIN_SPLITS')
@click.pass_obj
def syncer(
        config: AppConfig,
        sync_range: str,
        resync: bool,
        resync_chain_splits: bool,
        workers: int,
        port: int,
) -> None:
    """
    Service to sync data from RawDB to MainDB
    """
    stats.setup_syncer_metrics()
    logs.configure(
        log_level=config.log_level,
        formatter_class=logs.select_formatter_class(config.no_json_formatter)
    )

    block_range = parse_range(sync_range)

    if workers > 1:
        logger.info("Scale... ", extra={"workers": workers})
        run_workers_pool(
            sync_range=block_range,
            workers=workers,
            **{
                'resync': resync,
                'resync_chain_splits': resync_chain_splits,
                'log_level': config.log_level,
                'no_json_formatter': config.no_json_formatter,
            }
        )
    else:
        run_worker(block_range, port, resync, resync_chain_splits)
