import logging

import click

from jsearch import settings
from jsearch.common import stats, logs
from jsearch.common.structs import BlockRange
from jsearch.syncer.utils import wait_for_version
from jsearch.syncer.workers import run_workers_pool, run_worker
from jsearch.utils import parse_range

logger = logging.getLogger(__name__)


def start(
        version_id: str,
        workers: int,
        port: int,
        resync: bool,
        resync_chain_splits: bool,
        block_range: BlockRange,
        log_level: str,
        no_json_formatter: bool
) -> None:
    if version_id:
        wait_for_version(version_id)

    if workers > 1:
        logger.info("Scale... ", extra={"workers": workers})
        run_workers_pool(
            sync_range=block_range,
            workers=workers,
            **{
                'resync': resync,
                'resync_chain_splits': resync_chain_splits,
                'log_level': log_level,
                'no_json_formatter': no_json_formatter,
            }
        )
    else:
        run_worker(block_range, port, resync, resync_chain_splits)


@click.command()
@click.option('-r', '--sync-range', envvar='SYNC_RANGE', help="Blocks range to sync")
@click.option('-w', '--workers', type=int, envvar='SYNCER_WORKERS')
@click.option('-p', '--port', type=int, default=settings.SYNCER_API_PORT)
@click.option('--resync', type=bool, envvar='SYNCER_RESYNC')
@click.option('--resync-chain-splits', type=bool, envvar='SYNCER_RESYNC_CHAIN_SPLITS')
@click.option('--wait-migration', envvar='SYNCER_WAIT_MIGRATION')
@click.option('--log-level', envvar='LOG_LEVEL', help="Log level")
@click.option('--no-json-formatter', is_flag=True, envvar='NO_JSON_FORMATTER', help='Use default formatter')
def syncer(
        sync_range: str,
        resync: bool,
        resync_chain_splits: bool,
        wait_migration: str,
        workers: int,
        port: int,
        log_level: str,
        no_json_formatter: bool
) -> None:
    """
    Service to sync data from RawDB to MainDB
    """
    stats.setup_syncer_metrics()

    formatter_cls = logs.select_formatter_class(no_json_formatter)

    logs.configure(
        log_level=log_level,
        formatter_class=formatter_cls,
    )

    block_range = parse_range(sync_range)

    start(wait_migration, workers, port, resync, resync_chain_splits, block_range, log_level, no_json_formatter)
