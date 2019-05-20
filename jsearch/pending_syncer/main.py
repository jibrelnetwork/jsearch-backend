import os

import click
from jsearch import settings

from jsearch.common import logs, worker
from jsearch.pending_syncer import services
from jsearch.utils import parse_range


@click.command()
@click.option('--log-level', default=os.getenv('LOG_LEVEL', 'INFO'), help="Log level")
@click.option('--sync-range', default=None, help="Blocks range to sync")
def run(log_level, sync_range):
    logs.configure(log_level)

    worker.Worker(
        services.PendingSyncerService(
            raw_db_dsn=settings.JSEARCH_RAW_DB,
            main_db_dsn=settings.JSEARCH_MAIN_DB,
            sync_range=parse_range(sync_range),
        ),
        services.ApiService(),
    ).execute_from_commandline()


if __name__ == '__main__':
    run()
