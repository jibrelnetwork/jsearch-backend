import os

import click
from jsearch import settings

from jsearch.common import logs, worker
from jsearch.pending_syncer import services


@click.command()
@click.option('--log-level', default=os.getenv('LOG_LEVEL', 'INFO'), help="Log level")
def run(log_level):
    logs.configure(log_level)

    worker.Worker(
        services.PendingSyncerService(
            raw_db_dsn=settings.JSEARCH_RAW_DB,
            main_db_dsn=settings.JSEARCH_MAIN_DB,
        ),
        services.ApiService(),
    ).execute_from_commandline()


if __name__ == '__main__':
    run()
