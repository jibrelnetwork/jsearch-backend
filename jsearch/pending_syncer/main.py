import os

import click
from jsearch.utils import parse_range

from jsearch import settings

from jsearch.common import logs, worker
from jsearch.common.structs import SyncRange
from jsearch.pending_syncer import services


@click.command()
@click.option('--log-level', default=os.getenv('LOG_LEVEL', 'INFO'), help="Log level")
@click.option('--sync-range', default=None, help="Log level")
def run(log_level, sync_range):
    logs.configure(log_level)

    # TODO (Nick Gashkov): Move `SyncRange` to the `parse_range` function. I
    # didn't do it right now because this causes slight refactoring of the
    # `jsearch.syncer.manager.Manager` which causes merge conflicts.
    parsed_range = parse_range(sync_range)
    parsed_sync_range = SyncRange(start=parsed_range[0], end=parsed_range[1])

    worker.Worker(
        services.PendingSyncerService(
            raw_db_dsn=settings.JSEARCH_RAW_DB,
            main_db_dsn=settings.JSEARCH_MAIN_DB,
            sync_range=parsed_sync_range,
        ),
        services.ApiService(),
    ).execute_from_commandline()


if __name__ == '__main__':
    run()
