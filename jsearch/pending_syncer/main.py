import click

from jsearch import settings
from jsearch.common import logs, worker, stats
from jsearch.common.structs import SyncRange
from jsearch.pending_syncer import services
from jsearch.utils import parse_range


@click.command()
@click.option('--log-level', default=settings.LOG_LEVEL, help="Log level")
@click.option('--no-json-formatter', is_flag=True, default=settings.NO_JSON_FORMATTER, help='Use default formatter')
@click.option('--sync-range', default=None, help="Log level")
def run(log_level, no_json_formatter, sync_range):
    stats.setup_pending_syncer_metrics()
    logs.configure(log_level=log_level, formatter_class=logs.select_formatter_class(no_json_formatter))

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
