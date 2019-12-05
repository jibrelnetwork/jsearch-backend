import click

from jsearch import settings
from jsearch.common import logs, stats, worker
from jsearch.utils import parse_range
from jsearch.pending_syncer import services


@click.command()
@click.option('--log-level', default=settings.LOG_LEVEL, help="Log level")
@click.option('--no-json-formatter', is_flag=True, default=settings.NO_JSON_FORMATTER, help='Use default formatter')
@click.option('--sync-range', envvar='SYNC_RANGE', help="Log level")
def run(log_level, no_json_formatter, sync_range):
    stats.setup_pending_syncer_metrics()
    logs.configure(log_level=log_level, formatter_class=logs.select_formatter_class(no_json_formatter))

    worker.Worker(
        services.PendingSyncerService(
            raw_db_dsn=settings.JSEARCH_RAW_DB,
            main_db_dsn=settings.JSEARCH_MAIN_DB,
            sync_range=parse_range(sync_range),
        ),
    ).execute_from_commandline()


if __name__ == '__main__':
    run()
