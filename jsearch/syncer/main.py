import click

from jsearch import settings
from jsearch.common import logs
from jsearch.common import worker
from jsearch.syncer import services
from jsearch.utils import parse_range


@click.command()
@click.option('--log-level', default=settings.LOG_LEVEL, help="Log level")
@click.option('--no-json-formatter', is_flag=True, default=settings.NO_JSON_FORMATTER, help='Use default formatter')
@click.option('--sync-range', default=None, help="Blocks range to sync")
def run(log_level, no_json_formatter, sync_range):
    logs.configure(log_level=log_level, formatter_class=logs.select_formatter_class(no_json_formatter))

    syncer = services.SyncerService(sync_range=parse_range(sync_range))
    syncer.add_dependency(services.ApiService())

    worker.Worker(syncer).execute_from_commandline()


if __name__ == '__main__':
    run()
