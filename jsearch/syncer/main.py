import click
from jsearch.common import worker

from jsearch.common import logs
from jsearch.utils import parse_range
from jsearch.syncer.services import Syncer


@click.command()
@click.option('--log-level', default='INFO', help="Log level")
@click.option('--sync-range', default=None, help="Blocks range to sync")
def run(log_level, sync_range):
    logs.configure(log_level)

    worker.Worker(
        Syncer(sync_range=parse_range(sync_range)),
    ).execute_from_commandline()


if __name__ == '__main__':
    run()
