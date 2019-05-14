import asyncio

import aiomonitor
import click
from jsearch.common import worker

from jsearch.common import logs
from jsearch.syncer import services
from jsearch.utils import parse_range


@click.command()
@click.option('--log-level', default='INFO', help="Log level")
@click.option('--sync-range', default=None, help="Blocks range to sync")
def run(log_level, sync_range):
    logs.configure(log_level)

    loop = asyncio.get_event_loop()

    with aiomonitor.start_monitor(loop=loop):
        worker.Worker(
            services.SyncerService(sync_range=parse_range(sync_range)),
            services.ApiService(),
            loop=loop,
        ).execute_from_commandline()


if __name__ == '__main__':
    run()
