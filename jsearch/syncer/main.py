import asyncio

import click

from jsearch.common import logs
from jsearch.utils import parse_range, add_gracefully_shutdown_handlers
from .service import Service


@click.command()
@click.option('--log-level', default='INFO', help="Log level")
@click.option('--sync-range', default=None, help="Blocks range to sync")
def run(log_level, sync_range):
    logs.configure(log_level)

    sync_range = parse_range(value=sync_range)

    service = Service(sync_range)
    coro = service.run()

    loop = asyncio.get_event_loop()
    task = asyncio.ensure_future(coro)

    add_gracefully_shutdown_handlers(service.gracefully_shutdown)
    try:
        loop.run_forever()
    finally:
        loop.close()
        if not task.cancelled():
            task.result()


if __name__ == '__main__':
    run()
